package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"opentw/internal/monitor"
	"opentw/internal/status"
	"opentw/internal/storage"
)

const (
	outboxBatch      = 50 // строк за один проход воркера
	deliveryInterval = 30 * time.Second
	outboxRetention  = 7 * 24 * time.Hour // отправленное хранится неделю
	outboxTTL        = 24 * time.Hour     // недоставленное старше суток — мусор
	groupedLineCap   = 15                 // строк списка в сводке; остальное «... ещё N»
	cleanupEvery     = time.Hour
	floodFloor       = time.Second // минимум паузы батча после 429 без retry_after
	backoffBase      = 30 * time.Second
	backoffCap       = 15 * time.Minute
)

// RunDelivery — воркер доставки: панели и уведомления уходят асинхронно от
// цикла мониторинга, переживают недоступность Telegram и рестарты (D2/D3).
func (b *Bot) RunDelivery(ctx context.Context) error {
	ticker := time.NewTicker(deliveryInterval)
	defer ticker.Stop()
	b.deliveryTick(ctx) // немедленная попытка: при старте могли остаться неотправленные
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-b.wake:
		case <-ticker.C:
		}
		b.deliveryTick(ctx)
	}
}

// deliveryTick — один проход: уведомления (новости важнее), затем панели,
// затем периодическая очистка outbox.
func (b *Bot) deliveryTick(ctx context.Context) {
	b.processOutbox(ctx)
	b.syncPanelsOnce(ctx)
	b.cleanupOutboxOnce()
}

// processOutbox доставляет накопленные события: устаревший backlog чата
// сворачивается в один дайджест (анти-лавина, P1-2), свежие строки идут
// по одной в строгом FIFO внутри чата (6.3): из очереди выбирается только
// созревшая головная строка чата — более новые строки того же чата не могут
// её обогнать, а чат с отложенной головой не занимает слоты батча и не
// блокирует чужие чаты (6.2). 429 паузит весь батч: флуд-лимит у бота
// глобальный (P1-3/P1-4). Временные ошибки дают нарастающий backoff (6.1).
func (b *Bot) processOutbox(ctx context.Context) {
	if until := b.floodUntil.Load(); until != 0 && time.Now().Unix() < until {
		return
	}
	failedChat := map[int64]bool{} // чат с ошибкой не обгоняется новыми строками до доезда старых (P2-6)
	if b.digestStaleBacklogs(ctx, failedChat) {
		return
	}
	// после доставки головы следующая строка чата становится головой и
	// уезжает в этом же тике (прогресс пере-запрашивает очередь);
	// ограничение проходов страхует от вечного цикла при сбойном MarkOutboxSent
	budget := outboxBatch
	for pass := 0; pass < outboxBatch; pass++ {
		rows, err := b.store.PendingOutboxDue(outboxBatch)
		if err != nil {
			b.log.Error("delivery: pending outbox", "err", err)
			return
		}
		if len(rows) == 0 {
			return
		}
		progressed := false
		for _, row := range rows {
			if ctx.Err() != nil { // остановка не завышает attempts и не шумит (P3-7)
				return
			}
			if failedChat[row.ChatID] {
				continue
			}
			var p deliveryPayload
			if err := json.Unmarshal(row.Payload, &p); err != nil {
				b.log.Error("delivery: decode payload", "chat", row.ChatID, "err", err)
				if err := b.store.MarkOutboxSent(row.ID); err != nil {
					b.log.Error("delivery: drop broken row", "err", err)
				}
				budget--
				progressed = true
				continue
			}
			if _, err := b.api.SendMessage(ctx, row.ChatID, renderOutbox(p)); err != nil {
				if chatUnavailable(err) {
					b.dropDeadChat(row.ChatID)
					continue
				}
				b.log.Error("delivery: send", "chat", row.ChatID, "err", err)
				if after, flood := b.pauseAfter(err); flood {
					if deferErr := b.store.DeferOutboxAttempt(row.ID, after); deferErr != nil {
						b.log.Error("delivery: defer attempt", "err", deferErr)
					}
					return
				}
				// 6.1: временная ошибка — нарастающий backoff (30с/1м/2м… до 15м):
				// строка живёт до доставки или TTL, удаления «после N попыток» нет
				if deferErr := b.store.DeferOutboxAttempt(row.ID, deferBackoff(row.Attempts)); deferErr != nil {
					b.log.Error("delivery: defer attempt", "err", deferErr)
				}
				failedChat[row.ChatID] = true
				continue
			}
			if err := b.store.MarkOutboxSent(row.ID); err != nil {
				b.log.Error("delivery: mark sent", "chat", row.ChatID, "err", err)
			}
			budget--
			progressed = true
		}
		if !progressed || budget <= 0 {
			return
		}
	}
}

// deferBackoff — нарастающая пауза между повторами временной ошибки
// доставки (не 429: для него свой retry_after): 30с, 1м, 2м… до 15 минут.
// attempts — число неудачных попыток строки до текущей.
func deferBackoff(attempts int) time.Duration {
	d := backoffBase << attempts
	if d <= 0 || d > backoffCap { // переполнение сдвига — потолок
		d = backoffCap
	}
	return d
}

// pauseAfter останавливает рассылку батча до retry_after для 429
// (флуд-лимит бота глобальный). ok=false для прочих ошибок.
func (b *Bot) pauseAfter(err error) (time.Duration, bool) {
	var apiErr *ApiError
	if !errors.As(err, &apiErr) || apiErr.Code != 429 {
		return 0, false
	}
	after := time.Duration(apiErr.RetryAfter * float64(time.Second))
	if after < floodFloor {
		after = floodFloor
	}
	b.floodUntil.Store(time.Now().Add(after).Unix() + 1)
	return after, true
}

// dropDeadChat убирает чат, в который бот больше не может писать (заблокирован,
// исключён, чат удалён): подписки и очередь мертвы. Лог обязателен — иначе
// владелец не поймёт, куда пропали уведомления (P2-5). Панельная привязка и
// in-memory кэши тоже мертвы (6.4): иначе syncPanel вечно ловил бы ошибки
// редактирования несуществующего сообщения.
func (b *Bot) dropDeadChat(chatID int64) {
	if err := b.store.DeleteSubscriptions(chatID); err != nil {
		b.log.Error("delivery: drop dead chat subscriptions", "chat", chatID, "err", err)
	}
	n, err := b.store.DeleteOutboxChat(chatID)
	if err != nil {
		b.log.Error("delivery: drop dead chat rows", "chat", chatID, "err", err)
		return
	}
	if err := b.store.DeleteStatusMessage(chatID); err != nil {
		b.log.Error("delivery: drop dead chat panel binding", "chat", chatID, "err", err)
	}
	b.mu.Lock()
	delete(b.lastText, chatID)
	delete(b.pinWarn, chatID)
	delete(b.drafts, chatID)
	b.mu.Unlock()
	b.log.Info("delivery: chat unavailable, dropped subscriptions and queued events", "chat", chatID, "rows", n)
}

// digestStaleBacklogs сворачивает устаревший backlog каждого чата в одно
// дайджест-сообщение: восстановление Telegram после долгого простоя не
// должно обрушивать лавину устаревших сообщений (P1-2). Чат с неудавшимся
// дайджестом попадает в failed — в этом же тике его строки не пробуются
// по одной. true — рассылка остановлена флуд-паузой.
func (b *Bot) digestStaleBacklogs(ctx context.Context, failed map[int64]bool) bool {
	chats, err := b.store.StaleOutboxChats(b.backlogStaleAfter())
	if err != nil {
		b.log.Error("delivery: stale chats", "err", err)
		return false
	}
	for _, chatID := range chats {
		if ctx.Err() != nil {
			return false
		}
		rows, err := b.store.PendingOutboxChat(chatID)
		if err != nil {
			b.log.Error("delivery: backlog rows", "chat", chatID, "err", err)
			continue
		}
		if len(rows) == 0 {
			continue
		}
		// Точные ID прочитанных строк: ApplyCycle может добавить чату новое
		// событие прямо во время отправки дайджеста — оно НЕ входило в
		// отправленный текст и обязано остаться pending (D1).
		ids := make([]int64, len(rows))
		for i := range rows {
			ids[i] = rows[i].ID
		}
		if _, err := b.api.SendMessage(ctx, chatID, renderDigest(rows)); err != nil {
			if chatUnavailable(err) {
				b.dropDeadChat(chatID)
				continue
			}
			b.log.Error("delivery: digest send", "chat", chatID, "err", err)
			if _, flood := b.pauseAfter(err); flood {
				return true
			}
			failed[chatID] = true
			continue // дайджест повторится следующим тиком
		}
		n, err := b.store.MarkOutboxSentByIDs(ids)
		if err != nil {
			b.log.Error("delivery: mark digest sent", "chat", chatID, "err", err)
			continue
		}
		b.log.Info("delivery: backlog digest sent", "chat", chatID, "rows", n)
	}
	return false
}

// backlogStaleAfter — порог устаревания backlog: два интервала мониторинга.
func (b *Bot) backlogStaleAfter() time.Duration {
	if b.backlogStale > 0 {
		return b.backlogStale
	}
	return 2 * b.interval()
}

// syncPanelsOnce обновляет панели всех чатов с привязкой, если цикл пометил
// их грязными; текст строится из store.LastChecks() — неполный цикл не затирает
// известное состояние (D1).
func (b *Bot) syncPanelsOnce(ctx context.Context) {
	if !b.panelDirty.Swap(false) {
		return
	}
	checks, err := b.store.LastChecks()
	if err != nil {
		b.log.Error("panel: last checks", "err", err)
		return
	}
	text := b.panelText(checks)
	chats, err := b.store.StatusChats()
	if err != nil {
		b.log.Error("panel: status chats", "err", err)
		return
	}
	for _, chatID := range chats {
		b.syncPanel(ctx, chatID, text)
	}
}

func (b *Bot) cleanupOutboxOnce() {
	now := time.Now().Unix()
	if last := b.lastCleanup.Load(); last != 0 && now-last < int64(cleanupEvery/time.Second) {
		return
	}
	b.lastCleanup.Store(now)
	if _, err := b.store.DeleteOldOutbox(outboxRetention); err != nil {
		b.log.Error("delivery: outbox cleanup", "err", err)
	}
	// TTL — предохранитель (при живом дайджесте недостижим); удалённое
	// обязательно логируется: молчаливая потеря событий недопустима (P1-2)
	if n, err := b.store.DropStaleOutbox(outboxTTL); err != nil {
		b.log.Error("delivery: outbox ttl", "err", err)
	} else if n > 0 {
		b.log.Warn("delivery: dropped undeliverable events older than TTL", "rows", n)
	}
}

// renderOutbox строит текст из сохранённого payload: единственное событие —
// одиночный формат, иначе сводка цикла.
func renderOutbox(p deliveryPayload) string {
	if len(p.Trs) == 1 {
		return RenderNotification(p.Trs[0])
	}
	return RenderGrouped(p)
}

// digestState — накопленный итог по одной модели за период недоставки.
type digestState struct {
	model  string
	first  status.Status // состояние на начало окна
	final  status.Status // итоговое состояние
	flips  int           // сколько раз менялась
	since  time.Time     // начало текущего плохого периода
	lastAt time.Time     // время последнего перехода
	dur    time.Duration // длительность завершившегося сбоя (последнее восстановление)
}

// digestLine — строка одной модели в дайджесте: итог + длительность + счётчик.
func digestLine(st *digestState) string {
	suffix := ""
	if st.flips > 1 {
		suffix = fmt.Sprintf(" (переключалась %s)", pluralEvents(st.flips))
	}
	switch st.final {
	case status.Down:
		if d := st.lastAt.Sub(st.since); d > 0 {
			return codeID(st.model) + " - недоступна ~" + shortDuration(d) + suffix
		}
		return codeID(st.model) + " - недоступна" + suffix
	case status.Degraded:
		return codeID(st.model) + " - работает нестабильно" + suffix
	default:
		if st.dur > 0 {
			return codeID(st.model) + " - сбой длился " + shortDuration(st.dur) + suffix
		}
		return codeID(st.model) + " - восстановилась" + suffix
	}
}

// renderDigest строит одно дайджест-сообщение из накопленных строк чата:
// итоговые состояния моделей и честные счётчики изменений вместо лавины
// устаревших сообщений (P1-2). Порядок строк — порядок первых appearances.
func renderDigest(rows []storage.OutboxRow) string {
	states := map[string]*digestState{}
	var order []string
	total := 0
	firstSeen, lastSeen, lastCheck := time.Time{}, time.Time{}, time.Time{}
	for _, row := range rows {
		var p deliveryPayload
		if err := json.Unmarshal(row.Payload, &p); err != nil {
			continue
		}
		total += len(p.Trs)
		if firstSeen.IsZero() || row.CreatedAt.Before(firstSeen) {
			firstSeen = row.CreatedAt
		}
		if row.CreatedAt.After(lastSeen) {
			lastSeen = row.CreatedAt
		}
		for _, tr := range p.Trs {
			st, ok := states[tr.ModelID]
			if !ok {
				st = &digestState{model: tr.ModelID, first: tr.From, since: tr.Check.At}
				states[tr.ModelID] = st
				order = append(order, tr.ModelID)
			}
			if tr.To != status.Operational && st.final == status.Operational {
				st.since = tr.Check.At // вход в плохой период
			}
			st.final = tr.To
			st.flips++
			st.lastAt = tr.Check.At
			if tr.To == status.Operational && tr.Incident != nil {
				st.dur = tr.Incident.Duration
			}
			if tr.Check.At.After(lastCheck) {
				lastCheck = tr.Check.At
			}
		}
	}

	var b strings.Builder
	fmt.Fprintf(&b, "📬 Пока Telegram был недоступен: %s за %s\n",
		pluralEvents(total), shortDuration(lastSeen.Sub(firstSeen)))
	shown, rest := 0, 0
	for _, section := range []struct {
		emoji string
		title string
		match func(status.Status) bool
	}{{
		emoji: "🔴", title: "Сейчас недоступны", match: func(s status.Status) bool { return s == status.Down },
	}, {
		emoji: "🟡", title: "Нестабильны", match: func(s status.Status) bool { return s == status.Degraded },
	}, {
		emoji: "🟢", title: "Восстановились", match: func(s status.Status) bool { return s == status.Operational },
	}} {
		var lines []string
		for _, id := range order {
			if states[id] != nil && section.match(states[id].final) {
				lines = append(lines, digestLine(states[id]))
			}
		}
		if len(lines) == 0 {
			continue
		}
		limit := groupedLineCap - shown
		if limit < 0 {
			limit = 0
		}
		display := lines
		if len(display) > limit {
			rest += len(display) - limit
			display = display[:limit]
		}
		shown += len(display)
		fmt.Fprintf(&b, "\n%s %s (%d):\n", section.emoji, section.title, len(lines))
		for _, l := range display {
			b.WriteString(l + "\n")
		}
	}
	if rest > 0 {
		fmt.Fprintf(&b, "... ещё %s\n", pluralModels(rest))
	}
	b.WriteString("\nПроверено: " + fmtClockMSK(lastCheck) + " MSK")
	return b.String()
}

func pluralEvents(n int) string {
	mod10, mod100 := n%10, n%100
	switch {
	case mod10 == 1 && mod100 != 11:
		return fmt.Sprintf("%d событие", n)
	case mod10 >= 2 && mod10 <= 4 && (mod100 < 12 || mod100 > 14):
		return fmt.Sprintf("%d события", n)
	default:
		return fmt.Sprintf("%d событий", n)
	}
}

// RenderGrouped — сводка событий одного цикла для одного чата: заголовок
// отражает масштаб, строки — только его подписки.
func RenderGrouped(p deliveryPayload) string {
	trs := p.Trs
	var header string
	allDown, allRecovered := true, true
	for _, tr := range trs {
		if tr.To != status.Down {
			allDown = false
		}
		if tr.To != status.Operational {
			allRecovered = false
		}
	}
	switch {
	case allDown && p.DownInCycle*2 >= p.Total:
		header = fmt.Sprintf("🔴 Массовый сбой: %d из %d моделей недоступны", p.DownInCycle, p.Total)
	case allDown:
		header = "🔴 Недоступны: " + pluralModels(len(trs))
	case allRecovered:
		header = "🟢 Восстановились: " + pluralModels(len(trs))
	default:
		header = "Состояния изменились: " + pluralModels(len(trs))
	}

	var b strings.Builder
	b.WriteString(header + "\n\n")
	shown := trs
	rest := 0
	if len(shown) > groupedLineCap {
		rest = len(shown) - groupedLineCap
		shown = shown[:groupedLineCap]
	}
	checkedAt := time.Time{}
	for _, tr := range shown {
		if tr.Check.At.After(checkedAt) {
			checkedAt = tr.Check.At
		}
		b.WriteString(groupedLine(tr) + "\n")
	}
	if rest > 0 {
		b.WriteString("... ещё " + pluralModels(rest) + "\n")
	}
	for _, tr := range trs { // время = самая поздняя проверка цикла
		if tr.Check.At.After(checkedAt) {
			checkedAt = tr.Check.At
		}
	}
	b.WriteString("\nПроверено: " + fmtClockMSK(checkedAt) + " MSK")
	return b.String()
}

// groupedLine — строка модели в сводке: как строка панели, для восстановления —
// длительность сбоя.
func groupedLine(tr monitor.Transition) string {
	switch tr.To {
	case status.Operational:
		if tr.Incident != nil && tr.Incident.Duration > 0 {
			return codeID(tr.ModelID) + " - сбой длился " + shortDuration(tr.Incident.Duration)
		}
		return codeID(tr.ModelID) + " - восстановилась"
	case status.Degraded:
		return codeID(tr.ModelID) + " - работает нестабильно: " + panelErrorHead(tr.Check)
	default: // Down
		line := codeID(tr.ModelID) + " - " + panelErrorHead(tr.Check)
		if tr.From == status.Unknown {
			// модель вернулась из слепого окна уже упавшей (D2)
			line += " (время падения неизвестно)"
		}
		return line
	}
}

// shortDuration —Duration без секунд: «1ч 30м», «15м».
func shortDuration(d time.Duration) string {
	sec := int64(d.Seconds())
	if sec < 0 {
		sec = 0
	}
	switch {
	case sec < 3600:
		return fmt.Sprintf("%dм", sec/60)
	default:
		return fmt.Sprintf("%dч %02dм", sec/3600, (sec%3600)/60)
	}
}

// panelErrorHead — короткое обозначение ошибки для списков (обрезка 20 рун).
func panelErrorHead(c storage.Check) string {
	switch c.ErrorType {
	case "http":
		head := fmt.Sprintf("HTTP %d", c.HTTPStatus)
		if c.ErrorMessage != "" {
			head += " · " + EscapeHTML(trunc(oneLine(c.ErrorMessage), maxPanelErr))
		}
		return head
	case "timeout":
		return "Timeout"
	case "connrefused":
		return "Connection refused"
	case "empty":
		return "HTTP 200 · пустая генерация"
	case "perf":
		return "деградация производительности"
	default:
		return "ошибка монитора"
	}
}
