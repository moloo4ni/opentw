package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"opentw/internal/status"
	"opentw/internal/storage"
)

const (
	outboxBatch      = 50 // строк за один проход воркера
	deliveryInterval = 30 * time.Second
	outboxRetention  = 7 * 24 * time.Hour // отправленное хранится неделю
	outboxTTL        = 24 * time.Hour     // недоставленное старше суток — мусор
	cleanupEvery     = time.Hour
	floodFloor       = time.Second // минимум паузы батча после 429 без retry_after
	backoffBase      = 30 * time.Second
	backoffCap       = 15 * time.Minute
)

// RunDelivery — воркер доставки: панели и уведомления уходят асинхронно от
// цикла мониторинга, переживают недоступность Telegram и рестарты.
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

// processOutbox доставляет накопленные события сверкой актуальности.
// FIFO внутри чата сохраняется: PendingOutboxDue отдаёт только
// созревшую головную строку чата; при её обработке чат целиком сворачивается
// в одно сообщение по ТЕКУЩИМ состояниям. 429 паузит весь батч: флуд-лимит
// у бота глобальный. Временные ошибки дают нарастающий backoff.
// Отправка есть — отчёта нет (коммит после отправки не прошёл) даёт
// at-least-once: строка может доехать повтором, это задокументированное
// ограничение, очередь не фризится.
func (b *Bot) processOutbox(ctx context.Context) {
	if until := b.floodUntil.Load(); until != 0 && time.Now().Unix() < until {
		return
	}
	failedChat := map[int64]bool{} // чат с ошибкой не обгоняется новыми строками
	budget := outboxBatch
	// после доставки головы следующая строка чата становится головой и
	// уезжает в этом же тике (прогресс пере-запрашивает очередь);
	// ограничение проходов страхует от вечного цикла при сбойном MarkOutboxSent
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
		seen := map[int64]bool{} // один чат — одна свёртка за проход
		for _, row := range rows {
			if ctx.Err() != nil { // остановка не завышает attempts и не шумит
				return
			}
			if failedChat[row.ChatID] || seen[row.ChatID] {
				continue
			}
			seen[row.ChatID] = true
			switch b.deliverChat(ctx, row.ChatID) {
			case deliverDone:
				progressed = true
			case deliverFlood:
				return
			case deliverFail:
				failedChat[row.ChatID] = true
			}
			budget--
			if budget <= 0 {
				return
			}
		}
		if !progressed {
			return
		}
	}
}

// deliverState — исход свёртки одного чата.
type deliverState int

const (
	deliverDone deliverState = iota
	deliverFlood
	deliverFail
)

// deliverChat обрабатывает все pending-строки чата одной сверкой:
// строки группируются по модели, решение принимается по ТЕКУЩЕМУ состоянию
// (models.last_status) и chat_notified. cur Unknown → тишина; cur == notified
// → тишина; иначе одна строка по cur. Строки — в порядке конфига, одним
// rich-сообщением, без заголовков.
//
// OPW-01: пометка «отправлено» присваивается ТОЛЬКО строкам, все модели
// которых наблюдаемы достоверно (или больше не наблюдаемы: отписка,
// уход из конфигурации). Строки модели в ⚪ остаются pending:
// неопределённость проверки не является доставкой и не уничтожает
// свидетельство о недоставленном изменении. Строки одной модели решаются
// вместе (факт определяется текущим статусом модели — восстановление не
// обгонит недоставленное падение), строки чужих моделей не ждут
// неопределённой (изоляция моделей одного чата).
func (b *Bot) deliverChat(ctx context.Context, chatID int64) deliverState {
	rows, err := b.store.PendingOutboxChat(chatID)
	if err != nil {
		b.log.Error("delivery: backlog rows", "chat", chatID, "err", err)
		return deliverFail
	}
	if len(rows) == 0 {
		return deliverDone
	}
	// Точные ID прочитанных строк: ApplyCycle может добавить чату новое
	// событие прямо во время доставки — оно не входило в решение и обязано
	// остаться pending.
	type pendingRow struct {
		row    storage.OutboxRow
		models map[string]bool
	}
	decoded := make([]pendingRow, 0, len(rows))
	modelsPending := map[string]bool{}
	for _, row := range rows {
		var p deliveryPayload
		if err := json.Unmarshal(row.Payload, &p); err != nil {
			b.log.Error("delivery: decode payload", "chat", row.ChatID, "err", err)
			if err := b.store.MarkOutboxSent(row.ID); err != nil {
				b.log.Error("delivery: drop broken row", "err", err)
			}
			continue
		}
		pr := pendingRow{row: row, models: map[string]bool{}}
		for _, tr := range p.Trs {
			pr.models[tr.ModelID] = true
			modelsPending[tr.ModelID] = true
		}
		decoded = append(decoded, pr)
	}
	if len(decoded) == 0 {
		return deliverDone
	}

	models, err := b.store.Models()
	if err != nil {
		b.log.Error("delivery: models", "chat", chatID, "err", err)
		return b.deferHead(chatID, decoded[0].row)
	}
	cur := map[string]status.Status{}
	for _, m := range models {
		cur[m.ID] = m.LastStatus
	}
	lastChecks, err := b.store.LastChecks()
	if err != nil {
		b.log.Error("delivery: last checks", "chat", chatID, "err", err)
		return b.deferHead(chatID, decoded[0].row)
	}
	// подписка могла быть снята между событием и доставкой: строку выбрасываем
	subs, err := b.store.Subscriptions(chatID)
	if err != nil {
		b.log.Error("delivery: subscriptions", "chat", chatID, "err", err)
		return b.deferHead(chatID, decoded[0].row)
	}
	subscribed := map[string]bool{}
	for _, id := range subs {
		subscribed[id] = true
	}

	var lines []string
	notifiedSet := map[string]status.Status{}
	for _, m := range b.models { // порядок строк — порядок конфига
		if !modelsPending[m.ID] || !subscribed[m.ID] {
			continue
		}
		st, exists := cur[m.ID]
		if !exists {
			continue // модель удалена из конфигурации: наблюдать нечего
		}
		if st != status.Operational && st != status.Down {
			continue // cur Unknown (или старый статус): тишина, chat_notified не трогаем
		}
		notified, ok, err := b.store.ChatNotifiedState(chatID, m.ID)
		if err != nil {
			b.log.Error("delivery: chat notified state", "chat", chatID, "model", m.ID, "err", err)
			return b.deferHead(chatID, decoded[0].row)
		}
		if ok && notified == st {
			continue // пользователь уже знает: строк не отправлять, пометить sent
		}
		if st == status.Down {
			c := lastChecks[m.ID]
			if _, ok := lastChecks[m.ID]; !ok {
				// модель без check-строки (теоретически): подпись из модели
				c = storage.Check{ModelID: m.ID, Status: st, ErrorType: m.ErrorType, ErrorMessage: m.ErrorMessage}
			}
			lines = append(lines, downLine(m.ID, c))
		} else {
			lines = append(lines, recoveredLine(m.ID))
		}
		notifiedSet[m.ID] = st
	}

	// OPW-01: строка гасится (помечается sent) только если ВСЕ её модели
	// наблюдаемы достоверно — ⚪-модель оставляет свою строку pending:
	// неопределённость не имеет права поглощать единственное свидетельство
	// о недоставленном изменении. Факт доставки определяется текущим
	// статусом модели, поэтому строки одной модели решаются вместе.
	var ids []int64
	var stuckHead *storage.OutboxRow
	for i := range decoded {
		resolved := true
		for mid := range decoded[i].models {
			if !subscribed[mid] {
				continue // отписка: строку выбрасываем
			}
			st, exists := cur[mid]
			if !exists {
				continue // модель ушла из конфигурации: наблюдать нечего
			}
			if st != status.Operational && st != status.Down {
				resolved = false // cur Unknown: свид-во недоставленного изменения
			}
		}
		if resolved {
			ids = append(ids, decoded[i].row.ID)
		} else if stuckHead == nil {
			stuckHead = &decoded[i].row
		}
	}

	if len(lines) > 0 {
		if _, err := b.api.SendRichMessage(ctx, chatID, strings.Join(lines, "")); err != nil {
			if chatUnavailable(err) {
				b.dropDeadChat(chatID)
				return deliverDone
			}
			b.log.Error("delivery: send", "chat", chatID, "err", err)
			if after, flood := b.pauseAfter(err); flood {
				_ = b.store.DeferOutboxAttempt(decoded[0].row.ID, after)
				return deliverFlood
			}
			// 6.1: временная ошибка — нарастающий backoff (30с/1м/2м… до 15м):
			// строки живут до доставки или TTL, удаления «после N попыток» нет
			return b.deferHead(chatID, decoded[0].row)
		}
	}
	// Порядок обязателен: сначала Telegram подтвердил отправку (выше), затем
	// ОДНИМ коммитом фиксируются уведомлённые состояния и пометки строк.
	// До ответа Telegram chat_notified не пишется — иначе при сбое доставки
	// событие считалось бы отправленным и потерялось. Сбой коммита не признаёт
	// доставку готовой: строки остаются pending (at-least-once — редкий дубль
	// вместо потери), пауза через backoff без долбления БД.
	if len(ids) > 0 || len(notifiedSet) > 0 {
		if err := b.store.CommitDelivery(chatID, notifiedSet, ids); err != nil {
			b.log.Error("delivery: commit", "chat", chatID, "err", err)
			return b.deferHead(chatID, decoded[0].row)
		}
	}
	if stuckHead != nil {
		// ⚪-свидетельство остаётся pending; голова получает короткую паузу
		// БЕЗ счётчика попыток (не искажает backoff реальных сбоев, не даёт
		// чату занимать слоты батча и не гоняет свёртку вхолостую каждый тик).
		if err := b.store.DeferOutboxRetry(stuckHead.ID, deliveryInterval); err != nil {
			b.log.Error("delivery: defer stuck row", "chat", chatID, "err", err)
		}
		if len(ids) == 0 && len(notifiedSet) == 0 {
			return deliverFail // свёртке нечего зафиксировать: чат в этом тике больше не трогаем
		}
	}
	return deliverDone
}

// deferHead откладывает головную строку чата и возвращает исход «сбой».
func (b *Bot) deferHead(chatID int64, head storage.OutboxRow) deliverState {
	if err := b.store.DeferOutboxAttempt(head.ID, deferBackoff(head.Attempts)); err != nil {
		b.log.Error("delivery: defer attempt", "chat", chatID, "err", err)
	}
	return deliverFail
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
// исключён, чат удалён): подписки, очередь, привязка панели и уведомлённые
// состояния мертвы. Лог обязателен — иначе владелец не поймёт, куда пропали
// уведомления. In-memory кэши тоже.
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
	if err := b.store.DeleteChatNotifiedState(chatID); err != nil {
		b.log.Error("delivery: drop dead chat notified state", "chat", chatID, "err", err)
	}
	b.mu.Lock()
	delete(b.lastText, chatID)
	delete(b.pinWarn, chatID)
	delete(b.drafts, chatID)
	b.mu.Unlock()
	b.log.Info("delivery: chat unavailable, dropped subscriptions and queued events", "chat", chatID, "rows", n)
}

// syncPanelsOnce обновляет панели всех чатов с привязкой, если цикл пометил
// их грязными; текст строится из store.LastChecks() — неполный цикл не затирает
// известное состояние.
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
	// TTL — предохранитель; удалённое обязательно логируется: молчаливая
	// потеря событий недопустима
	if n, err := b.store.DropStaleOutbox(outboxTTL); err != nil {
		b.log.Error("delivery: outbox ttl", "err", err)
	} else if n > 0 {
		b.log.Warn("delivery: dropped undeliverable events older than TTL", "rows", n)
	}
}
