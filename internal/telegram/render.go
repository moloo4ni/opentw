package telegram

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"html"
	"sort"
	"strings"
	"time"

	"opentw/internal/status"
	"opentw/internal/storage"
)

// MSK — часовой пояс всех времён в интерфейсе OpenTW.
var MSK = time.FixedZone("MSK", 3*60*60)

const (
	absent          = "N/A"
	maxPanelErr     = 20 // панель — одна строка на десктопе; полный текст в /status model-id
	maxCallbackData = 64 // лимит Telegram на callback_data в байтах
)

func fmtClockMSK(t time.Time) string { return t.In(MSK).Format("15:04:05") }

// fmtDateClockMSK — дата+время: панель трёхдневной свежести должна отличаться
// от свежей (D8).
func fmtDateClockMSK(t time.Time) string { return t.In(MSK).Format("02.01.2006 15:04:05") }

// intervalHuman — интервал проверки словами для подвала панели.
func intervalHuman(d time.Duration) string {
	h, m := int(d.Hours()), int(d.Minutes())%60
	switch {
	case h == 0:
		return fmt.Sprintf("%d мин", m)
	case m == 0:
		return fmt.Sprintf("%dч", h)
	default:
		return fmt.Sprintf("%dч %dм", h, m)
	}
}

func fmtSecs(us int64) string  { return fmt.Sprintf("%.2fs", float64(us)/1e6) }
func fmtSecs1(us int64) string { return fmt.Sprintf("%.1fs", float64(us)/1e6) }

// statusTitleRu — русские подписи статусов (детальная страница; эмодзи уже передают цвет).
func statusTitleRu(st status.Status) string {
	switch st {
	case status.Operational:
		return "работает"
	case status.Degraded:
		return "нестабильно"
	case status.Down:
		return "недоступна"
	default:
		return "нет данных"
	}
}

// EscapeHTML экранирует любые внешние строки перед отправкой в Telegram.
func EscapeHTML(s string) string { return html.EscapeString(s) }

func trunc(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "..."
}

func f64Ptr(v *float64) (string, bool) {
	if v == nil || *v <= 0 {
		return absent, false
	}
	return fmt.Sprintf("%.1f", *v), true
}

func i64Ptr(v *int64) string {
	if v == nil {
		return absent
	}
	return fmt.Sprintf("%d", *v)
}

// panelLegend — расшифровка кружков (элемент IA Cliff), последняя строка панели.
const panelLegend = "🟢 работает · 🟡 нестабильно · 🔴 недоступна · ⚪ нет данных"

// RenderStatusPanel renders the pinned /status panel: one line per model,
// grouped by family, exact model IDs, N/A for unknown metrics.
func RenderStatusPanel(models []storage.Model, checks map[string]storage.Check, updatedAt, nextAt time.Time, interval time.Duration) string {
	var b strings.Builder
	b.WriteString("<b>OpenTW</b>\n\n")

	checked := 0
	counts := map[status.Status]int{}
	for _, m := range models {
		if c, ok := checks[m.ID]; ok && c.Status != "" {
			checked++
			counts[c.Status]++
		}
	}

	if checked == 0 {
		// до первого цикла честно предупреждаем вместо фальшивого «Обновлено: сейчас»
		b.WriteString("Первый цикл проверки выполняется - панель обновится автоматически\n")
	} else {
		b.WriteString(panelSummary(counts) + "\n")
		if counts[status.Unknown] == checked {
			b.WriteString("⚠ Монитор не может проверить модели - возможна проблема ключа API\n")
		}
	}

	family := ""
	for _, m := range models {
		if m.Family != family {
			family = m.Family
			b.WriteString("\n<b>" + EscapeHTML(family) + "</b>\n")
		}
		b.WriteString(modelLine(m, checks[m.ID]))
		b.WriteString("\n")
	}

	if checked > 0 {
		b.WriteString("\nОбновлено: " + fmtDateClockMSK(updatedAt) + " MSK\n")
		b.WriteString(fmt.Sprintf("Следующая проверка: %s MSK (интервал %s)\n", fmtClockMSK(nextAt), intervalHuman(interval)))
	}
	b.WriteString(panelLegend)
	return b.String()
}

// panelSummary — «Модели: 22 🟢 · 2 🟡 · 1 🔴»: ответ «всё ли ок» одним взглядом.
func panelSummary(counts map[status.Status]int) string {
	var parts []string
	for _, st := range []status.Status{status.Operational, status.Degraded, status.Down, status.Unknown} {
		if counts[st] > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", counts[st], st.Emoji()))
		}
	}
	return "Модели: " + strings.Join(parts, " · ")
}

// codeID оборачивает model ID в моноширинный формат Telegram.
func codeID(id string) string { return "<code>" + EscapeHTML(id) + "</code>" }

// modelLine формирует строку одной модели (обязательно одну).
func modelLine(m storage.Model, c storage.Check) string {
	if c.Status == "" {
		// ещё нет ни одной проверки этой модели
		return "⚪ " + codeID(m.ID) + " - нет данных"
	}
	if c.Status == status.Unknown {
		// ⚪ любой природы — проблема монитора, а не модели: причина и исходный
		// текст — в /status model-id
		return "⚪ " + codeID(m.ID) + " - ошибка монитора"
	}
	emoji := c.Status.Emoji()

	switch {
	case c.ErrorType == "perf":
		return emoji + " " + codeID(m.ID) + " - " + metricsTail(c) + " · деградация"
	case c.ErrorType == "empty":
		if msg := trunc(oneLine(c.ErrorMessage), maxPanelErr); c.ErrorMessage != "" && c.ErrorMessage != "empty generation" {
			// шлюз вернул 200 с JSON-ошибкой: его текст важнее «пустой генерации»
			return emoji + " " + codeID(m.ID) + " - HTTP 200 · " + EscapeHTML(msg)
		}
		return emoji + " " + codeID(m.ID) + " - HTTP 200 · пустая генерация"
	case c.ErrorType != "":
		return emoji + " " + codeID(m.ID) + " - " + errorHead(m.ID, c)
	default:
		return emoji + " " + codeID(m.ID) + " - " + metricsTail(c)
	}
}

// errorHead формирует краткое описание ошибки для панели.
func errorHead(modelID string, c storage.Check) string {
	var head string
	switch c.ErrorType {
	case "http":
		head = fmt.Sprintf("HTTP %d", c.HTTPStatus)
	case "timeout":
		head = "Timeout"
	case "connrefused":
		head = "Connection refused"
	case "invalid":
		head = "ошибка монитора"
	default:
		head = c.ErrorType
	}
	if c.ErrorMessage == "" || c.ErrorType == "invalid" {
		return head
	}
	msg := trunc(oneLine(c.ErrorMessage), maxPanelErr)
	return head + " · " + EscapeHTML(msg)
}

func oneLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// metricsTail — компактные метрики строки панели: разделитель «·»,
// одна десятичная секунды, токены «in/out» без слов (полные подписи — ТЗ §8).
func metricsTail(c storage.Check) string {
	var parts []string
	if c.LatencyUS != nil {
		parts = append(parts, fmtSecs1(*c.LatencyUS))
	} else {
		parts = append(parts, absent)
	}
	tps, _ := f64Ptr(c.TPS)
	parts = append(parts, tps+" TPS")
	if c.InputTokens != nil && c.OutputTokens != nil {
		parts = append(parts, fmt.Sprintf("%d/%d", *c.InputTokens, *c.OutputTokens))
	}
	return strings.Join(parts, " · ")
}

// detailErrorLine — полная строка последней ошибки (без обрезки).
func detailErrorLine(c storage.Check) string {
	head := ""
	switch c.ErrorType {
	case "http":
		head = fmt.Sprintf("HTTP %d", c.HTTPStatus)
	case "timeout":
		head = "Timeout"
	case "connrefused":
		head = "Connection refused"
	case "invalid":
		head = "Некорректный ответ API"
	case "empty":
		head = "HTTP 200 | пустая генерация"
	default:
		head = c.ErrorType
	}
	if c.ErrorMessage != "" && c.ErrorType != "invalid" && c.ErrorType != "empty" {
		return head + " · " + EscapeHTML(oneLine(c.ErrorMessage))
	}
	return head
}

// RenderModelDetail renders the /status model-id view.
func RenderModelDetail(modelID string, st status.Status, last storage.Check, last24 []storage.Check) string {
	var b strings.Builder
	b.WriteString("<b>" + EscapeHTML(modelID) + "</b> - " + st.Emoji() + " " + statusTitleRu(st) + "\n\n")
	if last.At.IsZero() {
		// модель ещё ни разу не проверялась — осмысленной даты нет
		b.WriteString("Последняя проверка: " + absent + "\n\n")
	} else {
		b.WriteString("Последняя проверка: " + fmtClockMSK(last.At) + " MSK\n\n")
	}

	b.WriteString("Последний запрос\n")
	if last.HTTPStatus > 0 {
		b.WriteString(fmt.Sprintf("HTTP: %d\n", last.HTTPStatus))
	} else {
		b.WriteString("HTTP: " + absent + "\n")
	}
	b.WriteString("Latency: " + secPtr(last.LatencyUS) + "\n")
	b.WriteString("TTFT: " + secPtr(last.TTFTUS) + "\n")
	b.WriteString("Generation time: " + secPtr(last.GenUS) + "\n")
	tps, _ := f64Ptr(last.TPS)
	b.WriteString("TPS: " + tps + "\n")
	b.WriteString("Input tokens: " + i64Ptr(last.InputTokens) + "\n")
	b.WriteString("Output tokens: " + i64Ptr(last.OutputTokens) + "\n")

	b.WriteString("\nЗа последние 24 часа\n")
	if len(last24) == 0 {
		b.WriteString("Проверок: 0\n")
	} else {
		var okCount, downCount, unknownCount int
		var latencies []float64
		var tpsVals []float64
		for _, c := range last24 {
			switch c.Status {
			case status.Operational:
				okCount++
			case status.Down:
				downCount++
			case status.Unknown:
				unknownCount++
			}
			if c.LatencyUS != nil && c.Status == status.Operational {
				latencies = append(latencies, float64(*c.LatencyUS)/1e6)
			}
			if c.TPS != nil && *c.TPS > 0 && c.Status == status.Operational {
				tpsVals = append(tpsVals, *c.TPS)
			}
		}
		// ⚪ — не аптайм и не сбой: день с битым ключом монитора не должен
		// обещать 100% доступности (D11)
		known := len(last24) - unknownCount
		if known > 0 {
			up := float64(okCount) / float64(known) * 100
			b.WriteString(fmt.Sprintf("Uptime: %.2f%%\n", up))
		}
		if unknownCount > 0 {
			b.WriteString(fmt.Sprintf("Без данных (монитор): %d\n", unknownCount))
		}
		b.WriteString(fmt.Sprintf("Проверок: %d\n", len(last24)))
		b.WriteString(fmt.Sprintf("Успешных: %d\n", okCount))
		b.WriteString(fmt.Sprintf("Неудачных: %d\n", downCount))
		if len(latencies) > 0 {
			sort.Float64s(latencies)
			mean := 0.0
			for _, v := range latencies {
				mean += v
			}
			mean /= float64(len(latencies))
			b.WriteString(fmt.Sprintf("Средний latency: %.2fs\n", mean))
			b.WriteString(fmt.Sprintf("P50 latency: %.2fs\n", percentileF(latencies, 0.50)))
			b.WriteString(fmt.Sprintf("P95 latency: %.2fs\n", percentileF(latencies, 0.95)))
		}
		if len(tpsVals) > 0 {
			mean := 0.0
			for _, v := range tpsVals {
				mean += v
			}
			mean /= float64(len(tpsVals))
			b.WriteString(fmt.Sprintf("Средний TPS: %.1f\n", mean))
		}
		// флэппинг-наблюдаемость: по тем же данным, без новой семантики
		if n, last, dwell := flappingStats(last24); n > 0 {
			b.WriteString(fmt.Sprintf("Переключений состояния: %d\n", n))
			b.WriteString("Последнее переключение: " + fmtClockMSK(last) + " MSK\n")
			b.WriteString("Среднее пребывание в статусе: " + intervalHuman(dwell) + "\n")
		}
	}

	// последняя ошибка — последняя проверка не в Operational с ошибкой
	var lastErr *storage.Check
	for i := len(last24) - 1; i >= 0; i-- {
		if last24[i].Status == status.Down || last24[i].Status == status.Degraded {
			c := last24[i]
			lastErr = &c
			break
		}
	}
	if lastErr == nil && (last.Status == status.Down || last.Status == status.Degraded) {
		lastErr = &last
	}
	if lastErr != nil {
		b.WriteString("\nПоследняя ошибка\n")
		b.WriteString(fmtClockMSK(lastErr.At) + " · " + detailErrorLine(*lastErr))
	}
	return b.String()
}

func secPtr(v *int64) string {
	if v == nil {
		return absent
	}
	return fmtSecs(*v)
}

// flappingStats — переключения за окно, время последнего и среднее пребывание
// в статусе (равные интервалы проверки → среднее = окно/переключений).
func flappingStats(checks []storage.Check) (int, time.Time, time.Duration) {
	var n int
	var last time.Time
	prev := status.Status("")
	for _, c := range checks {
		if prev != "" && c.Status != prev {
			n++
			last = c.At
		}
		prev = c.Status
	}
	if n == 0 || len(checks) < 2 {
		return 0, time.Time{}, 0
	}
	span := checks[len(checks)-1].At.Sub(checks[0].At)
	return n, last, span / time.Duration(n)
}

func percentileF(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(q * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func pluralModels(n int) string {
	mod10, mod100 := n%10, n%100
	switch {
	case mod10 == 1 && mod100 != 11:
		return fmt.Sprintf("%d модель", n)
	case mod10 >= 2 && mod10 <= 4 && (mod100 < 12 || mod100 > 14):
		return fmt.Sprintf("%d модели", n)
	default:
		return fmt.Sprintf("%d моделей", n)
	}
}

// cbData кодирует идентификатор модели/семейства в callback-кнопку:
// идентификатор идёт как есть — он стабилен при переупорядочении конфига
// и рестартах (позиционный индекс такой гарантии не даёт); длинные ID
// сворачиваются в устойчивый хэш (cbData с префиксом не влезает в лимит).
func cbData(prefix, id string) string {
	if len(prefix)+len(id)+1 <= maxCallbackData {
		return prefix + ":" + id
	}
	return prefix + ":h:" + shortHash(id)
}

// shortHash — 10 hex-символов sha256: достаточно без коллизий для десятков ID.
func shortHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:5])
}

// resolveCallback сопоставляет payload нажатой кнопки с идентификатором
// из текущей конфигурации; ok=false — кнопка ссылается на удалённую
// модель/семейство (устаревшая клавиатура).
func resolveCallback(payload string, candidates []string) (string, bool) {
	for _, id := range candidates {
		if id == payload {
			return id, true
		}
	}
	if h, ok := strings.CutPrefix(payload, "h:"); ok && len(h) == 10 {
		for _, id := range candidates {
			if shortHash(id) == h {
				return id, true
			}
		}
	}
	return "", false
}

// modelIDs — список ID моделей (кандидаты для кнопок "s:").
func modelIDs(models []storage.Model) []string {
	ids := make([]string, len(models))
	for i, m := range models {
		ids[i] = m.ID
	}
	return ids
}

// diffBracket — «(+A -R)» относительно сохранённого; пустая строка без изменений.
func diffBracket(scope []storage.Model, draft, saved map[string]bool) string {
	added, removed := 0, 0
	for _, m := range scope {
		if draft[m.ID] && !saved[m.ID] {
			added++
		} else if !draft[m.ID] && saved[m.ID] {
			removed++
		}
	}
	var parts []string
	if added > 0 {
		parts = append(parts, fmt.Sprintf("+%d", added))
	}
	if removed > 0 {
		parts = append(parts, fmt.Sprintf("-%d", removed))
	}
	if len(parts) == 0 {
		return ""
	}
	return " (изменения: " + strings.Join(parts, " ") + ")"
}

// RenderSubscribeRoot — первый экран /subscribe: кнопки семейств с числом
// выбранных, масс-операции, Сохранить и Отменить. Callback data: "f:<family>".
func RenderSubscribeRoot(models []storage.Model, draft, saved map[string]bool) (string, *InlineKeyboardMarkup) {
	var b strings.Builder
	b.WriteString("OpenTW - Настройка уведомлений\n\n")
	b.WriteString("Уведомления приходят только по выбранным моделям\nпри смене их состояния. Панель /status не зависит от подписок.\n")

	families := FamiliesOf(models)
	counts := map[string][2]int{} // family: {selected, total}
	for _, m := range models {
		c := counts[m.Family]
		c[1]++
		if draft[m.ID] {
			c[0]++
		}
		counts[m.Family] = c
	}
	selected := 0
	for _, c := range counts {
		selected += c[0]
	}
	b.WriteString(fmt.Sprintf("\nВыбрано: %d из %d%s", selected, len(models), diffBracket(models, draft, saved)))

	var rows [][]InlineButton
	var pair []InlineButton
	for _, fam := range families {
		c := counts[fam]
		btn := InlineButton{
			Text:         fmt.Sprintf("%s %d/%d", EscapeHTML(fam), c[0], c[1]),
			CallbackData: cbData("f", fam),
		}
		pair = append(pair, btn)
		if len(pair) == 2 {
			rows = append(rows, pair)
			pair = nil
		}
	}
	if len(pair) > 0 {
		rows = append(rows, pair)
	}
	rows = append(rows, []InlineButton{
		{Text: "Выбрать всё", CallbackData: "all"},
		{Text: "Снять всё", CallbackData: "none"},
	})
	rows = append(rows, []InlineButton{
		{Text: "Сохранить", CallbackData: "save"},
		{Text: "Отменить", CallbackData: "cancel"},
	})
	return b.String(), &InlineKeyboardMarkup{InlineKeyboard: rows}
}

// RenderSubscribeFamily — экран одного семейства: модели столбцом с
// идентификаторами в кнопках ("s:<model-id>"), масс-операции семейства,
// Назад/Отменить/Сохранить.
func RenderSubscribeFamily(family string, allModels []storage.Model, draft, saved map[string]bool) (string, *InlineKeyboardMarkup) {
	var b strings.Builder
	b.WriteString("<b>" + EscapeHTML(family) + "</b>\n\n")

	var rows [][]InlineButton
	var scope []storage.Model
	selected, total := 0, 0
	for _, m := range allModels {
		if m.Family != family {
			continue
		}
		total++
		scope = append(scope, m)
		label := EscapeHTML(m.ID)
		if draft[m.ID] {
			label = "+ " + label
			selected++
		}
		rows = append(rows, []InlineButton{{Text: label, CallbackData: cbData("s", m.ID)}})
	}
	b.WriteString(fmt.Sprintf("Выбрано: %d из %d%s", selected, total, diffBracket(scope, draft, saved)))
	rows = append(rows, []InlineButton{
		{Text: "Выбрать всё", CallbackData: cbData("fa", family)},
		{Text: "Снять всё", CallbackData: cbData("fn", family)},
	})
	rows = append(rows, []InlineButton{
		{Text: "Назад", CallbackData: "back"},
		{Text: "Отменить", CallbackData: "cancel"},
		{Text: "Сохранить", CallbackData: "save"},
	})
	return b.String(), &InlineKeyboardMarkup{InlineKeyboard: rows}
}

// FamiliesOf — семейства в порядке конфигурации без повторов.
func FamiliesOf(models []storage.Model) []string {
	var out []string
	seen := map[string]bool{}
	for _, m := range models {
		if !seen[m.Family] {
			seen[m.Family] = true
			out = append(out, m.Family)
		}
	}
	return out
}

// RenderHelp renders /help.
func RenderHelp() string {
	return strings.Join([]string{
		"OpenTW - мониторинг доступности моделей.",
		"",
		"/status - статус всех моделей (панель закрепляется и обновляется сама).",
		"/status model-id - детали: метрики, статистика 24ч, последняя ошибка.",
		"/subscribe - выбрать модели для уведомлений.",
		"/start - то же, что /status.",
		"/help - эта справка.",
		"",
		"Статусы: 🟢 работает · 🟡 нестабильно · 🔴 недоступна · ⚪ нет данных.",
		"Latency, TPS и токены - метрики одного проверочного запроса (каждые 15 минут).",
		"Уведомления приходят только по выбранным моделям; панель /status показывает все.",
		"Отключить надоевшую модель: /subscribe -> семейство -> снять отметку -> Сохранить.",
	}, "\n")
}

// RenderSubscribedSaved подтверждает сохранение подписок.
func RenderSubscribedSaved(count int) string {
	if count == 0 {
		return "Уведомления отключены: подписок нет.\nСтатус всех моделей по-прежнему виден в /status."
	}
	return "Подписки сохранены: " + pluralModels(count) + ".\nУведомления будут приходить при изменении состояния."
}
