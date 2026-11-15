package telegram

import (
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
	absent      = "N/A"
	maxPanelErr = 30 // панель — одна строка на десктопе; полный текст в /status model-id
)

func fmtClockMSK(t time.Time) string { return t.In(MSK).Format("15:04:05") }

func fmtSecs(us int64) string  { return fmt.Sprintf("%.2fs", float64(us)/1e6) }
func fmtSecs1(us int64) string { return fmt.Sprintf("%.1fs", float64(us)/1e6) }

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

// RenderStatusPanel renders the pinned /status panel: one line per model,
// grouped by family, exact model IDs, N/A for unknown metrics.
func RenderStatusPanel(models []storage.Model, checks map[string]storage.Check, updatedAt, nextAt time.Time) string {
	var b strings.Builder
	b.WriteString("OpenTW\n\n")
	b.WriteString(fmt.Sprintf("Обновлено: %s MSK\n", fmtClockMSK(updatedAt)))
	b.WriteString(fmt.Sprintf("Следующая проверка: %s MSK\n", fmtClockMSK(nextAt)))

	family := ""
	for _, m := range models {
		if m.Family != family {
			family = m.Family
			b.WriteString("\n<b>" + EscapeHTML(family) + "</b>\n")
		}
		b.WriteString(modelLine(m, checks[m.ID]))
		b.WriteString("\n")
	}
	return b.String()
}

// codeID оборачивает model ID в моноширинный формат Telegram.
func codeID(id string) string { return "<code>" + EscapeHTML(id) + "</code>" }

// modelLine формирует строку одной модели (обязательно одну).
func modelLine(m storage.Model, c storage.Check) string {
	if c.Status == "" {
		// ещё нет ни одной проверки этой модели
		return "⚪ " + codeID(m.ID) + " - нет данных"
	}
	emoji := c.Status.Emoji()

	switch {
	case c.ErrorType == "perf":
		return emoji + " " + codeID(m.ID) + " - " + metricsTail(c) + " | деградация"
	case c.ErrorType == "empty":
		return emoji + " " + codeID(m.ID) + " - HTTP 200 | пустая генерация"
	case c.ErrorType != "":
		return emoji + " " + codeID(m.ID) + " - " + errorHead(c)
	default:
		return emoji + " " + codeID(m.ID) + " - " + metricsTail(c)
	}
}

// errorHead формирует краткое описание ошибки для панели.
func errorHead(c storage.Check) string {
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
	return head + " | " + EscapeHTML(msg)
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
	return strings.Join(parts, " | ")
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
		return head + " | " + EscapeHTML(oneLine(c.ErrorMessage))
	}
	return head
}

// RenderModelDetail renders the /status model-id view.
func RenderModelDetail(modelID string, st status.Status, last storage.Check, last24 []storage.Check) string {
	var b strings.Builder
	b.WriteString(EscapeHTML(modelID) + " — " + st.Emoji() + " " + st.Title() + "\n\n")
	b.WriteString("Последняя проверка: " + fmtClockMSK(last.At) + " MSK\n\n")

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
		var okCount, downCount int
		var latencies []float64
		var tpsVals []float64
		for _, c := range last24 {
			switch c.Status {
			case status.Operational:
				okCount++
			case status.Down:
				downCount++
			}
			if c.LatencyUS != nil && c.Status == status.Operational {
				latencies = append(latencies, float64(*c.LatencyUS)/1e6)
			}
			if c.TPS != nil && *c.TPS > 0 && c.Status == status.Operational {
				tpsVals = append(tpsVals, *c.TPS)
			}
		}
		uptime := float64(len(last24)-downCount) / float64(len(last24)) * 100
		b.WriteString(fmt.Sprintf("Uptime: %.2f%%\n", uptime))
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
		b.WriteString(fmtClockMSK(lastErr.At) + " | " + detailErrorLine(*lastErr))
	}
	return b.String()
}

func metricOrNA(ok bool, v string) string {
	if !ok {
		return absent
	}
	return v
}

func secPtr(v *int64) string {
	if v == nil {
		return absent
	}
	return fmtSecs(*v)
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

// RenderSubscribePanel renders the /subscribe view and its inline keyboard.
// Callback data: "s:<index>" по порядку отсортированного списка моделей,
// "save" — сохранение. Индексы стабильны, пока не изменён конфиг.
func RenderSubscribePanel(models []storage.Model, subscribed map[string]bool) (string, *InlineKeyboardMarkup) {
	var b strings.Builder
	b.WriteString("OpenTW — Настройка уведомлений\n\n")
	b.WriteString("Выберите модели, об изменениях состояния\nкоторых хотите получать уведомления.\n")

	rows := make([][]InlineButton, 0, len(models)+1)
	selected := 0
	for i, m := range models {
		label := EscapeHTML(m.ID)
		if subscribed[m.ID] {
			label = "✓ " + label
			selected++
		}
		rows = append(rows, []InlineButton{
			{Text: label, CallbackData: fmt.Sprintf("s:%d", i)},
		})
	}
	b.WriteString("\nВыбрано: " + pluralModels(selected))
	rows = append(rows, []InlineButton{{Text: "Сохранить", CallbackData: "save"}})
	return b.String(), &InlineKeyboardMarkup{InlineKeyboard: rows}
}

// RenderHelp renders /help.
func RenderHelp() string {
	return strings.Join([]string{
		"OpenTW",
		"",
		"/start — показать статус моделей.",
		"/status — общий статус всех моделей.",
		"/status model-id — подробная информация о конкретной модели.",
		"/subscribe — настройка уведомлений об изменениях состояния моделей.",
		"/help — справка.",
	}, "\n")
}

// RenderSubscribedSaved подтверждает сохранение подписок.
func RenderSubscribedSaved(count int) string {
	return "Подписки сохранены: " + pluralModels(count) + ".\nУведомления будут приходить при изменении состояния."
}
