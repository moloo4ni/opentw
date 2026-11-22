package telegram

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"html"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/moloo4ni/opentw/internal/status"
	"github.com/moloo4ni/opentw/internal/storage"
)

// MSK is the timezone of all times shown in the OpenTW UI.
var MSK = time.FixedZone("MSK", 3*60*60)

const (
	maxRichMessage  = 32768 // rich-message limit, in runes
	maxCallbackData = 64    // Telegram's callback_data limit, in bytes
)

func fmtClockMSK(t time.Time) string { return t.In(MSK).Format("15:04:05") }

// fmtDateClockMSK prints date+time: a three-day-old panel must differ from
// a fresh one.
func fmtDateClockMSK(t time.Time) string { return t.In(MSK).Format("02.01.2006 15:04:05") }

// intervalHuman renders the check interval in words for the help text.
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

// fmtSecs1 is the single time precision of the card: one decimal digit.
func fmtSecs1(us int64) string { return fmt.Sprintf("%.1fs", float64(us)/1e6) }

// EscapeHTML escapes any external string before it goes to Telegram.
func EscapeHTML(s string) string { return html.EscapeString(s) }

func trunc(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "..."
}

// statusWordRu is the single dictionary of the three statuses' user-facing
// names: the panel legend and /help, the card header, model lines and the
// no-data error label all take the word from here only.
func statusWordRu(st status.Status) string {
	switch st {
	case status.Operational:
		return "Работает"
	case status.Down:
		return "Недоступна"
	default:
		return "Нет данных"
	}
}

// panelLegend is the circles legend, the last line of the panel and help.
func panelLegend() string {
	return status.Operational.Emoji() + " " + statusWordRu(status.Operational) +
		" · " + status.Down.Emoji() + " " + statusWordRu(status.Down) +
		" · " + status.Unknown.Emoji() + " " + statusWordRu(status.Unknown)
}

// shortErrorLabel is the single short error label of a check: the panel,
// notifications and cards are all built on it. Never called for Operational.
func shortErrorLabel(c storage.Check) string {
	switch c.ErrorType {
	case "http":
		return fmt.Sprintf("HTTP %d", c.HTTPStatus)
	case "timeout":
		return "Timeout"
	case "connrefused":
		return "Connection refused"
	case "response", "empty":
		return "Unexpected response"
	default: // unknown, no data and other monitor-side cases
		return statusWordRu(status.Unknown)
	}
}

// stButtonData encodes the error-button model: exact id; long ids take the
// hash form, like all callback buttons.
func stButtonData(id string) string { return cbData("st", id) }

// richErrorButton is a clickable model error: text and data attribute are
// escaped; visually a borderless link.
func richErrorButton(id, label string) string {
	return fmt.Sprintf(`<tg-button type="callback_data" style="link" data="%s">%s</tg-button>`,
		EscapeHTML(stButtonData(id)), EscapeHTML(label))
}

// codeID wraps a model ID in Telegram's monospace format.
func codeID(id string) string { return "<code>" + EscapeHTML(id) + "</code>" }

// downModelLine is an unavailable model's line: shared HTML for the main
// panel and notifications; only the caller data differs (model ID, error label).
func downModelLine(modelID, label string) string {
	return "🔴 " + codeID(modelID) + " - " + richErrorButton(modelID, label)
}

// modelLine is one model's rich line: Operational shows only latency;
// Down/Unknown/stale statuses get a labelled button; the caller sets the
// config order. Returns the bare line: panels join a family's lines into
// one block with <br/>, notifications wrap in <p> themselves.
func modelLine(m storage.Model, c storage.Check) string {
	id := codeID(m.ID)
	switch c.Status {
	case status.Operational:
		if c.LatencyUS != nil {
			return "🟢 " + id + " - " + fmtSecs1(*c.LatencyUS)
		}
		return "🟢 " + id
	case status.Down:
		return downModelLine(m.ID, shortErrorLabel(c))
	default: // Unknown and stale statuses from the payload — no panic
		return "⚪ " + id + " - " + richErrorButton(m.ID, statusWordRu(status.Unknown))
	}
}

// RenderStatusPanel renders the pinned /status panel: no summary, line order
// follows the config only, Operational shows latency.
func RenderStatusPanel(models []storage.Model, checks map[string]storage.Check, updatedAt, nextAt time.Time) string {
	var b strings.Builder
	b.WriteString("<p><b>OpenTW</b></p>")

	checked := false
	for _, m := range models {
		if c, ok := checks[m.ID]; ok && c.Status != "" {
			checked = true
			break
		}
	}
	if !checked {
		// before the first cycle an honest notice beats a fake "updated now"
		b.WriteString("<p>Первый цикл проверки выполняется - панель обновится автоматически</p>")
	}

	// A family is one semantic block: a header plus model lines joined with
	// <br/>; plain \n is ignored in rich HTML, only <p> boundaries create
	// visual spacing.
	family := ""
	var block []string
	flush := func() {
		if len(block) == 0 {
			return
		}
		b.WriteString("<p><b>" + EscapeHTML(family) + "</b><br/>" + strings.Join(block, "<br/>") + "</p>")
		block = block[:0]
	}
	for _, m := range models {
		if m.Family != family {
			flush()
			family = m.Family
		}
		block = append(block, modelLine(m, checks[m.ID]))
	}
	flush()

	if checked {
		b.WriteString("<p>Обновлено: " + fmtDateClockMSK(updatedAt) + " MSK</p>")
		b.WriteString("<p>Следующая проверка: " + fmtClockMSK(nextAt) + " MSK</p>")
	}
	b.WriteString("<p>" + panelLegend() + "</p>")
	return b.String()
}

// renderModelCard returns the card and the tail of the "Response" section
// that exceeds the limit (raw, unescaped text): silent data truncation is
// forbidden. The budget is counted in ESCAPED runes: EscapeHTML grows
// special runes 4–6×, and the rich-message limit applies after escaping.
func renderModelCard(modelID string, st status.Status, last storage.Check, last24 []storage.Check) (string, string) {
	card := buildModelCard(modelID, st, last, last24, EscapeHTML(last.Response))
	if last.Response == "" || utf8.RuneCountInString(card) <= maxRichMessage {
		return card, ""
	}
	// the fixed part sets the response budget; the section's wrapper overhead
	// is the card with a one-rune response minus the card without one, minus
	// that rune
	fixed := utf8.RuneCountInString(buildModelCard(modelID, st, last, last24, ""))
	one := utf8.RuneCountInString(buildModelCard(modelID, st, last, last24, "x"))
	overhead := one - fixed - 1
	budget := maxRichMessage - fixed - overhead
	resp := []rune(last.Response)
	// the longest prefix whose escaping fits the budget
	// (binary search: escaped length is monotone in the prefix)
	lo, hi := 0, len(resp)
	for lo < hi {
		mid := (lo + hi + 1) / 2
		if escapedRunes(string(resp[:mid])) <= budget {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return buildModelCard(modelID, st, last, last24, EscapeHTML(string(resp[:lo]))), string(resp[lo:])
}

// escapedRunes is the length of s in runes AFTER EscapeHTML: every special
// rune becomes a 4–6 character entity, and it is the escaped text that is
// bounded by the rich-message limit.
func escapedRunes(s string) int {
	n := 0
	for _, r := range s {
		n += escapedRuneLen(r)
	}
	return n
}

func escapedRuneLen(r rune) int {
	switch r {
	case '&':
		return 5 // &amp;
	case '<', '>':
		return 4 // &lt; / &gt;
	case '\'':
		return 5 // &#39;
	case '"':
		return 6 // &quot;
	default:
		return 1
	}
}

// buildModelCard is the model card; respHTML is the escaped verbatim
// response ("" — the section is omitted). Empty fields are not printed.
func buildModelCard(modelID string, st status.Status, last storage.Check, last24 []storage.Check, respHTML string) string {
	var b strings.Builder
	// Header: emoji, model ID (<code>, like the panel lines) and status,
	// all on one line
	b.WriteString("<p>" + st.Emoji() + " " + codeID(modelID) + " - " + statusWordRu(st) + "</p>")
	if !last.At.IsZero() {
		// the 24h window crosses midnight: timestamps carry the date
		b.WriteString("<p>Последняя проверка: " + fmtDateClockMSK(last.At) + " MSK</p>")
	}
	b.WriteString(requestBlock(last))
	if respHTML != "" {
		title := "Ответ API"
		// The text is the model's own generation: a successful answer ("", i.e.
		// OK) or an unexpected answer instead of OK ("response"); the rest is
		// API text.
		if last.ErrorType == "" || last.ErrorType == "response" {
			title = "Ответ модели"
		}
		b.WriteString("<p><b>" + title + "</b></p><p>" + respHTML + "</p>")
	} else if last.Response == "" && last.HTTPStatus == 200 &&
		(last.ErrorType == "" || last.ErrorType == "response" || last.ErrorType == "empty") {
		// HTTP 200 arrived but there is no final user text. A fact, not an
		// interpretation of the cause; reasoning from a separate field is not
		// shown.
		b.WriteString("<p><b>Ответ модели</b></p><p>Итоговый текст отсутствует.</p>")
	}
	if stats := last24Block(last24); stats != "" {
		b.WriteString("<p><b>За последние 24 часа</b></p>")
		b.WriteString(stats)
	}
	return b.String()
}

// requestBlock is the "Last request" block: HTTP code and latency. An HTTP
// response arrived — "Latency", even when the content failed the check
// (including 4xx/5xx: the server answered). The time-to-error label appears
// only when there was no HTTP response at all (timeout, connection failure)
// and the duration was measured. Times use one decimal digit. Empty fields
// are omitted.
func requestBlock(c storage.Check) string {
	var parts []string
	if c.HTTPStatus > 0 {
		parts = append(parts, fmt.Sprintf("HTTP %d", c.HTTPStatus))
	}
	if c.LatencyUS != nil {
		lat := fmtSecs1(*c.LatencyUS)
		if c.HTTPStatus > 0 {
			parts = append(parts, "Latency "+lat)
		} else {
			parts = append(parts, "Время до ошибки "+lat)
		}
	}
	// Diagnostics are shown only when there is no final text. nil never turns
	// into zero; a missing finish_reason is not invented.
	if c.Response == "" {
		if c.OutputTokens != nil {
			parts = append(parts, fmt.Sprintf("Токенов ответа: %d", *c.OutputTokens))
		}
		if reason := finishReason(c.ErrorMessage); reason != "" {
			parts = append(parts, "Завершение: "+EscapeHTML(reason))
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return "<p><b>Последний запрос</b></p><p>" + strings.Join(parts, " · ") + "</p>"
}

// finishReason extracts only the service diagnostic finish_reason=...
// value; plain provider text is not interpreted.
func finishReason(message string) string {
	for _, part := range strings.Split(message, "; ") {
		if reason, ok := strings.CutPrefix(part, "finish_reason="); ok && reason != "" {
			return reason
		}
	}
	return ""
}

// last24Block is the 24-hour statistics: Uptime = ok/(ok+down), Unknown
// excluded from the denominator; the whole block is omitted without
// history. Every line is its own <p> paragraph: Telegram sets the vertical
// spacing between paragraphs as between the card's other sections, so the
// caller inserts the result without an extra wrapper.
func last24Block(checks []storage.Check) string {
	if len(checks) == 0 {
		return ""
	}
	var okCount, downCount, unknownCount int
	for _, c := range checks {
		switch c.Status {
		case status.Operational:
			okCount++
		case status.Down:
			downCount++
		default: // Unknown and stale statuses
			unknownCount++
		}
	}
	var lines []string
	if n := okCount + downCount; n > 0 {
		// 23/24 → 95.83%: two significant digits, the agreed format
		lines = append(lines, fmt.Sprintf("Uptime: %.2f%%", float64(okCount)/float64(n)*100))
	}
	lines = append(lines, fmt.Sprintf("Проверок: %d", len(checks)))
	// status emojis only in the category line; the Unknown word comes from
	// the dictionary
	lines = append(lines, fmt.Sprintf("🟢 Успешных: %d · 🔴 Неудачных: %d · ⚪ %s: %d", okCount, downCount, statusWordRu(status.Unknown), unknownCount))
	// every line is a separate rich paragraph: no <br/>, no leading spaces,
	// all lines share one left edge
	return "<p>" + strings.Join(lines, "</p><p>") + "</p>"
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

// cbData encodes a model/family id into a callback button: the id goes
// as-is — it is stable across config reordering and restarts (a positional
// index is not); long ids collapse to a stable hash (prefixed cbData does
// not fit the limit).
func cbData(prefix, id string) string {
	if len(prefix)+len(id)+1 <= maxCallbackData {
		return prefix + ":" + id
	}
	return prefix + ":h:" + shortHash(id)
}

// shortHash is 10 hex chars of sha256: collision-free enough for dozens of ids.
func shortHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:5])
}

// resolveCallback matches the pressed button's payload against the current
// configuration; ok=false means the button references a removed
// model/family (a stale keyboard). Exact match first, then the hash form;
// no prefix candidates.
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

// modelIDs lists the model IDs (candidates for "s:"/"st:" buttons).
func modelIDs(models []storage.Model) []string {
	ids := make([]string, len(models))
	for i, m := range models {
		ids[i] = m.ID
	}
	return ids
}

// diffBracket is "(+A -R)" against the saved set; empty string when equal.
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

// RenderSubscribeRoot is the first /subscribe screen: family buttons with
// selected counts, bulk operations, Save and Cancel. Callback data: "f:<family>".
func RenderSubscribeRoot(models []storage.Model, draft, saved map[string]bool) (string, *InlineKeyboardMarkup) {
	var b strings.Builder
	b.WriteString("OpenTW - Настройка уведомлений\n\n")
	b.WriteString("Уведомления приходят только по выбранным моделям при смене их состояния. Панель /status не зависит от подписок.\n")

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

// RenderSubscribeFamily is the family screen: models in a column with ids
// in the buttons ("s:<model-id>"), family bulk operations, Back/Cancel/Save.
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

// FamiliesOf lists families in config order without repeats.
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

// RenderHelp renders /help; the check interval comes from the actual
// configuration (0 — the neutral per-cycle wording).
func RenderHelp(interval time.Duration) string {
	perCycle := "каждый цикл проверки"
	if interval > 0 {
		perCycle = "каждые " + intervalHuman(interval)
	}
	return strings.Join([]string{
		"OpenTW - мониторинг доступности моделей.",
		"",
		"/status - статус всех моделей (панель закрепляется и обновляется сама).",
		"/status model-id - карточка модели: последняя проверка, ответ и статистика 24ч.",
		"/subscribe - выбрать модели для уведомлений.",
		"/start - то же, что /status.",
		"/help - эта справка.",
		"",
		"Нажатие на ошибку в панели или уведомлении открывает карточку модели.",
		"Статусы: " + panelLegend(),
		"Уведомления приходят по выбранным моделям при смене состояния (" + perCycle + ").",
		"Отключить надоевшую модель: /subscribe -> семейство -> снять отметку -> Сохранить.",
	}, "\n")
}

// RenderSubscribedSaved confirms the saved subscriptions.
func RenderSubscribedSaved(count int) string {
	if count == 0 {
		return "Уведомления отключены: подписок нет.\nСтатус всех моделей по-прежнему виден в /status."
	}
	return "Подписки сохранены: " + pluralModels(count) + ".\nУведомления будут приходить при изменении состояния."
}
