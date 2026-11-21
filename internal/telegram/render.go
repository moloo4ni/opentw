package telegram

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"html"
	"strings"
	"time"
	"unicode/utf8"

	"opentw/internal/status"
	"opentw/internal/storage"
)

// MSK — часовой пояс всех времён в интерфейсе OpenTW.
var MSK = time.FixedZone("MSK", 3*60*60)

const (
	maxRichMessage  = 32768 // лимит rich-сообщения в рунах
	maxCallbackData = 64    // лимит Telegram на callback_data в байтах
)

func fmtClockMSK(t time.Time) string { return t.In(MSK).Format("15:04:05") }

// fmtDateClockMSK — дата+время: панель трёхдневной свежести должна отличаться
// от свежей.
func fmtDateClockMSK(t time.Time) string { return t.In(MSK).Format("02.01.2006 15:04:05") }

// intervalHuman — интервал проверки словами для справки.
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

// fmtSecs1 — единая точность времени в карточке: один знак после запятой.
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

// statusWordRu — единый словарь пользовательских названий трёх статусов:
// легенда панели и /help, заголовок карточки, строки моделей и подпись
// «Нет данных» у ошибок берут слово только отсюда.
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

// panelLegend — расшифровка кружков, последняя строка панели и справки.
func panelLegend() string {
	return status.Operational.Emoji() + " " + statusWordRu(status.Operational) +
		" · " + status.Down.Emoji() + " " + statusWordRu(status.Down) +
		" · " + status.Unknown.Emoji() + " " + statusWordRu(status.Unknown)
}

// shortErrorLabel — единая короткая подпись ошибки по проверке:
// панель, уведомления и карточки строятся на ней. Operational не вызывается.
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
	default: // unknown, нет данных, прочие мониторные варианты
		return statusWordRu(status.Unknown)
	}
}

// stButtonData кодирует модель кнопки-ошибки: точный id; длинные id —
// хэш-форма, как у всех callback-кнопок.
func stButtonData(id string) string { return cbData("st", id) }

// richErrorButton — кликабельная ошибка модели: текст и атрибут data
// экранируются; визуально — ссылка без рамки.
func richErrorButton(id, label string) string {
	return fmt.Sprintf(`<tg-button type="callback_data" style="link" data="%s">%s</tg-button>`,
		EscapeHTML(stButtonData(id)), EscapeHTML(label))
}

// codeID оборачивает model ID в моноширинный формат Telegram.
func codeID(id string) string { return "<code>" + EscapeHTML(id) + "</code>" }

// downModelLine — строка недоступной модели: единый HTML для главной панели
// и уведомлений, различаются только caller-данные (model ID и label ошибки).
func downModelLine(modelID, label string) string {
	return "🔴 " + codeID(modelID) + " - " + richErrorButton(modelID, label)
}

// modelLine — rich-строка одной модели: Operational — только latency;
// Down/Unknown/старые статусы — кнопка с подписью; порядок конфига задаёт
// вызывающий код. Возвращает строку без обёртки: панели склеивают строки
// семейства в один блок с <br/>, уведомления оборачивают в <p> сами.
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
	default: // Unknown и старые статусы из payload — без паники
		return "⚪ " + id + " - " + richErrorButton(m.ID, statusWordRu(status.Unknown))
	}
}

// RenderStatusPanel renders the pinned /status panel: без сводки,
// порядок строк — только конфиг,Operational показывает latency.
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
		// до первого цикла честно предупреждаем вместо фальшивого «Обновлено: сейчас»
		b.WriteString("<p>Первый цикл проверки выполняется - панель обновится автоматически</p>")
	}

	// Семейство — один смысловой блок: заголовок и строки моделей через <br/>;
	// обычные \n в rich-HTML игнорируются, визуальные интервалы дают
	// только границы блоков <p>.
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

// renderModelCard — карточка и не поместившийся в лимит хвост секции «Ответ»
// (сырой, неэкранированный текст): молчаливая обрезка данных запрещена.
// Бюджет считается в ЭКРАНИРОВАННЫХ рунах: EscapeHTML растит спец-руны
// в 4–6 раз, лимит rich-сообщения нарушается именно после экранирования.
func renderModelCard(modelID string, st status.Status, last storage.Check, last24 []storage.Check) (string, string) {
	card := buildModelCard(modelID, st, last, last24, EscapeHTML(last.Response))
	if last.Response == "" || utf8.RuneCountInString(card) <= maxRichMessage {
		return card, ""
	}
	// фиксированная часть задаёт бюджет ответа; обёртка секции = карточка
	// с одной руной ответа минус карточка без ответа минус эта руна
	fixed := utf8.RuneCountInString(buildModelCard(modelID, st, last, last24, ""))
	one := utf8.RuneCountInString(buildModelCard(modelID, st, last, last24, "x"))
	overhead := one - fixed - 1
	budget := maxRichMessage - fixed - overhead
	resp := []rune(last.Response)
	// наибольший префикс, экранирование которого укладывается в бюджет
	// (бинарный поиск: экранированная длина монотонна по префиксу)
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

// escapedRunes — длина s в рунах ПОСЛЕ EscapeHTML: каждая спец-руна
// становится entity из 4–6 символов, и именно экранированный текст
// ограничен лимитом rich-сообщения.
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

// buildModelCard — карточка модели; respHTML — экранированный дословный ответ
// ("" — секция опускается). Поля без значения не выводятся.
func buildModelCard(modelID string, st status.Status, last storage.Check, last24 []storage.Check, respHTML string) string {
	var b strings.Builder
	// Заголовок: эмодзи, model ID (<code>, как в строках панели) и статус
	// в одной строке
	b.WriteString("<p>" + st.Emoji() + " " + codeID(modelID) + " - " + statusWordRu(st) + "</p>")
	if !last.At.IsZero() {
		// 24-часовое окно пересекает полночь: отметки с датой
		b.WriteString("<p>Последняя проверка: " + fmtDateClockMSK(last.At) + " MSK</p>")
	}
	b.WriteString(requestBlock(last))
	if respHTML != "" {
		title := "Ответ API"
		// Текст — генерация самой модели: успешный ответ («», т.е. OK) или
		// неожиданный ответ вместо OK («response»); остальное — текст API.
		if last.ErrorType == "" || last.ErrorType == "response" {
			title = "Ответ модели"
		}
		b.WriteString("<p><b>" + title + "</b></p><p>" + respHTML + "</p>")
	} else if last.Response == "" && last.HTTPStatus == 200 &&
		(last.ErrorType == "" || last.ErrorType == "response" || last.ErrorType == "empty") {
		// HTTP 200 был, но итогового пользовательского текста нет. Это факт,
		// а не интерпретация причины; reasoning из отдельного поля не показываем.
		b.WriteString("<p><b>Ответ модели</b></p><p>Итоговый текст отсутствует.</p>")
	}
	if stats := last24Block(last24); stats != "" {
		b.WriteString("<p><b>За последние 24 часа</b></p>")
		b.WriteString(stats)
	}
	return b.String()
}

// requestBlock — блок «Последний запрос»: HTTP-код и latency. HTTP-ответ
// получен — «Latency», даже если содержимое не прошло проверку (включая
// 4xx/5xx: сервер ответил). «Время до ошибки» — только когда HTTP-ответа
// не было вовсе (timeout, connection failure) и длительность измерена.
// Время везде — одним знаком после запятой. Пустые поля опускаются.
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
	// Диагностика выводится только когда итогового текста нет. nil не
	// превращается в ноль, отсутствующий finish_reason не выдумывается.
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

// finishReason извлекает только значение служебного диагностического
// finish_reason=...; обычный текст провайдера не интерпретируется.
func finishReason(message string) string {
	for _, part := range strings.Split(message, "; ") {
		if reason, ok := strings.CutPrefix(part, "finish_reason="); ok && reason != "" {
			return reason
		}
	}
	return ""
}

// last24Block — статистика 24 часов: Uptime = ok/(ok+down), Unknown не
// входит в знаменатель; блок целиком опускается без истории. Каждая строка —
// самостоятельный абзац <p>: вертикальные интервалы между абзацами задаёт
// Telegram, как и между остальными секциями карточки, поэтому вызывающий
// код вставляет результат без дополнительной обёртки.
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
		default: // Unknown и старые статусы
			unknownCount++
		}
	}
	var lines []string
	if n := okCount + downCount; n > 0 {
		// 23/24 → 95.83%: две значащие цифры, как в согласованном формате
		lines = append(lines, fmt.Sprintf("Uptime: %.2f%%", float64(okCount)/float64(n)*100))
	}
	lines = append(lines, fmt.Sprintf("Проверок: %d", len(checks)))
	// статусные эмодзи — только в строке категорий; слово Unknown — из словаря
	lines = append(lines, fmt.Sprintf("🟢 Успешных: %d · 🔴 Неудачных: %d · ⚪ %s: %d", okCount, downCount, statusWordRu(status.Unknown), unknownCount))
	// каждая строка — отдельный rich-абзац: без <br/> и начальных пробелов,
	// все строки начинаются от одного левого края
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
// модель/семейство (устаревшая клавиатура). Точное совпадение, потом
// хэш-форма; кандидатов по префиксу нет.
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

// modelIDs — список ID моделей (кандидаты для кнопок "s:"/"st:").
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

// RenderHelp renders /help; интервал проверки — из фактической конфигурации
// (0 — нейтральная формулировка «каждый цикл проверки»).
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

// RenderSubscribedSaved подтверждает сохранение подписок.
func RenderSubscribedSaved(count int) string {
	if count == 0 {
		return "Уведомления отключены: подписок нет.\nСтатус всех моделей по-прежнему виден в /status."
	}
	return "Подписки сохранены: " + pluralModels(count) + ".\nУведомления будут приходить при изменении состояния."
}
