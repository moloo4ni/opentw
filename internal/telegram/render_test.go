package telegram

import (
	"os"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"gopkg.in/yaml.v3"

	"opentw/internal/status"
	"opentw/internal/storage"
)

func mskTime(t *testing.T, h, m, s int) time.Time {
	t.Helper()
	// полночь MSK = 21:00 UTC предыдущего дня; время задаём в MSK
	base := time.Date(2026, 11, 15, 21, 0, 0, 0, time.UTC)
	return base.Add(time.Duration(h)*time.Hour + time.Duration(m)*time.Minute + time.Duration(s)*time.Second)
}

func ptr1(v int64) *int64     { return &v }
func ptrF(v float64) *float64 { return &v }

// stBtn — кнопка-ошибка модели: точный ожидаемый фрагмент строки.
func stBtn(id, label string) string {
	return `<tg-button type="callback_data" style="link" data="st:` + id + `">` + label + `</tg-button>`
}

// Панель успешных моделей: заголовок, семейства, latency в строке,
// футер с датой, легенда последней строкой, БЕЗ сводки и метрик.
func TestPanelFormatOperational(t *testing.T) {
	models := []storage.Model{
		{ID: "gpt-6-astra", Family: "GPT"},
		{ID: "glm-5.3", Family: "GLM"},
	}
	checks := map[string]storage.Check{
		"gpt-6-astra": {ModelID: "gpt-6-astra", Status: status.Operational, HTTPStatus: 200,
			LatencyUS: ptr1(2_700_000), TTFTUS: ptr1(420_000), GenUS: ptr1(2_280_000),
			TPS: ptrF(42.3), InputTokens: ptr1(154), OutputTokens: ptr1(96), Response: "OK"},
		"glm-5.3": {ModelID: "glm-5.3", Status: status.Operational, HTTPStatus: 200,
			LatencyUS: ptr1(4_200_000), TTFTUS: ptr1(630_000), TPS: ptrF(54.1)},
	}
	text := RenderStatusPanel(models, checks, mskTime(t, 16, 15, 3), mskTime(t, 16, 30, 0))
	for _, want := range []string{
		"<p><b>OpenTW</b></p>",
		"<p><b>GPT</b><br/>🟢 <code>gpt-6-astra</code> - 2.7s</p>",
		"<p><b>GLM</b><br/>🟢 <code>glm-5.3</code> - 4.2s</p>",
		"<p>Обновлено: 16.11.2026 16:15:03 MSK</p>",
		"<p>Следующая проверка: 16:30:00 MSK</p>",
		"<p>🟢 Работает · 🔴 Недоступна · ⚪ Нет данных</p>",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("panel missing %q:\n%s", want, text)
		}
	}
	for _, bad := range []string{
		"Модели:", // сводка удалена
		"TPS",     // метрики удалены
		"TTFT",
		"tokens",
		"154",
		"42.3",
	} {
		if strings.Contains(text, bad) {
			t.Errorf("panel must not contain %q:\n%s", bad, text)
		}
	}
}

// Матрица строк панели: три статуса, кнопка у ошибок, latency у Operational,
// Operational без latency — без « - »; семейство — один блок с <br/>.
func TestPanelThreeStatusLines(t *testing.T) {
	models := []storage.Model{
		{ID: "m-down", Family: "M"},
		{ID: "m-un", Family: "M"},
		{ID: "m-ok", Family: "M"},
		{ID: "m-ok-nolat", Family: "M"},
	}
	checks := map[string]storage.Check{
		"m-down":     {Status: status.Down, HTTPStatus: 404, ErrorType: "http", ErrorMessage: "Invalid model."},
		"m-un":       {Status: status.Unknown, ErrorType: "timeout", ErrorMessage: "deadline"},
		"m-ok":       {Status: status.Operational, HTTPStatus: 200, LatencyUS: ptr1(2_000_000)},
		"m-ok-nolat": {Status: status.Operational},
	}
	text := RenderStatusPanel(models, checks, time.Now(), time.Now())
	want := "<p><b>M</b><br/>" +
		"🔴 <code>m-down</code> - " + stBtn("m-down", "HTTP 404") + "<br/>" +
		"⚪ <code>m-un</code> - " + stBtn("m-un", "Нет данных") + "<br/>" +
		"🟢 <code>m-ok</code> - 2.0s<br/>" +
		"🟢 <code>m-ok-nolat</code></p>"
	if !strings.Contains(text, want) {
		t.Errorf("panel family block wrong, want:\n%s\ngot:\n%s", want, text)
	}
	// служебное сообщение об ошибке в панель не показывается — есть кнопка с label
	if strings.Contains(text, "Invalid model.") || strings.Contains(text, "deadline") {
		t.Fatalf("error messages must not surface in the panel:\n%s", text)
	}
}

// Единая короткая подпись ошибки — вся матрица только в этих вариантах.
func TestShortErrorLabel(t *testing.T) {
	for _, tc := range []struct {
		name string
		c    storage.Check
		want string
	}{
		{"http 400", storage.Check{ErrorType: "http", HTTPStatus: 400}, "HTTP 400"},
		{"http 404", storage.Check{ErrorType: "http", HTTPStatus: 404}, "HTTP 404"},
		{"http 429", storage.Check{ErrorType: "http", HTTPStatus: 429}, "HTTP 429"},
		{"http 503", storage.Check{ErrorType: "http", HTTPStatus: 503}, "HTTP 503"},
		{"timeout", storage.Check{ErrorType: "timeout"}, "Timeout"},
		{"connrefused", storage.Check{ErrorType: "connrefused"}, "Connection refused"},
		{"response", storage.Check{ErrorType: "response"}, "Unexpected response"},
		{"empty", storage.Check{ErrorType: "empty"}, "Unexpected response"},
		{"invalid (монитор)", storage.Check{ErrorType: "invalid"}, "Нет данных"},
		{"нет типа/нет данных", storage.Check{}, "Нет данных"},
	} {
		if got := shortErrorLabel(tc.c); got != tc.want {
			t.Errorf("%s: shortErrorLabel = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// Матрица кнопок: каждый вид ошибки даёт кнопку с корректным label.
func TestPanelErrorButtonMatrix(t *testing.T) {
	for _, tc := range []struct {
		name string
		c    storage.Check
		want string
	}{
		{"400", storage.Check{Status: status.Down, ErrorType: "http", HTTPStatus: 400}, "HTTP 400"},
		{"404", storage.Check{Status: status.Down, ErrorType: "http", HTTPStatus: 404}, "HTTP 404"},
		{"429", storage.Check{Status: status.Down, ErrorType: "http", HTTPStatus: 429}, "HTTP 429"},
		{"503", storage.Check{Status: status.Down, ErrorType: "http", HTTPStatus: 503}, "HTTP 503"},
		{"timeout", storage.Check{Status: status.Down, ErrorType: "timeout"}, "Timeout"},
		{"connrefused", storage.Check{Status: status.Down, ErrorType: "connrefused"}, "Connection refused"},
		{"unexpected", storage.Check{Status: status.Down, ErrorType: "response"}, "Unexpected response"},
		{"unknown-статус монитора", storage.Check{Status: status.Unknown, ErrorType: "invalid"}, "Нет данных"},
	} {
		models := []storage.Model{{ID: "m", Family: "M"}}
		text := RenderStatusPanel(models, map[string]storage.Check{"m": tc.c}, time.Now(), time.Now())
		prefix := "🔴"
		if tc.c.Status == status.Unknown {
			prefix = "⚪"
		}
		if !strings.Contains(text, "<p><b>M</b><br/>"+prefix+" <code>m</code> - "+stBtn("m", tc.want)+"</p>") {
			t.Errorf("%s: line with label %q missing:\n%s", tc.name, tc.want, text)
		}
	}
}

// Порядок строк панели — только конфигурация при любом сочетании статусов.
func TestPanelOrderFollowsConfig(t *testing.T) {
	models := []storage.Model{
		{ID: "m1", Family: "F1"},
		{ID: "m2", Family: "F1"},
		{ID: "m3", Family: "F2"},
	}
	checks := map[string]storage.Check{
		"m3": {Status: status.Operational, HTTPStatus: 200, LatencyUS: ptr1(2_000_000)},
		"m1": {Status: status.Down, HTTPStatus: 503, ErrorType: "http"},
		"m2": {Status: status.Unknown},
	}
	text := RenderStatusPanel(models, checks, time.Now(), time.Now())
	i1 := strings.Index(text, "<code>m1</code>")
	i2 := strings.Index(text, "<code>m2</code>")
	i3 := strings.Index(text, "<code>m3</code>")
	if !(i1 < i2 && i2 < i3) {
		t.Fatalf("config order broken: %d %d %d\n%s", i1, i2, i3, text)
	}
}

// Экранирование model ID в <code> и в атрибуте data кнопки.
func TestPanelEscapesModelID(t *testing.T) {
	models := []storage.Model{{ID: `x<y>&z`, Family: "M"}}
	checks := map[string]storage.Check{
		`x<y>&z`: {Status: status.Down, HTTPStatus: 500, ErrorType: "http"},
	}
	text := RenderStatusPanel(models, checks, time.Now(), time.Now())
	if !strings.Contains(text, "<code>x&lt;y&gt;&amp;z</code>") {
		t.Fatalf("code id must be escaped:\n%s", text)
	}
	if !strings.Contains(text, `data="st:x&lt;y&gt;&amp;z"`) {
		t.Fatalf("button data must be escaped:\n%s", text)
	}
}

// До первого цикла панель честна: без «Обновлено», у модели — кнопка «Нет данных».
func TestPanelFirstCycle(t *testing.T) {
	models := []storage.Model{{ID: "a", Family: "A"}}
	text := RenderStatusPanel(models, nil, time.Now(), time.Now())
	wants := []string{
		"Первый цикл проверки выполняется",
		"<p><b>A</b><br/>⚪ <code>a</code> - " + stBtn("a", "Нет данных") + "</p>",
		"<p>🟢 Работает · 🔴 Недоступна · ⚪ Нет данных</p>",
	}
	for _, want := range wants {
		if !strings.Contains(text, want) {
			t.Errorf("first-cycle panel missing %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "Обновлено:") || strings.Contains(text, "Следующая проверка:") {
		t.Fatalf("no times before the first cycle:\n%s", text)
	}
}

// Полный список из config.example.yaml: все 25 моделей, в лимите rich-сообщения.
func TestPanelFullExampleConfigFitsRich(t *testing.T) {
	raw, err := os.ReadFile("../../config.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var cfg struct {
		Models []struct {
			Family string   `yaml:"family"`
			IDs    []string `yaml:"ids"`
		} `yaml:"models"`
	}
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	var models []storage.Model
	for _, fam := range cfg.Models {
		for _, id := range fam.IDs {
			models = append(models, storage.Model{ID: id, Family: fam.Family})
		}
	}
	if len(models) != 25 {
		t.Fatalf("example config must hold 25 models, got %d", len(models))
	}
	checks := map[string]storage.Check{}
	for i, m := range models {
		if i%7 == 0 {
			checks[m.ID] = storage.Check{Status: status.Down, HTTPStatus: 503, ErrorType: "http"}
		} else {
			checks[m.ID] = storage.Check{Status: status.Operational, HTTPStatus: 200,
				LatencyUS: ptr1(int64(2_000_000 + i*100_000))}
			if i%5 == 0 {
				checks[m.ID] = storage.Check{Status: status.Unknown, ErrorType: "invalid"}
			}
		}
	}
	text := RenderStatusPanel(models, checks, time.Now(), time.Now())
	if l := utf8.RuneCountInString(text); l > maxRichMessage {
		t.Fatalf("panel too long for a rich message: %d runes", l)
	}
	for _, m := range models {
		if !strings.Contains(text, "<code>"+m.ID+"</code>") {
			t.Fatalf("model %s missing from panel", m.ID)
		}
	}
}

// Длинный id: кнопка — в форме устойчивого хэша в пределах 64 байт; resolveCallback понимает её.
func TestStButtonDataLongIDHashForm(t *testing.T) {
	long := strings.Repeat("m", 100)
	data := stButtonData(long)
	if !strings.HasPrefix(data, "st:h:") || len(data) > maxCallbackData {
		t.Fatalf("long id must become st:h:<hash> ≤64 bytes: %q", data)
	}
	if _, ok := resolveCallback(strings.TrimPrefix(data, "st:"), []string{long}); !ok {
		t.Fatalf("hash form must resolve back to the model")
	}
	if got := stButtonData("m"); got != "st:m" {
		t.Fatalf("short ids must stay exact: %q", got)
	}
}

// --- Карточка модели ---

// Доступная модель: заголовок, статус, дата последней проверки,
// «Последний запрос» с Latency, «Ответ модели», статистика 24ч.
// Никаких TTFT/TPS/токенов/переключений.
func TestModelDetailOperational(t *testing.T) {
	last := storage.Check{ModelID: "gpt-6-astra", At: mskTime(t, 16, 15, 3), Status: status.Operational,
		HTTPStatus: 200, LatencyUS: ptr1(2_700_000), TTFTUS: ptr1(420_000), TPS: ptrF(42.3),
		InputTokens: ptr1(154), OutputTokens: ptr1(96), Response: "OK"}
	base := time.Now().Add(-23 * time.Hour)
	var checks []storage.Check
	for i := 0; i < 23; i++ {
		c := last
		c.At = base.Add(time.Duration(i) * time.Hour)
		c.LatencyUS = ptr1(2_900_000)
		c.TTFTUS, c.TPS, c.InputTokens, c.OutputTokens = nil, nil, nil, nil
		checks = append(checks, c)
	}
	checks = append(checks, storage.Check{ModelID: "gpt-6-astra", At: base.Add(23 * time.Hour),
		Status: status.Down, HTTPStatus: 429, ErrorType: "http", LatencyUS: nil})
	text, _ := renderModelCard("gpt-6-astra", status.Operational, last, checks)
	for _, want := range []string{
		"<p>🟢 <code>gpt-6-astra</code> - Работает</p>",
		"Последняя проверка: 16.11.2026 16:15:03 MSK",
		"<p><b>Последний запрос</b></p><p>HTTP 200 · Latency 2.7s</p>",
		"<p><b>Ответ модели</b></p><p>OK</p>",
		"Uptime: 95.83%",
		"Проверок: 24",
		"🟢 Успешных: 23 · 🔴 Неудачных: 1 · ⚪ Нет данных: 0",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("card missing %q in:\n%s", want, text)
		}
	}
	for _, bad := range []string{
		"TTFT", "TPS", "токен", "Переключени", "Среднее", "P50", "P95", "Нестабильн", "N/A",
	} {
		if strings.Contains(text, bad) {
			t.Errorf("card must not contain %q:\n%s", bad, text)
		}
	}
	// карточка не ссылается на саму себя
	if strings.Contains(text, `data="st:gpt-6-astra"`) {
		t.Errorf("card must not contain a self-link:\n%s", text)
	}
}

// Статистика 24 часов: два полноценных вертикальных промежутка между тремя
// строками, но ни одного горизонтального отступа. Проверяем итоговый rich-HTML
// целиком, чтобы изменения не затронули остальные блоки карточки.
func TestModelCard24HourStatsVerticalSpacingWithoutIndent(t *testing.T) {
	last := storage.Check{
		ModelID:    "glm-5.3-prime",
		At:         time.Date(2026, 11, 21, 17, 45, 9, 0, MSK),
		Status:     status.Operational,
		HTTPStatus: 200,
		LatencyUS:  ptr1(36_700_000),
		Response:   "OK",
	}
	checks := make([]storage.Check, 0, 97)
	for i := 0; i < 55; i++ {
		checks = append(checks, storage.Check{ModelID: "glm-5.3-prime", Status: status.Operational})
	}
	for i := 0; i < 38; i++ {
		checks = append(checks, storage.Check{ModelID: "glm-5.3-prime", Status: status.Down})
	}
	for i := 0; i < 4; i++ {
		checks = append(checks, storage.Check{ModelID: "glm-5.3-prime", Status: status.Unknown})
	}
	got, _ := renderModelCard("glm-5.3-prime", status.Operational, last, checks)
	want := "<p>🟢 <code>glm-5.3-prime</code> - Работает</p>" +
		"<p>Последняя проверка: 21.11.2026 17:45:09 MSK</p>" +
		"<p><b>Последний запрос</b></p><p>HTTP 200 · Latency 36.7s</p>" +
		"<p><b>Ответ модели</b></p><p>OK</p>" +
		"<p><b>За последние 24 часа</b></p>" +
		"<p>Uptime: 59.14%<br/><br/>Проверок: 97<br/><br/>" +
		"🟢 Успешных: 55 · 🔴 Неудачных: 38 · ⚪ Нет данных: 4</p>"
	if got != want {
		t.Fatalf("model card HTML mismatch\nwant:\n%s\ngot:\n%s", want, got)
	}
}

// Упавшая модель: HTTP-код с «Latency» (HTTP-ответ получен), секция «Ответ API» дословно.
func TestModelDetailDownWithResponse(t *testing.T) {
	last := storage.Check{ModelID: "m", At: mskTime(t, 16, 15, 3), Status: status.Down,
		HTTPStatus: 429, ErrorType: "http", LatencyUS: ptr1(1_220_000),
		Response: "unknown provider for model m"}
	checks := []storage.Check{last, last}
	text, _ := renderModelCard("m", status.Down, last, checks)
	for _, want := range []string{
		"<p>🔴 <code>m</code> - Недоступна</p>",
		"<p><b>Последний запрос</b></p><p>HTTP 429 · Latency 1.2s</p>",
		"<p><b>Ответ API</b></p><p>unknown provider for model m</p>",
		"Uptime: 0.00%",
		"🟢 Успешных: 0 · 🔴 Неудачных: 2 · ⚪ Нет данных: 0",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("card missing %q in:\n%s", want, text)
		}
	}
}

// Заголовок секции ответа: «Ответ модели» для текста модели («» и «response»),
// «Ответ API» для остальных. HTTP 200 без итогового текста всё равно
// показывает факт его отсутствия; у HTTP/транспортной ошибки такой секции нет.
func TestModelDetailAnswerSectionTitle(t *testing.T) {
	for _, tc := range []struct {
		errType string
		want    string
	}{
		{"", "Ответ модели"},
		{"response", "Ответ модели"},
		{"http", "Ответ API"},
		{"timeout", "Ответ API"},
		{"connrefused", "Ответ API"},
		{"invalid", "Ответ API"},
	} {
		last := storage.Check{ModelID: "m", At: time.Now(), Status: status.Down,
			ErrorType: tc.errType, Response: "текст ответа"}
		text, _ := renderModelCard("m", status.Down, last, nil)
		if want := "<p><b>" + tc.want + "</b></p><p>текст ответа</p>"; !strings.Contains(text, want) {
			t.Errorf("errType=%q: section %q missing:\n%s", tc.errType, want, text)
		}
	}
	empty := storage.Check{ModelID: "m", At: time.Now(), Status: status.Down,
		ErrorType: "response", HTTPStatus: 200, ErrorMessage: "finish_reason=stop"}
	text, _ := renderModelCard("m", status.Down, empty, nil)
	if want := "<p><b>Ответ модели</b></p><p>Итоговый текст отсутствует.</p>"; !strings.Contains(text, want) {
		t.Fatalf("HTTP 200 with empty Response must say that the final text is missing:\n%s", text)
	}

	httpEmpty := storage.Check{ModelID: "m", At: time.Now(), Status: status.Down, ErrorType: "http", HTTPStatus: 429}
	text, _ = renderModelCard("m", status.Down, httpEmpty, nil)
	if strings.Contains(text, "Ответ API") || strings.Contains(text, "Ответ модели") {
		t.Fatalf("empty HTTP-error Response must omit the model/API answer section:\n%s", text)
	}
}

// Диагностика пустого итогового ответа: только фактически доступные данные.
// Ноль и отсутствующий finish_reason не выдумываются, непустой ответ не
// подменяется диагностикой, HTTP и транспортные ошибки не превращаются в
// «ответ модели».
func TestModelCardEmptyResponseDiagnostics(t *testing.T) {
	last := storage.Check{ModelID: "m", At: time.Now(), Status: status.Down,
		HTTPStatus: 200, ErrorType: "response", LatencyUS: ptr1(4_100_000),
		OutputTokens: ptr1(64), ErrorMessage: "finish_reason=length"}
	text, _ := renderModelCard("m", status.Down, last, nil)
	for _, want := range []string{
		"<p><b>Последний запрос</b></p><p>HTTP 200 · Latency 4.1s · Токенов ответа: 64 · Завершение: length</p>",
		"<p><b>Ответ модели</b></p><p>Итоговый текст отсутствует.</p>",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %q:\n%s", want, text)
		}
	}

	last = storage.Check{ModelID: "m", At: time.Now(), Status: status.Down,
		HTTPStatus: 200, ErrorType: "empty", LatencyUS: ptr1(4_100_000), ErrorMessage: "finish_reason=stop"}
	text, _ = renderModelCard("m", status.Down, last, nil)
	if strings.Contains(text, "Токенов ответа") {
		t.Errorf("unknown output tokens must be omitted:\n%s", text)
	}
	if !strings.Contains(text, "Завершение: stop") || !strings.Contains(text, "Итоговый текст отсутствует.") {
		t.Errorf("stop reason and missing final text must be shown without inventing a token limit:\n%s", text)
	}

	last = storage.Check{ModelID: "m", At: time.Now(), Status: status.Down,
		HTTPStatus: 200, ErrorType: "empty", LatencyUS: ptr1(4_100_000), ErrorMessage: "empty generation"}
	text, _ = renderModelCard("m", status.Down, last, nil)
	if strings.Contains(text, "Токенов ответа") || strings.Contains(text, "Завершение") {
		t.Errorf("absent diagnostics must not be invented:\n%s", text)
	}
	if !strings.Contains(text, "Итоговый текст отсутствует.") {
		t.Errorf("missing final text remains factual:\n%s", text)
	}

	reasoning := "Let me think through this carefully..."
	last = storage.Check{ModelID: "m", At: time.Now(), Status: status.Down,
		HTTPStatus: 200, ErrorType: "response", LatencyUS: ptr1(4_100_000),
		OutputTokens: ptr1(64), Response: reasoning}
	text, _ = renderModelCard("m", status.Down, last, nil)
	if !strings.Contains(text, "<p><b>Ответ модели</b></p><p>"+reasoning+"</p>") {
		t.Errorf("reasoning in user content must stay verbatim:\n%s", text)
	}
	if strings.Contains(text, "Итоговый текст отсутствует.") || strings.Contains(text, "Токенов ответа") {
		t.Errorf("nonempty response must not be replaced by diagnostics:\n%s", text)
	}

	last = storage.Check{ModelID: "m", At: time.Now(), Status: status.Down,
		HTTPStatus: 429, ErrorType: "http", LatencyUS: ptr1(1_000_000), Response: "Upstream rate limit"}
	text, _ = renderModelCard("m", status.Down, last, nil)
	if !strings.Contains(text, "<p><b>Ответ API</b></p><p>Upstream rate limit</p>") {
		t.Errorf("HTTP provider text must be preserved:\n%s", text)
	}
	if strings.Contains(text, "Итоговый текст отсутствует.") {
		t.Errorf("HTTP error must not be shown as an empty model answer:\n%s", text)
	}

	last = storage.Check{ModelID: "m", At: time.Now(), Status: status.Down,
		ErrorType: "timeout", LatencyUS: ptr1(3_000_000), ErrorMessage: "context deadline exceeded"}
	text, _ = renderModelCard("m", status.Down, last, nil)
	if !strings.Contains(text, "<p>Время до ошибки 3.0s</p>") {
		t.Errorf("transport error timing must be preserved:\n%s", text)
	}
	if strings.Contains(text, "Ответ модели") || strings.Contains(text, "Итоговый текст отсутствует.") {
		t.Errorf("transport error has no model answer section:\n%s", text)
	}
}

// Поля без значения не выводятся вообще: ни N/A, ни пустых блоков.
func TestModelDetailOmitsAbsentFields(t *testing.T) {
	// timeout без HTTP-кода и latency: блока «Последний запрос» нет
	last := storage.Check{ModelID: "m", Status: status.Down, ErrorType: "timeout", ErrorMessage: "deadline"}
	text, _ := renderModelCard("m", status.Down, last, nil)
	for _, bad := range []string{"Последний запрос", "N/A", "0001", "Последняя проверка:"} {
		if strings.Contains(text, bad) {
			t.Errorf("must not contain %q:\n%s", bad, text)
		}
	}
	// нет истории — блока статистики нет
	if strings.Contains(text, "За последние 24 часа") || strings.Contains(text, "Проверок:") {
		t.Fatalf("no history must omit the 24h block:\n%s", text)
	}
	// Operational без latency: только дата проверки и статус
	ok := storage.Check{ModelID: "m", At: time.Now(), Status: status.Operational}
	text, _ = renderModelCard("m", status.Operational, ok, nil)
	if strings.Contains(text, "Последний запрос") {
		t.Fatalf("no latency, no request block:\n%s", text)
	}
}

// Uptime не считает ⚪: знаменатель — только ok+down.
func TestModelDetailUptimeExcludesUnknown(t *testing.T) {
	base := time.Now().Add(-2 * time.Hour)
	var checks []storage.Check
	for i, st := range []status.Status{
		status.Operational, status.Operational, status.Operational, status.Operational,
		status.Operational, status.Operational, status.Operational,
		status.Down, status.Down, status.Unknown,
	} {
		checks = append(checks, storage.Check{ModelID: "m", At: base.Add(time.Duration(i) * 15 * time.Minute), Status: st})
	}
	text, _ := renderModelCard("m", status.Operational, checks[len(checks)-1], checks)
	for _, want := range []string{
		"Uptime: 77.78%",
		"Проверок: 10",
		"🟢 Успешных: 7 · 🔴 Неудачных: 2 · ⚪ Нет данных: 1",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %q in:\n%s", want, text)
		}
	}
}

// Все проверки за 24 часа — ⚪: строки Uptime нет, счётчики остаются.
func TestModelDetailUptimeAllUnknown(t *testing.T) {
	checks := []storage.Check{
		{ModelID: "m", At: time.Now(), Status: status.Unknown, ErrorType: "invalid"},
		{ModelID: "m", At: time.Now(), Status: status.Unknown, ErrorType: "timeout"},
	}
	text, _ := renderModelCard("m", status.Unknown, checks[len(checks)-1], checks)
	if strings.Contains(text, "Uptime:") {
		t.Fatalf("no ok+down — no uptime line:\n%s", text)
	}
	for _, want := range []string{"Проверок: 2", "🟢 Успешных: 0 · 🔴 Неудачных: 0 · ⚪ Нет данных: 2"} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %q in:\n%s", want, text)
		}
	}
}

// Ответ хранится дословно и экранируется перед отправкой.
func TestModelDetailEscapesResponse(t *testing.T) {
	last := storage.Check{ModelID: "m", At: time.Now(), Status: status.Down,
		ErrorType: "response", Response: "<script>alert('x')</script>"}
	text, _ := renderModelCard("m", status.Down, last, nil)
	if strings.Contains(text, "<script>") {
		t.Fatalf("raw HTML leaked:\n%s", text)
	}
	if !strings.Contains(text, "&lt;script&gt;") {
		t.Fatalf("escaped response missing:\n%s", text)
	}
}

// Длинный «Ответ» не обрезается молча: карточка в лимите, хвост — follow-up.
func TestModelCardLongResponseSplit(t *testing.T) {
	resp := strings.Repeat("a", 40000)
	last := storage.Check{ModelID: "m", At: time.Now(), Status: status.Down,
		ErrorType: "http", HTTPStatus: 500, Response: resp}
	card, followup := renderModelCard("m", status.Down, last, nil)
	if l := utf8.RuneCountInString(card); l > maxRichMessage {
		t.Fatalf("card must fit the rich limit, %d runes", l)
	}
	if followup == "" {
		t.Fatal("overflow must not be silently dropped")
	}
	cardA := strings.Count(card, "a")
	if got := cardA + utf8.RuneCountInString(followup); got != 40000 {
		t.Fatalf("response data lost: %d of 40000 runes", got)
	}
	if !strings.Contains(card, "<b>Ответ API</b>") {
		t.Fatalf("answer head must stay in the card:\n%.200s", card)
	}
}

// Регрессия: спец-символы растут в 5–6 раз после
// экранирования — бюджет должен считаться по экранированному тексту,
// иначе карточка превышает лимит и не отправляется вовсе.
func TestModelCardEscapableResponseSplitFitsLimit(t *testing.T) {
	resp := strings.Repeat("&", 40000) // & → &amp; (×5)
	last := storage.Check{ModelID: "m", At: time.Now(), Status: status.Down,
		ErrorType: "http", HTTPStatus: 500, Response: resp}
	card, followup := renderModelCard("m", status.Down, last, nil)
	if l := utf8.RuneCountInString(card); l > maxRichMessage {
		t.Fatalf("card must fit the rich limit after escaping, %d runes", l)
	}
	if followup == "" {
		t.Fatal("overflow must not be silently dropped")
	}
	amp := strings.Count(card, "&amp;") + strings.Count(followup, "&")
	if amp != 40000 {
		t.Fatalf("response data lost: %d of 40000 runes", amp)
	}
	if !strings.Contains(card, "<b>Ответ API</b>") {
		t.Fatalf("answer head must stay in the card:\n%.200s", card)
	}
}

// Единый словарь статусов: слова трёх статусов живут только в statusWordRu;
// легенда панели, /help, заголовок карточки, строки моделей и подпись ошибок
// строятся из него — любое расхождение ловится здесь.
func TestStatusWordsSingleSource(t *testing.T) {
	for st, want := range map[status.Status]string{
		status.Operational: "Работает",
		status.Down:        "Недоступна",
		status.Unknown:     "Нет данных",
	} {
		if got := statusWordRu(st); got != want {
			t.Errorf("statusWordRu(%s) = %q, want %q", st, got, want)
		}
	}
	if got := panelLegend(); got != "🟢 Работает · 🔴 Недоступна · ⚪ Нет данных" {
		t.Errorf("panel legend = %q", got)
	}
	// /help показывает ту же легенду — без завершающей точки, как панель
	help := RenderHelp(15 * time.Minute)
	if !strings.Contains(help, "Статусы: "+panelLegend()+"\n") {
		t.Errorf("help legend must equal the panel legend with no trailing period:\n%s", help)
	}
	// подпись «Нет данных» у ошибок и строки Unknown — тоже из словаря
	if got := shortErrorLabel(storage.Check{}); got != statusWordRu(status.Unknown) {
		t.Errorf("shortErrorLabel default = %q, want %q", got, statusWordRu(status.Unknown))
	}
	line := modelLine(storage.Model{ID: "m-un", Family: "M"}, storage.Check{Status: status.Unknown})
	if !strings.Contains(line, stBtn("m-un", statusWordRu(status.Unknown))) {
		t.Errorf("unknown line must use the dictionary word:\n%s", line)
	}
	card, _ := renderModelCard("m", status.Down, storage.Check{}, nil)
	if !strings.Contains(card, "🔴 <code>m</code> - "+statusWordRu(status.Down)) {
		t.Errorf("card header must use the dictionary word:\n%s", card)
	}
}

// Заголовок карточки: model ID в <code>, как в строках панели, не в жирном.
func TestModelCardHeaderCodeID(t *testing.T) {
	last := storage.Check{ModelID: "deepseek-v4.1-flash", At: time.Now(), Status: status.Operational}
	card, _ := renderModelCard("deepseek-v4.1-flash", status.Operational, last, nil)
	if !strings.Contains(card, "<p>🟢 <code>deepseek-v4.1-flash</code> - Работает</p>") {
		t.Fatalf("header must wrap model ID in <code>:\n%s", card)
	}
	if strings.Contains(card, "<p>🟢 <b>") {
		t.Fatalf("header must not use bold for model ID:\n%s", card)
	}
	// экранирование сохраняется и в заголовке
	card, _ = renderModelCard("x<y>", status.Down, storage.Check{}, nil)
	if !strings.Contains(card, "<p>🔴 <code>x&lt;y&gt;</code> - Недоступна</p>") {
		t.Fatalf("header must escape model ID:\n%s", card)
	}
}

// Единая точность времени в карточке: один знак после запятой и у Latency,
// и у «Времени до ошибки»; различие только в названии метрики.
func TestModelCardLatencySingleDecimal(t *testing.T) {
	// HTTP-ответ получен, содержимое не прошло проверку — всё равно Latency
	last := storage.Check{ModelID: "m", At: time.Now(), Status: status.Down,
		HTTPStatus: 200, ErrorType: "response", LatencyUS: ptr1(1_220_000)}
	text, _ := renderModelCard("m", status.Down, last, nil)
	if !strings.Contains(text, "<p>HTTP 200 · Latency 1.2s</p>") {
		t.Errorf("HTTP 200 with unexpected content must show Latency 1.2s:\n%s", text)
	}
	// HTTP-ответа не было (timeout) — «Время до ошибки» из измеренной длительности
	last = storage.Check{ModelID: "m", At: time.Now(), Status: status.Down,
		ErrorType: "timeout", LatencyUS: ptr1(3_000_000)}
	text, _ = renderModelCard("m", status.Down, last, nil)
	if !strings.Contains(text, "<p>Время до ошибки 3.0s</p>") {
		t.Errorf("timeout without HTTP response must show Время до ошибки 3.0s:\n%s", text)
	}
	// двух знаков после запятой нет нигде
	for _, bad := range []string{"1.22s", "3.00s", ".00s"} {
		if strings.Contains(text, bad) {
			t.Errorf("single decimal expected, found %q:\n%s", bad, text)
		}
	}
}

// Примеры уведомлений в README соответствуют фактическому формату строк:
// разделитель после model ID — ASCII « - », не «·».
func TestReadmeNotificationExamples(t *testing.T) {
	raw, err := os.ReadFile("../../README.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if !strings.Contains(text, "- 🔴 `model-id` - `HTTP 429`") {
		t.Errorf("down notification example must use the actual ` - ` separator")
	}
	if !strings.Contains(text, "- 🟢 `model-id` - Восстановилась") {
		t.Errorf("recovery notification example must use the actual ` - ` separator")
	}
	if strings.Contains(text, "`model-id` · `HTTP 429`") || strings.Contains(text, "`model-id` · Восстановилась") {
		t.Errorf("notification examples must not use the `·` separator after model ID")
	}
}

// Root-меню подписок: фраза без искусственного переноса внутри предложения —
// перенос строк отдаёт клиенту Telegram.
func TestSubscribeRootNoManualBreak(t *testing.T) {
	text, _ := RenderSubscribeRoot(nil, nil, nil)
	if !strings.Contains(text, "Уведомления приходят только по выбранным моделям при смене их состояния. Панель /status не зависит от подписок.") {
		t.Fatalf("sentence must be whole, no manual line break:\n%s", text)
	}
	if strings.Contains(text, "моделям\nпри") {
		t.Fatalf("no manual break inside the sentence:\n%s", text)
	}
}

// --- Меню подписок (не изменилось) и /help ---

func TestSubscribePanel(t *testing.T) {
	models := []storage.Model{
		{ID: "gpt-6-astra", Family: "GPT"},
		{ID: "gpt-6.1-sol", Family: "GPT"},
		{ID: "glm-5.3", Family: "GLM"},
		{ID: "glm-5.3-prime", Family: "GLM"},
	}
	draft := map[string]bool{"gpt-6-astra": true, "glm-5.3": true}
	saved := map[string]bool{"glm-5.3-prime": true}

	text, kb := RenderSubscribeRoot(models, draft, saved)
	if !strings.Contains(text, "OpenTW - Настройка уведомлений") {
		t.Fatalf("header missing:\n%s", text)
	}
	if !strings.Contains(text, "Выбрано: 2 из 4 (изменения: +2 -1)") {
		t.Fatalf("selection counter with diff wrong:\n%s", text)
	}
	if len(kb.InlineKeyboard) != 3 { // пары семейств + масс + Сохранить/Отменить
		t.Fatalf("root rows = %d: %+v", len(kb.InlineKeyboard), kb.InlineKeyboard)
	}
	if famRow := kb.InlineKeyboard[0]; len(famRow) != 2 || famRow[0].Text != "GPT 1/2" || famRow[0].CallbackData != "f:GPT" ||
		famRow[1].Text != "GLM 1/2" || famRow[1].CallbackData != "f:GLM" {
		t.Fatalf("family row = %+v", famRow)
	}
	if massRow := kb.InlineKeyboard[1]; len(massRow) != 2 || massRow[0].CallbackData != "all" || massRow[1].CallbackData != "none" {
		t.Fatalf("mass row = %+v", massRow)
	}
	if lastRow := kb.InlineKeyboard[2]; len(lastRow) != 2 || lastRow[0].CallbackData != "save" || lastRow[1].CallbackData != "cancel" {
		t.Fatalf("save row = %+v", lastRow)
	}

	text, kb = RenderSubscribeFamily("GPT", models, draft, saved)
	if !strings.Contains(text, "<b>GPT</b>") || !strings.Contains(text, "Выбрано: 1 из 2 (изменения: +1)") {
		t.Fatalf("family text wrong:\n%s", text)
	}
	if len(kb.InlineKeyboard) != 4 {
		t.Fatalf("family rows = %d: %+v", len(kb.InlineKeyboard), kb.InlineKeyboard)
	}
	if b := kb.InlineKeyboard[0][0]; b.Text != "+ gpt-6-astra" || b.CallbackData != "s:gpt-6-astra" {
		t.Fatalf("first model button = %+v", b)
	}
	nav := kb.InlineKeyboard[3]
	if len(nav) != 3 || nav[0].CallbackData != "back" || nav[1].CallbackData != "cancel" || nav[2].CallbackData != "save" {
		t.Fatalf("nav row = %+v", nav)
	}
}

func TestSubscribePlural(t *testing.T) {
	for _, tc := range []struct {
		n    int
		want string
	}{{1, "1 модель"}, {2, "2 модели"}, {4, "4 модели"}, {5, "5 моделей"}, {11, "11 моделей"}, {22, "22 модели"}} {
		if got := pluralModels(tc.n); got != tc.want {
			t.Errorf("plural(%d) = %q, want %q", tc.n, got, tc.want)
		}
	}
}

func TestSubscribeEmptySelection(t *testing.T) {
	models := []storage.Model{{ID: "a", Family: "A"}}
	text, _ := RenderSubscribeRoot(models, nil, nil)
	if !strings.Contains(text, "Выбрано: 0 из 1") {
		t.Fatalf("text = %s", text)
	}
}

func TestHelp(t *testing.T) {
	text := RenderHelp(15 * time.Minute)
	for _, cmd := range []string{"/start", "/status", "/status model-id", "/subscribe", "/help"} {
		if !strings.Contains(text, cmd) {
			t.Errorf("help missing %q:\n%s", cmd, text)
		}
	}
	if strings.Contains(text, "/models") || strings.Contains(text, "/errors") || strings.Contains(text, "/digest") {
		t.Error("help must not advertise commands that do not exist")
	}
}

// /help: новая легенда трёх статусов и настраиваемый интервал.
func TestHelpContent(t *testing.T) {
	text := RenderHelp(15 * time.Minute)
	for _, want := range []string{
		"🟢 Работает · 🔴 Недоступна · ⚪ Нет данных",
		"каждые 15 мин",
		"Отключить надоевшую модель",
		"Нажатие на ошибку в панели или уведомлении открывает карточку модели",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("help missing %q:\n%s", want, text)
		}
	}
	text = RenderHelp(0) // интервал неизвестен — нейтральная формулировка
	if !strings.Contains(text, "каждый цикл проверки") {
		t.Fatalf("neutral wording expected:\n%s", text)
	}
}
