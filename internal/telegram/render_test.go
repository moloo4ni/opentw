package telegram

import (
	"strings"
	"testing"
	"time"

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

func TestPanelFormatSuccess(t *testing.T) {
	models := []storage.Model{
		{ID: "gpt-6-astra", Family: "GPT"},
		{ID: "glm-5.3", Family: "GLM"},
	}
	checks := map[string]storage.Check{
		"gpt-6-astra": {ModelID: "gpt-6-astra", Status: status.Operational, HTTPStatus: 200,
			LatencyUS: ptr1(2_700_000), TTFTUS: ptr1(420_000), GenUS: ptr1(2_280_000),
			TPS: ptrF(42.3), InputTokens: ptr1(154), OutputTokens: ptr1(96)},
		"glm-5.3": {ModelID: "glm-5.3", Status: status.Operational, HTTPStatus: 200,
			LatencyUS: ptr1(4_200_000), TTFTUS: ptr1(630_000), GenUS: ptr1(3_570_000), TPS: ptrF(54.1)},
	}
	text := RenderStatusPanel(models, checks, mskTime(t, 16, 15, 3), mskTime(t, 16, 30, 0))
	wantLines := []string{
		"<b>OpenTW</b>",
		"",
		"Обновлено: 16:15:03 MSK",
		"Следующая проверка: 16:30:00 MSK",
		"",
		"<b>GPT</b>",
		"🟢 <code>gpt-6-astra</code> - 2.7s · 42.3 TPS · 154/96",
		"",
		"<b>GLM</b>",
		"🟢 <code>glm-5.3</code> - 4.2s · 54.1 TPS",
	}
	got := strings.Split(strings.TrimRight(text, "\n"), "\n")
	if len(got) != len(wantLines) {
		t.Fatalf("panel lines = %d:\n%s", len(got), text)
	}
	for i, want := range wantLines {
		if got[i] != want {
			t.Fatalf("line %d:\n got: %q\nwant: %q", i, got[i], want)
		}
	}
}

func TestPanelOneLinePerModel(t *testing.T) {
	models := []storage.Model{}
	for i := 0; i < 30; i++ {
		f := "F"
		if i%2 == 1 {
			f = "G"
		}
		models = append(models, storage.Model{ID: strings.Repeat("m", 10), Family: f})
	}
	text := RenderStatusPanel(models, nil, time.Now(), time.Now())
	modelLines := 0
	for _, l := range strings.Split(text, "\n") {
		if strings.HasPrefix(l, "🟢") || strings.HasPrefix(l, "⚪") {
			modelLines++
		}
	}
	if modelLines != 30 {
		t.Fatalf("model lines = %d, want 30", modelLines)
	}
}

func TestPanelErrorLinesAndNA(t *testing.T) {
	models := []storage.Model{
		{ID: "gpt-6-sol", Family: "GPT"},
		{ID: "glm-5.3-fast", Family: "GLM"},
		{ID: "claude-opus-5", Family: "Claude"},
		{ID: "glm-5.3", Family: "GLM"},
		{ID: "un", Family: "U"},
	}
	checks := map[string]storage.Check{
		"gpt-6-sol":     {Status: status.Degraded, HTTPStatus: 429, ErrorType: "http", ErrorMessage: "Upstream rate limit"},
		"glm-5.3-fast":  {Status: status.Down, HTTPStatus: 404, ErrorType: "http", ErrorMessage: "Invalid model."},
		"claude-opus-5": {Status: status.Unknown, ErrorType: "invalid", ErrorMessage: "unparseable response body"},
		"glm-5.3":       {Status: status.Operational, HTTPStatus: 200, LatencyUS: ptr1(3_100_000)},
		"un":            {Status: status.Down, ErrorType: "timeout", ErrorMessage: "context deadline exceeded"},
	}
	text := RenderStatusPanel(models, checks, time.Now(), time.Now())
	for _, want := range []string{
		"🟡 <code>gpt-6-sol</code> - HTTP 429 · Upstream rate limit",
		"🔴 <code>glm-5.3-fast</code> - HTTP 404 · Invalid model.",
		"⚪ <code>claude-opus-5</code> - ошибка монитора",
		"🔴 <code>un</code> - Timeout",
		"🟢 <code>glm-5.3</code> - 3.1s · N/A TPS",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("panel missing line %q\npanel:\n%s", want, text)
		}
	}
	if strings.Contains(text, "glm-5.3</code> - 3.1s · N/A TPS ·") {
		t.Error("tokens block must be omitted when tokens are unknown")
	}
}

func TestPanelPerfDegradedAndEmptyAndNoData(t *testing.T) {
	models := []storage.Model{
		{ID: "a-perf", Family: "A"},
		{ID: "a-empty", Family: "A"},
		{ID: "a-new", Family: "A"},
	}
	checks := map[string]storage.Check{
		"a-perf":  {Status: status.Degraded, HTTPStatus: 200, LatencyUS: ptr1(30_000_000), TTFTUS: ptr1(3_000_000), TPS: ptrF(12.3), ErrorType: "perf"},
		"a-empty": {Status: status.Degraded, HTTPStatus: 200, ErrorType: "empty", ErrorMessage: "empty generation"},
	}
	text := RenderStatusPanel(models, checks, time.Now(), time.Now())
	for _, want := range []string{
		"🟡 <code>a-perf</code> - 30.0s · 12.3 TPS · деградация",
		"🟡 <code>a-empty</code> - HTTP 200 · пустая генерация",
		"⚪ <code>a-new</code> - нет данных",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %q in:\n%s", want, text)
		}
	}
}

func TestPanelEscapesProviderText(t *testing.T) {
	models := []storage.Model{{ID: "m", Family: "M"}}
	checks := map[string]storage.Check{
		"m": {Status: status.Down, HTTPStatus: 500, ErrorType: "http", ErrorMessage: "<script>alert('x')</script>"},
	}
	text := RenderStatusPanel(models, checks, time.Now(), time.Now())
	if strings.Contains(text, "<script>") {
		t.Fatal("raw HTML must be escaped")
	}
	if !strings.Contains(text, "&lt;script&gt;") {
		t.Fatalf("escaped text missing:\n%s", text)
	}
}

func TestPanelTruncatesLongError(t *testing.T) {
	long := strings.Repeat("a", 200)
	models := []storage.Model{{ID: "m", Family: "M"}}
	checks := map[string]storage.Check{
		"m": {Status: status.Down, HTTPStatus: 500, ErrorType: "http", ErrorMessage: long},
	}
	text := RenderStatusPanel(models, checks, time.Now(), time.Now())
	lineIdx := strings.Index(text, "🔴 <code>m</code> -")
	line := text[lineIdx:]
	if strings.Count(line, "a") > 24 {
		t.Fatalf("long error not truncated: %s", line)
	}
	if !strings.HasSuffix(strings.TrimSpace(line), "...") {
		t.Fatalf("truncation marker missing: %q", line)
	}
}

func TestModelDetailFormat(t *testing.T) {
	last := storage.Check{ModelID: "gpt-6-astra", At: mskTime(t, 16, 15, 3), Status: status.Operational,
		HTTPStatus: 200, LatencyUS: ptr1(2_700_000), TTFTUS: ptr1(420_000), GenUS: ptr1(2_280_000),
		TPS: ptrF(42.3), InputTokens: ptr1(154), OutputTokens: ptr1(96)}
	base := time.Now().Add(-23 * time.Hour)
	var checks []storage.Check
	for i := 0; i < 144; i++ {
		c := last
		c.At = base.Add(time.Duration(i) * 6 * time.Minute)
		c.LatencyUS = ptr1(2_900_000)
		c.TPS = ptrF(45.7)
		c.InputTokens, c.OutputTokens = nil, nil
		if i == 100 {
			c.Status = status.Down
			c.HTTPStatus = 429
			c.ErrorType = "http"
			c.ErrorMessage = "Upstream rate limit"
			c.LatencyUS, c.TPS, c.TTFTUS, c.GenUS = nil, nil, nil, nil
		}
		checks = append(checks, c)
	}
	text := RenderModelDetail("gpt-6-astra", status.Operational, last, checks)
	for _, want := range []string{
		"<b>gpt-6-astra</b> - 🟢 работает",
		"Последняя проверка: 16:15:03 MSK",
		"HTTP: 200",
		"Latency: 2.70s",
		"TTFT: 0.42s",
		"Generation time: 2.28s",
		"TPS: 42.3",
		"Input tokens: 154",
		"Output tokens: 96",
		"Uptime: 99.31%",
		"Проверок: 144",
		"Средний latency: 2.90s",
		"Средний TPS: 45.7",
		"HTTP 429 · Upstream rate limit",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("detail missing %q in:\n%s", want, text)
		}
	}
	if strings.Contains(text, "N/A") {
		t.Errorf("unexpected N/A in fully populated detail:\n%s", text)
	}
}

func TestModelDetailNoHistory(t *testing.T) {
	last := storage.Check{ModelID: "x", At: time.Now().Add(-time.Hour), Status: status.Down,
		HTTPStatus: 503, ErrorType: "http", ErrorMessage: "Service Unavailable"}
	text := RenderModelDetail("x", status.Down, last, nil)
	if strings.Contains(text, "Uptime:") {
		t.Fatalf("no uptime without history:\n%s", text)
	}
	if !strings.Contains(text, "Проверок: 0") {
		t.Fatalf("checks counter missing:\n%s", text)
	}
	if !strings.Contains(text, "HTTP 503 · Service Unavailable") {
		t.Fatalf("last error missing:\n%s", text)
	}
}

func TestModelDetailErrorWithNA(t *testing.T) {
	last := storage.Check{ModelID: "x", At: time.Now(), Status: status.Down, HTTPStatus: 0,
		ErrorType: "timeout", ErrorMessage: "deadline"}
	text := RenderModelDetail("x", status.Down, last, nil)
	if !strings.Contains(text, "TPS: N/A") || !strings.Contains(text, "Latency: N/A") || !strings.Contains(text, "HTTP: N/A") {
		t.Fatalf("N/A metrics expected:\n%s", text)
	}
}

func TestSubscribePanel(t *testing.T) {
	models := []storage.Model{
		{ID: "gpt-6-astra", Family: "GPT"},
		{ID: "gpt-6.1-sol", Family: "GPT"},
		{ID: "glm-5.3", Family: "GLM"},
		{ID: "glm-5.3-prime", Family: "GLM"},
	}
	text, kb := RenderSubscribePanel(models, map[string]bool{"gpt-6-astra": true, "glm-5.3": true})
	if !strings.Contains(text, "OpenTW - Настройка уведомлений") {
		t.Fatalf("header missing:\n%s", text)
	}
	if !strings.Contains(text, "Выбрано: 2 модели") {
		t.Fatalf("selection counter wrong:\n%s", text)
	}
	// 2 ряда пар кнопок + ряд Сохранить/Отменить
	if len(kb.InlineKeyboard) != 3 {
		t.Fatalf("keyboard rows = %d", len(kb.InlineKeyboard))
	}
	first := kb.InlineKeyboard[0][0]
	if first.Text != "+ gpt-6-astra" || first.CallbackData != "s:0" {
		t.Fatalf("first button = %+v", first)
	}
	second := kb.InlineKeyboard[0][1]
	if second.Text != "gpt-6.1-sol" || second.CallbackData != "s:1" {
		t.Fatalf("second button = %+v", second)
	}
	third := kb.InlineKeyboard[1][0]
	if third.Text != "+ glm-5.3" || third.CallbackData != "s:2" {
		t.Fatalf("third button = %+v", third)
	}
	save := kb.InlineKeyboard[2][0]
	if save.Text != "Сохранить" || save.CallbackData != "save" {
		t.Fatalf("save button = %+v", save)
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
	text, _ := RenderSubscribePanel(models, nil)
	if !strings.Contains(text, "Выбрано: 0 моделей") {
		t.Fatalf("text = %s", text)
	}
}

func TestHelp(t *testing.T) {
	text := RenderHelp()
	for _, cmd := range []string{"/start", "/status", "/status model-id", "/subscribe", "/help"} {
		if !strings.Contains(text, cmd) {
			t.Errorf("help missing %q:\n%s", cmd, text)
		}
	}
	if strings.Contains(text, "/models") || strings.Contains(text, "/errors") {
		t.Error("help must not advertise commands that do not exist")
	}
}
