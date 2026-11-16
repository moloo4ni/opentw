package telegram

import (
	"os"
	"regexp"
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
	text := RenderStatusPanel(models, checks, mskTime(t, 16, 15, 3), mskTime(t, 16, 30, 0), 15*time.Minute)
	wantLines := []string{
		"<b>OpenTW</b>",
		"",
		"Модели: 2 🟢",
		"",
		"<b>GPT</b>",
		"🟢 <code>gpt-6-astra</code> - 2.7s · 42.3 TPS · 154/96",
		"",
		"<b>GLM</b>",
		"🟢 <code>glm-5.3</code> - 4.2s · 54.1 TPS",
		"",
		"Обновлено: 16.11.2026 16:15:03 MSK",
		"Следующая проверка: 16:30:00 MSK (интервал 15 мин)",
		"🟢 работает · 🟡 нестабильно · 🔴 недоступна · ⚪ нет данных",
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
	text := RenderStatusPanel(models, nil, time.Now(), time.Now(), 15*time.Minute)
	modelLines := 0
	for _, l := range strings.Split(text, "\n") {
		if strings.Contains(l, "<code>") {
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
	text := RenderStatusPanel(models, checks, time.Now(), time.Now(), 15*time.Minute)
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
	text := RenderStatusPanel(models, checks, time.Now(), time.Now(), 15*time.Minute)
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
	text := RenderStatusPanel(models, checks, time.Now(), time.Now(), 15*time.Minute)
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
	text := RenderStatusPanel(models, checks, time.Now(), time.Now(), 15*time.Minute)
	lineIdx := strings.Index(text, "🔴 <code>m</code> -")
	line := text[lineIdx:]
	if nl := strings.IndexByte(line, '\n'); nl >= 0 {
		line = line[:nl]
	}
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
		"Последняя проверка: 16.11.2026 16:15:03 MSK",
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
	draft := map[string]bool{"gpt-6-astra": true, "glm-5.3": true}
	saved := map[string]bool{"glm-5.3-prime": true}

	// корневой экран: семейства + масс-операции + Сохранить/Отменить
	text, kb := RenderSubscribeRoot(models, draft, saved)
	if !strings.Contains(text, "OpenTW - Настройка уведомлений") {
		t.Fatalf("header missing:\n%s", text)
	}
	if !strings.Contains(text, "Выбрано: 2 из 4 (изменения: +2 -1)") {
		t.Fatalf("selection counter with diff wrong:\n%s", text)
	}
	if strings.Contains(text, "gpt-6-astra") {
		t.Fatalf("root text must not list models:\n%s", text)
	}
	if len(kb.InlineKeyboard) != 3 { // пары семейств + масс + Сохранить/Отменить
		t.Fatalf("root rows = %d: %+v", len(kb.InlineKeyboard), kb.InlineKeyboard)
	}
	famRow := kb.InlineKeyboard[0]
	if len(famRow) != 2 || famRow[0].Text != "GPT 1/2" || famRow[0].CallbackData != "f:GPT" ||
		famRow[1].Text != "GLM 1/2" || famRow[1].CallbackData != "f:GLM" {
		t.Fatalf("family row = %+v", famRow)
	}
	massRow := kb.InlineKeyboard[1]
	if len(massRow) != 2 || massRow[0].Text != "Выбрать всё" || massRow[0].CallbackData != "all" ||
		massRow[1].Text != "Снять всё" || massRow[1].CallbackData != "none" {
		t.Fatalf("mass row = %+v", massRow)
	}
	lastRow := kb.InlineKeyboard[2]
	if len(lastRow) != 2 || lastRow[0].Text != "Сохранить" || lastRow[0].CallbackData != "save" ||
		lastRow[1].Text != "Отменить" || lastRow[1].CallbackData != "cancel" {
		t.Fatalf("save row = %+v", lastRow)
	}

	// экран семейства GPT: модели столбцом, масс-операции семейства, Назад/Отменить/Сохранить
	text, kb = RenderSubscribeFamily("GPT", models, draft, saved)
	if !strings.Contains(text, "<b>GPT</b>") || !strings.Contains(text, "Выбрано: 1 из 2 (изменения: +1)") {
		t.Fatalf("family text wrong:\n%s", text)
	}
	if len(kb.InlineKeyboard) != 4 { // 2 модели + масс + навигация
		t.Fatalf("family rows = %d: %+v", len(kb.InlineKeyboard), kb.InlineKeyboard)
	}
	if kb.InlineKeyboard[0][0].Text != "+ gpt-6-astra" || kb.InlineKeyboard[0][0].CallbackData != "s:gpt-6-astra" {
		t.Fatalf("first model button = %+v", kb.InlineKeyboard[0][0])
	}
	if kb.InlineKeyboard[1][0].Text != "gpt-6.1-sol" || kb.InlineKeyboard[1][0].CallbackData != "s:gpt-6.1-sol" {
		t.Fatalf("second model button = %+v", kb.InlineKeyboard[1][0])
	}
	mass := kb.InlineKeyboard[2]
	if len(mass) != 2 || mass[0].Text != "Выбрать всё" || mass[0].CallbackData != "fa:GPT" ||
		mass[1].Text != "Снять всё" || mass[1].CallbackData != "fn:GPT" {
		t.Fatalf("family mass row = %+v", mass)
	}
	nav := kb.InlineKeyboard[3]
	if len(nav) != 3 || nav[0].Text != "Назад" || nav[0].CallbackData != "back" ||
		nav[1].Text != "Отменить" || nav[1].CallbackData != "cancel" ||
		nav[2].Text != "Сохранить" || nav[2].CallbackData != "save" {
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
	if strings.Contains(text, "/models") || strings.Contains(text, "/errors") {
		t.Error("help must not advertise commands that do not exist")
	}
}

// Модель ни разу не проверялась (zero time): вместо бессмысленной даты
// должен показываться N/A.
func TestModelDetailZeroTimeShowsNA(t *testing.T) {
	text := RenderModelDetail("x", status.Unknown, storage.Check{}, nil)
	if !strings.Contains(text, "Последняя проверка: N/A") {
		t.Fatalf("zero time must render as N/A:\n%s", text)
	}
	if strings.Contains(text, "0001") {
		t.Fatalf("meaningless date leaked:\n%s", text)
	}
}

// Сводка состояния сразу после заголовка, легенда — последней строкой (AC-8).
func TestPanelSummaryAndLegend(t *testing.T) {
	models := []storage.Model{
		{ID: "a", Family: "A"},
		{ID: "b", Family: "A"},
		{ID: "c", Family: "A"},
	}
	checks := map[string]storage.Check{
		"a": {Status: status.Operational, HTTPStatus: 200, LatencyUS: ptr1(2_000_000)},
		"b": {Status: status.Degraded, HTTPStatus: 429, ErrorType: "http", ErrorMessage: "rate limit"},
		"c": {Status: status.Down, HTTPStatus: 500, ErrorType: "http", ErrorMessage: "boom"},
	}
	text := RenderStatusPanel(models, checks, time.Now(), time.Now(), 15*time.Minute)
	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")
	if lines[2] != "Модели: 1 🟢 · 1 🟡 · 1 🔴" {
		t.Fatalf("summary line = %q\n%s", lines[2], text)
	}
	last := lines[len(lines)-1]
	if last != "🟢 работает · 🟡 нестабильно · 🔴 недоступна · ⚪ нет данных" {
		t.Fatalf("legend must be the last line: %q", last)
	}
}

// Все проверенные ⚪: сводка без нотифицируемых + предупреждение (AC-12).
func TestPanelAllUnknownWarnsMonitor(t *testing.T) {
	models := []storage.Model{{ID: "a", Family: "A"}, {ID: "b", Family: "A"}}
	checks := map[string]storage.Check{
		"a": {Status: status.Unknown, ErrorType: "http", HTTPStatus: 401, ErrorMessage: "Invalid API key"},
		"b": {Status: status.Unknown, ErrorType: "timeout", ErrorMessage: "deadline"},
	}
	text := RenderStatusPanel(models, checks, time.Now(), time.Now(), 15*time.Minute)
	if !strings.Contains(text, "Модели: 2 ⚪") {
		t.Fatalf("summary must show only unknown:\n%s", text)
	}
	if !strings.Contains(text, "⚠ Монитор не может проверить модели") {
		t.Fatalf("monitor warning missing:\n%s", text)
	}
	if !strings.Contains(text, "⚪ <code>a</code> - ошибка монитора") {
		t.Fatalf("unknown model line:\n%s", text)
	}
}

// До первого цикла панель честна: без «Обновлено» (AC-11).
func TestPanelFirstCycle(t *testing.T) {
	models := []storage.Model{{ID: "a", Family: "A"}}
	text := RenderStatusPanel(models, nil, time.Now(), time.Now(), 15*time.Minute)
	if !strings.Contains(text, "Первый цикл проверки выполняется - панель обновится автоматически") {
		t.Fatalf("first-cycle line missing:\n%s", text)
	}
	if strings.Contains(text, "Обновлено:") || strings.Contains(text, "Следующая проверка:") {
		t.Fatalf("no times before the first cycle:\n%s", text)
	}
	if !strings.Contains(text, "⚪ <code>a</code> - нет данных") {
		t.Fatalf("unchecked model line:\n%s", text)
	}
	if !strings.Contains(text, "🟢 работает · 🟡 нестабильно · 🔴 недоступна · ⚪ нет данных") {
		t.Fatalf("legend must stay:\n%s", text)
	}
}

// Порядок строк панели — порядок конфигурации при любом сочетании статусов (AC-9).
func TestPanelOrderFollowsConfig(t *testing.T) {
	models := []storage.Model{
		{ID: "m1", Family: "F1"},
		{ID: "m2", Family: "F1"},
		{ID: "m3", Family: "F2"},
	}
	checks := map[string]storage.Check{
		"m3": {Status: status.Operational, HTTPStatus: 200, LatencyUS: ptr1(2_000_000)},
		"m1": {Status: status.Down, HTTPStatus: 503, ErrorType: "http", ErrorMessage: "x"},
		"m2": {Status: status.Operational, HTTPStatus: 200, LatencyUS: ptr1(2_000_000)},
	}
	text := RenderStatusPanel(models, checks, time.Now(), time.Now(), 15*time.Minute)
	i1 := strings.Index(text, "<code>m1</code>")
	i2 := strings.Index(text, "<code>m2</code>")
	i3 := strings.Index(text, "<code>m3</code>")
	if !(i1 < i2 && i2 < i3) {
		t.Fatalf("config order broken: %d %d %d\n%s", i1, i2, i3, text)
	}
}

// Дата в «Обновлено» + интервал в подвале (D8, AC-21).
func TestPanelDateAndInterval(t *testing.T) {
	models := []storage.Model{{ID: "a", Family: "A"}}
	checks := map[string]storage.Check{"a": {Status: status.Operational, HTTPStatus: 200, LatencyUS: ptr1(2_000_000)}}
	text := RenderStatusPanel(models, checks, mskTime(t, 16, 15, 3), mskTime(t, 16, 30, 0), 15*time.Minute)
	if !strings.Contains(text, "Обновлено: 16.11.2026 16:15:03 MSK") {
		t.Fatalf("date missing:\n%s", text)
	}
	if !strings.Contains(text, "Следующая проверка: 16:30:00 MSK (интервал 15 мин)") {
		t.Fatalf("interval missing:\n%s", text)
	}
	text = RenderStatusPanel(models, checks, time.Now(), time.Now(), 90*time.Minute)
	if !strings.Contains(text, "(интервал 1ч 30м)") {
		t.Fatalf("interval 90m formatting:\n%s", text)
	}
}

// Полный список из config.example.yaml: все 25 моделей, ≤ 4096 символов (AC-10/22).
func TestPanelFullExampleConfigFitsTelegram(t *testing.T) {
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
		c := storage.Check{Status: status.Operational, HTTPStatus: 200,
			LatencyUS: ptr1(int64(2_000_000 + i*100_000)), TPS: ptrF(40.0 + float64(i))}
		if i%7 == 0 {
			c = storage.Check{Status: status.Down, HTTPStatus: 503, ErrorType: "http", ErrorMessage: "No available accounts. Please retry."}
		}
		checks[m.ID] = c
	}
	text := RenderStatusPanel(models, checks, time.Now(), time.Now(), 15*time.Minute)
	if l := len([]rune(text)); l > 4096 {
		t.Fatalf("panel too long: %d runes", l)
	}
	for _, m := range models {
		if !strings.Contains(text, "<code>"+m.ID+"</code>") {
			t.Fatalf("model %s missing from panel", m.ID)
		}
	}
}

// Флэппинг-наблюдаемость: переключения за 24ч и последнее переключение (§5.1).
func TestModelDetailFlapping(t *testing.T) {
	base := time.Now().Add(-2 * time.Hour)
	checks := make([]storage.Check, 0, 4)
	for i, st := range []status.Status{status.Operational, status.Down, status.Operational, status.Down} {
		checks = append(checks, storage.Check{
			ModelID: "m", At: base.Add(time.Duration(i) * 15 * time.Minute),
			Status: st, HTTPStatus: 200,
		})
	}
	text := RenderModelDetail("m", status.Down, checks[len(checks)-1], checks)
	if !strings.Contains(text, "Переключений состояния: 3") {
		t.Fatalf("switch count missing:\n%s", text)
	}
	if !strings.Contains(text, "Последнее переключение: "+fmtDateClockMSK(base.Add(45*time.Minute))+" MSK") {
		t.Fatalf("last switch time missing:\n%s", text)
	}
	if !strings.Contains(text, "Среднее пребывание в статусе: 15 мин") {
		t.Fatalf("mean dwell missing:\n%s", text)
	}
}

// Uptime не считает ⚪ за аптайм; количество без данных названо (D11).
func TestUptimeExcludesUnknown(t *testing.T) {
	base := time.Now().Add(-2 * time.Hour)
	var checks []storage.Check
	for i, st := range []status.Status{
		status.Operational, status.Operational, status.Operational, status.Operational,
		status.Operational, status.Operational, status.Operational,
		status.Down, status.Down, status.Unknown,
	} {
		checks = append(checks, storage.Check{ModelID: "m", At: base.Add(time.Duration(i) * 15 * time.Minute), Status: st})
	}
	text := RenderModelDetail("m", status.Operational, checks[len(checks)-1], checks)
	if !strings.Contains(text, "Uptime: 77.78%") {
		t.Fatalf("uptime must be 7/9 without unknown:\n%s", text)
	}
	if !strings.Contains(text, "Без данных (монитор): 1") {
		t.Fatalf("unknown count line missing:\n%s", text)
	}
}

// /help: легенда, смысл метрик, путь отключения надоевшей модели (AC-20).
func TestHelpContent(t *testing.T) {
	text := RenderHelp(15 * time.Minute)
	for _, want := range []string{
		"🟢 работает · 🟡 нестабильно · 🔴 недоступна · ⚪ нет данных",
		"одного проверочного запроса",
		"Отключить надоевшую модель",
		"/status model-id",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("help missing %q:\n%s", want, text)
		}
	}
	for _, bad := range []string{"/models", "/errors", "/digest"} {
		if strings.Contains(text, bad) {
			t.Errorf("help must not advertise %q", bad)
		}
	}
}

// D13: текст ошибки шлюза в 200-ответе виден и в строке панели (Rev-6).
func TestPanelEmptyWithGatewayError(t *testing.T) {
	models := []storage.Model{{ID: "m", Family: "M"}}
	checks := map[string]storage.Check{
		"m": {Status: status.Degraded, HTTPStatus: 200, ErrorType: "empty",
			ErrorMessage: "No available accounts. Please retry later."},
	}
	text := RenderStatusPanel(models, checks, time.Now(), time.Now(), 15*time.Minute)
	if !strings.Contains(text, "🟡 <code>m</code> - HTTP 200 · No available account...") {
		t.Fatalf("gateway error must surface in the panel line:\n%s", text)
	}
	if strings.Contains(text, "пустая генерация") {
		t.Fatalf("misleading empty-generation label when a real error exists:\n%s", text)
	}
}

// --- Этап 7 ---

// 7.1: длинная ошибка провайдера не раздувает детальную страницу за лимит
// Telegram 4096 — ответ пользователю гарантирован.
func TestModelDetailErrorTextCapped(t *testing.T) {
	last := storage.Check{ModelID: "m", At: time.Now(), Status: status.Down,
		HTTPStatus: 503, ErrorType: "http",
		ErrorMessage: strings.Repeat("но<bws&>amp; ", 4000)} // 40к рун с HTML-символами
	text := RenderModelDetail("m", status.Down, last, nil)
	if l := len([]rune(text)); l > 4096 {
		t.Fatalf("detail must fit Telegram limit, %d runes:\n%.300s…", l, text)
	}
	if !strings.Contains(text, "HTTP 503") {
		t.Fatalf("error head must survive capping:\n%s", text)
	}
	if !utf8.ValidString(text) {
		t.Fatal("capping must not produce invalid UTF-8")
	}
	if strings.HasSuffix(strings.TrimSpace(text), "&") || entityTail.MatchString(strings.TrimSpace(text)) {
		t.Fatal("capping must not cut an HTML entity in half (Telegram 400)")
	}
}

// entityTail — оборванная entity на конце строки (&am без «;»).
var entityTail = regexp.MustCompile(`&[a-zA-Z0-9#]{1,7}$`)

// 7.3: детальные отметки времени несут дату — 24-часовое окно пересекает
// полночь, одно время суток вводит в заблуждение.
func TestModelDetailTimestampsHaveDate(t *testing.T) {
	last := storage.Check{ModelID: "m", At: mskTime(t, 16, 15, 3), Status: status.Down,
		HTTPStatus: 503, ErrorType: "http", ErrorMessage: "Service Unavailable"}
	checks := []storage.Check{
		{ModelID: "m", At: mskTime(t, 14, 0, 0), Status: status.Operational, HTTPStatus: 200},
		last,
	}
	text := RenderModelDetail("m", status.Down, last, checks)
	for _, want := range []string{
		"Последняя проверка: 16.11.2026 16:15:03 MSK",
		"Последнее переключение: 16.11.2026 16:15:03 MSK",
		"Последняя ошибка\n16.11.2026 16:15:03 MSK · HTTP 503 · Service Unavailable",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("date missing in %q:\n%s", want, text)
		}
	}
}

// 7.3: Degraded виден в статистике 24ч отдельной строкой, а не проваливается
// между «успешными» и «неудачными».
func TestStatsCountDegraded(t *testing.T) {
	base := time.Now().Add(-2 * time.Hour)
	var checks []storage.Check
	for i, st := range []status.Status{
		status.Operational, status.Degraded, status.Degraded, status.Down,
	} {
		checks = append(checks, storage.Check{ModelID: "m", At: base.Add(time.Duration(i) * 15 * time.Minute), Status: st, HTTPStatus: 200})
	}
	text := RenderModelDetail("m", status.Down, checks[len(checks)-1], checks)
	if !strings.Contains(text, "Нестабильных: 2") {
		t.Fatalf("degraded count missing:\n%s", text)
	}
	if !strings.Contains(text, "	Uptime: 25.00%") && !strings.Contains(text, "Uptime: 25.00%") {
		t.Fatalf("uptime must stay (1 op)/(4-0 known) = 25%%:\n%s", text)
	}
}

// 7.3: переключения состояния не считают ⚪ — это слепота монитора,
// а не смена состояния модели.
func TestFlappingIgnoresUnknown(t *testing.T) {
	base := time.Now().Add(-2 * time.Hour)
	var checks []storage.Check
	for i, st := range []status.Status{
		status.Operational, status.Unknown, status.Operational,
		status.Down, status.Unknown, status.Down,
	} {
		checks = append(checks, storage.Check{ModelID: "m", At: base.Add(time.Duration(i) * 15 * time.Minute), Status: st, HTTPStatus: 200})
	}
	n, lastAt, _ := flappingStats(checks)
	if n != 1 {
		t.Fatalf("only the real 🟢→🔴 switch must count, got %d", n)
	}
	want := checks[3].At
	if !lastAt.Equal(want) {
		t.Fatalf("last switch = %v, want %v", lastAt, want)
	}
}

// 7.3: единое форматирование длительностей — один словарь на все поверхности.
func TestDurationHuman(t *testing.T) {
	for _, tc := range []struct {
		d    time.Duration
		want string
	}{
		{0, "0с"},
		{45 * time.Second, "45с"},
		{30 * time.Minute, "30м"},
		{90 * time.Minute, "1ч 30м"},
		{26 * time.Hour, "26ч 00м"},
	} {
		if got := durationHuman(tc.d); got != tc.want {
			t.Errorf("durationHuman(%v) = %q, want %q", tc.d, got, tc.want)
		}
	}
}

// 7.3: единый форматтер заголовков ошибок — одна подпись на тип ошибки для
// панели, сводок, дайджеста и деталей.
func TestErrorHeadSingle(t *testing.T) {
	for _, tc := range []struct {
		name string
		c    storage.Check
		want string
	}{
		{"http", storage.Check{ErrorType: "http", HTTPStatus: 429, ErrorMessage: "Upstream rate limit"}, "HTTP 429 · Upstream rate limit"},
		{"http-empty-body", storage.Check{ErrorType: "http", HTTPStatus: 502}, "HTTP 502"},
		{"timeout", storage.Check{ErrorType: "timeout"}, "Timeout"},
		{"connrefused", storage.Check{ErrorType: "connrefused"}, "Connection refused"},
		{"empty-gen", storage.Check{ErrorType: "empty", ErrorMessage: "empty generation"}, "HTTP 200 · пустая генерация"},
		{"empty-gateway", storage.Check{ErrorType: "empty", HTTPStatus: 200, ErrorMessage: "No available accounts"}, "HTTP 200 · No available account..."},
		{"invalid", storage.Check{ErrorType: "invalid"}, "Некорректный ответ API"},
		{"perf", storage.Check{ErrorType: "perf"}, "деградация производительности"},
	} {
		if got := errorHead(tc.c); got != tc.want {
			t.Errorf("%s: errorHead = %q, want %q", tc.name, got, tc.want)
		}
	}
	if got := detailErrorLine(storage.Check{ErrorType: "invalid"}); got != "Некорректный ответ API" {
		t.Errorf("detailErrorLine(invalid) = %q", got)
	}
	if got := detailErrorLine(storage.Check{ErrorType: "empty", ErrorMessage: "empty generation"}); got != "HTTP 200 · пустая генерация" {
		t.Errorf("detailErrorLine(empty) = %q, want unified ' · ' separator", got)
	}
}

// 7.4: /help не обещает «каждые 15 минут» при настраиваемом интервале.
func TestHelpDynamicInterval(t *testing.T) {
	text := RenderHelp(15 * time.Minute)
	if !strings.Contains(text, "каждые 15 мин") {
		t.Fatalf("help must state the configured interval:\n%s", text)
	}
	if strings.Contains(text, "каждые 15 минут") {
		t.Fatalf("hardcoded interval must be gone:\n%s", text)
	}
	text = RenderHelp(0) // интервал неизвестен — нейтральная формулировка
	if !strings.Contains(text, "каждый цикл проверки") {
		t.Fatalf("neutral wording expected:\n%s", text)
	}
	text = RenderHelp(30 * time.Minute)
	if !strings.Contains(text, "каждые 30 мин") {
		t.Fatalf("configured interval must be reflected:\n%s", text)
	}
}
