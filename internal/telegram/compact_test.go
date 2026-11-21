package telegram

import (
	"strings"
	"testing"
	"time"

	"opentw/internal/status"
	"opentw/internal/storage"
)

// Панель в стиле референсного бота: model ID моноширинным (<code>),
// заголовки семейств жирным (<b>), одна строка на модель.
func TestPanelCompactOneLine(t *testing.T) {
	models := []storage.Model{{ID: "deepseek-v4.1-flash-1017", Family: "DeepSeek"}}
	checks := map[string]storage.Check{
		"deepseek-v4.1-flash-1017": {Status: status.Operational, HTTPStatus: 200,
			LatencyUS: ptr1(7_531_000), TTFTUS: ptr1(7_530_000), TPS: ptrF(12.53),
			InputTokens: ptr1(5), OutputTokens: ptr1(1)},
	}
	text := RenderStatusPanel(models, checks, time.Now(), time.Now())
	want := "<p><b>DeepSeek</b><br/>🟢 <code>deepseek-v4.1-flash-1017</code> - 7.5s</p>"
	if !strings.Contains(text, want) {
		t.Fatalf("compact family block:\nwant: %q\ngot:  %q", want, text)
	}
	if !strings.Contains(text, "<b>DeepSeek</b>") {
		t.Fatalf("family header must be bold:\n%s", text)
	}
	if strings.Contains(text, "12.53") || strings.Contains(text, "12.5 TPS") || strings.Contains(text, "5/1") {
		t.Fatalf("TPS/tokens must not leak into the panel:\n%s", text)
	}
}

// В строках панели нет декоративных Unicode-символов (—, …): после model ID —
// ASCII « - », метрики внутри строки разделяются «·» (§4/финальная полировка §6).
func TestPanelNoDashesOrEllipsis(t *testing.T) {
	models := []storage.Model{
		{ID: "grok-4.6", Family: "Grok"},
		{ID: "deepseek-v4.1-flash-1017", Family: "DeepSeek"},
	}
	checks := map[string]storage.Check{
		"grok-4.6": {Status: status.Down, HTTPStatus: 503, ErrorType: "http",
			ErrorMessage: "No available accounts. Please retry after 02:01 MSK."},
		"deepseek-v4.1-flash-1017": {Status: status.Operational, HTTPStatus: 200,
			LatencyUS: ptr1(7_531_000), TPS: ptrF(12.5)},
	}
	text := RenderStatusPanel(models, checks, time.Now(), time.Now())
	for _, bad := range []string{"—", "…", "TTFT", "TPS"} {
		if strings.Contains(text, bad) {
			t.Fatalf("%q must not appear in the panel:\n%s", bad, text)
		}
	}
}

// Строка ошибки: только короткий label в кнопке, длинный текст провайдера —
// в карточке модели (§4/ТЗ §7).
func TestPanelErrorLineCompact(t *testing.T) {
	models := []storage.Model{{ID: "grok-4.6", Family: "Grok"}}
	checks := map[string]storage.Check{
		"grok-4.6": {Status: status.Down, HTTPStatus: 503, ErrorType: "http",
			ErrorMessage: "No available accounts. Please retry after 02:01 MSK."},
	}
	text := RenderStatusPanel(models, checks, time.Now(), time.Now())
	if !strings.Contains(text, "🔴 <code>grok-4.6</code> - "+stBtn("grok-4.6", "HTTP 503")) {
		t.Fatalf("error line wrong:\n%s", text)
	}
	if strings.Contains(text, "Please retry after") {
		t.Fatalf("provider message must not surface in the panel line:\n%s", text)
	}
}

// Меню подписок: полный список моделей живёт в кнопках; текст не дублирует его.
func TestSubscribeTextCompact(t *testing.T) {
	models := []storage.Model{
		{ID: "gpt-6-astra", Family: "GPT"},
		{ID: "glm-5.3-prime", Family: "GLM"},
	}
	text, kb := RenderSubscribeRoot(models, map[string]bool{"glm-5.3-prime": true}, map[string]bool{"glm-5.3-prime": true})
	if strings.Contains(text, "gpt-6-astra") || strings.Contains(text, "glm-5.3-prime") {
		t.Fatalf("model list must live in the keyboard, not the text:\n%s", text)
	}
	if !strings.Contains(text, "Выбрано: 1 из 2") {
		t.Fatalf("selection counter missing:\n%s", text)
	}
	rows := kb.InlineKeyboard
	if len(rows) != 3 { // 2 семейства уложились в 1 ряд + масс + Сохранить/Отменить
		t.Fatalf("root rows = %d: %+v", len(rows), rows)
	}
	famRow := rows[0]
	if len(famRow) != 2 || famRow[0].Text != "GPT 0/1" || famRow[0].CallbackData != "f:GPT" ||
		famRow[1].Text != "GLM 1/1" || famRow[1].CallbackData != "f:GLM" {
		t.Fatalf("family row = %+v", famRow)
	}
}

// Уведомления §8: строка падения — интерактивная кнопка с label,
// строка восстановления — без кнопки.
func TestNotificationLineFormat(t *testing.T) {
	down := downLine("gpt-6-astra", storage.Check{ModelID: "gpt-6-astra", Status: status.Down,
		HTTPStatus: 429, ErrorType: "http", ErrorMessage: "Upstream rate limit"})
	want := "<p>🔴 <code>gpt-6-astra</code> - " + stBtn("gpt-6-astra", "HTTP 429") + "</p>"
	if down != want {
		t.Fatalf("down line:\n got: %q\nwant: %q", down, want)
	}
	rec := recoveredLine("glm-5.3")
	if rec != "<p>🟢 <code>glm-5.3</code> - Восстановилась</p>" {
		t.Fatalf("recovery line = %q", rec)
	}
	if strings.Contains(rec, "tg-button") {
		t.Fatalf("recovery line must not be interactive: %q", rec)
	}
}

// 15.4/15.6 (матрица label): каждый вид ошибки даёт корректную кнопку в строке уведомления.
func TestNotificationButtonLabelMatrix(t *testing.T) {
	for _, tc := range []struct {
		name string
		c    storage.Check
		want string
	}{
		{"400", storage.Check{ErrorType: "http", HTTPStatus: 400}, "HTTP 400"},
		{"404", storage.Check{ErrorType: "http", HTTPStatus: 404}, "HTTP 404"},
		{"429", storage.Check{ErrorType: "http", HTTPStatus: 429}, "HTTP 429"},
		{"503", storage.Check{ErrorType: "http", HTTPStatus: 503}, "HTTP 503"},
		{"timeout", storage.Check{ErrorType: "timeout"}, "Timeout"},
		{"connrefused", storage.Check{ErrorType: "connrefused"}, "Connection refused"},
		{"unexpected", storage.Check{ErrorType: "response"}, "Unexpected response"},
		{"empty", storage.Check{ErrorType: "empty"}, "Unexpected response"},
	} {
		line := downLine("m", tc.c)
		want := "<p>🔴 <code>m</code> - " + stBtn("m", tc.want) + "</p>"
		if line != want {
			t.Errorf("%s: down line = %q, want %q", tc.name, line, want)
		}
	}
}
