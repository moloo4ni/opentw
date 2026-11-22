package telegram

import (
	"strings"
	"testing"
	"time"

	"opentw/internal/status"
	"opentw/internal/storage"
)

// The panel in the reference bot's style: model IDs monospaced (<code>),
// family headers bold (<b>), one line per model.
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

// No decorative Unicode characters (—, …) in panel lines: after a model ID
// comes the ASCII " - ", metrics inside a line are separated by "·".
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

// The error line: only the short label in the button; the provider's long
// text goes to the model card.
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

// The subscribe menu: the full model list lives in the buttons; the text
// does not duplicate it.
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
	if len(rows) != 3 { // 2 families fit into 1 row + bulk + Save/Cancel
		t.Fatalf("root rows = %d: %+v", len(rows), rows)
	}
	famRow := rows[0]
	if len(famRow) != 2 || famRow[0].Text != "GPT 0/1" || famRow[0].CallbackData != "f:GPT" ||
		famRow[1].Text != "GLM 1/1" || famRow[1].CallbackData != "f:GLM" {
		t.Fatalf("family row = %+v", famRow)
	}
}

// Notifications: the down line is an interactive button with a label;
// the recovery line has no button.
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

// The label matrix: every error kind yields the correct button in a
// notification line.
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
