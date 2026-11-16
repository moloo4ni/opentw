package telegram

import (
	"strings"
	"testing"
	"time"

	"opentw/internal/status"
	"opentw/internal/storage"
)

// Панель в стиле референсного бота: model ID моноширинным (<code>),
// заголовки семейств жирным (<b>), пунктуация ASCII, разделители с пробелами.
func TestPanelCompactOneLine(t *testing.T) {
	models := []storage.Model{{ID: "deepseek-v4.1-flash-1017", Family: "DeepSeek"}}
	checks := map[string]storage.Check{
		"deepseek-v4.1-flash-1017": {Status: status.Operational, HTTPStatus: 200,
			LatencyUS: ptr1(7_531_000), TTFTUS: ptr1(7_530_000), GenUS: ptr1(1_000), TPS: ptrF(12.53),
			InputTokens: ptr1(5), OutputTokens: ptr1(1)},
	}
	text := RenderStatusPanel(models, checks, time.Now(), time.Now(), 15*time.Minute)
	want := "🟢 <code>deepseek-v4.1-flash-1017</code> - 7.5s · 12.5 TPS · 5/1"
	if !strings.Contains(text, want) {
		t.Fatalf("compact line:\nwant: %q\ngot:  %q", want, text)
	}
	if !strings.Contains(text, "<b>DeepSeek</b>") {
		t.Fatalf("family header must be bold:\n%s", text)
	}
}

// В строках панели не должно остаться non-ASCII-пунктуации: ни «·», ни «—»,
// ни «…» — только эмодзи статуса, кириллические подписи и ASCII-символы.
func TestPanelNoNonASCIIPunctuation(t *testing.T) {
	models := []storage.Model{
		{ID: "grok-4.6", Family: "Grok"},
		{ID: "deepseek-v4.1-flash-1017", Family: "DeepSeek"},
	}
	checks := map[string]storage.Check{
		"grok-4.6": {Status: status.Down, HTTPStatus: 503, ErrorType: "http",
			ErrorMessage: "No available accounts. Please retry after 02:01 MSK."},
		"deepseek-v4.1-flash-1017": {Status: status.Operational, HTTPStatus: 200,
			LatencyUS: ptr1(7_531_000), TPS: ptrF(12.5), InputTokens: ptr1(5), OutputTokens: ptr1(1)},
	}
	text := RenderStatusPanel(models, checks, time.Now(), time.Now(), 15*time.Minute)
	for _, bad := range []string{"—", "…"} {
		if strings.Contains(text, bad) {
			t.Fatalf("non-ASCII punctuation %q still present:\n%s", bad, text)
		}
	}
	for _, l := range strings.Split(text, "\n") {
		if strings.HasPrefix(l, "🟢") || strings.HasPrefix(l, "🔴") || strings.HasPrefix(l, "🟡") || strings.HasPrefix(l, "⚪") {
			// теги не отображаются — измеряем видимую длину
			visible := l
			for _, tag := range []string{"<code>", "</code>", "<b>", "</b>"} {
				visible = strings.ReplaceAll(visible, tag, "")
			}
			if len([]rune(visible)) > 64 {
				t.Fatalf("model line too long (visible): %q", visible)
			}
		}
	}
}

// Строка ошибки: сообщение провайдера аккуратно сокращено, полный текст —
// в /status model-id (ТЗ §7).
func TestPanelErrorLineCompact(t *testing.T) {
	models := []storage.Model{{ID: "grok-4.6", Family: "Grok"}}
	checks := map[string]storage.Check{
		"grok-4.6": {Status: status.Down, HTTPStatus: 503, ErrorType: "http",
			ErrorMessage: "No available accounts. Please retry after 02:01 MSK."},
	}
	text := RenderStatusPanel(models, checks, time.Now(), time.Now(), 15*time.Minute)
	if !strings.Contains(text, "🔴 <code>grok-4.6</code> - HTTP 503 ·") {
		t.Fatalf("error head wrong:\n%s", text)
	}
	if strings.Contains(text, "Please retry after") {
		t.Fatalf("message must be truncated harder:\n%s", text)
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
	mass := rows[1]
	if len(mass) != 2 || mass[0].Text != "Выбрать всё" || mass[0].CallbackData != "all" ||
		mass[1].Text != "Снять всё" || mass[1].CallbackData != "none" {
		t.Fatalf("mass row = %+v", mass)
	}
	last := rows[len(rows)-1]
	if len(last) != 2 || last[0].Text != "Сохранить" || last[0].CallbackData != "save" ||
		last[1].Text != "Отменить" || last[1].CallbackData != "cancel" {
		t.Fatalf("save row = %+v", last)
	}
}

// Уведомления: model ID моноширинным, ASCII-пунктуация.
func TestNotificationFormatting(t *testing.T) {
	tr := transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)
	text := RenderNotification(tr)
	if !strings.Contains(text, "🔴 <code>gpt-6-astra</code> - недоступна") {
		t.Fatalf("notification header wrong:\n%s", text)
	}
	if strings.Contains(text, "—") || strings.Contains(text, "·") {
		t.Fatalf("non-ASCII punctuation in notification:\n%s", text)
	}
}

// Сообщение провайдера показывается всегда (ТЗ §5: оригинальный код и текст).
func TestErrorDescriptionAlwaysShown(t *testing.T) {
	models := []storage.Model{{ID: "gpt-6-astra", Family: "GPT"}}
	checks := map[string]storage.Check{
		"gpt-6-astra": {Status: status.Down, HTTPStatus: 400, ErrorType: "http",
			ErrorMessage: `unknown provider for model gpt-6-astra`},
	}
	text := RenderStatusPanel(models, checks, time.Now(), time.Now(), 15*time.Minute)
	if !strings.Contains(text, "🔴 <code>gpt-6-astra</code> - HTTP 400 · unknown provider for...") {
		t.Fatalf("code + truncated message expected:\n%s", text)
	}

	tr := transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)
	tr.Check.HTTPStatus = 400
	tr.Check.ErrorType = "http"
	tr.Check.ErrorMessage = "unknown provider for model gpt-6-astra"
	ntf := RenderNotification(tr)
	if !strings.Contains(ntf, "HTTP 400") || !strings.Contains(ntf, "unknown provider for model gpt-6-astra") {
		t.Fatalf("notification must show the full message:\n%s", ntf)
	}
}
