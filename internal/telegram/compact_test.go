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
	text := RenderStatusPanel(models, checks, time.Now(), time.Now())
	want := "🟢 <code>deepseek-v4.1-flash-1017</code> - 7.5s | TTFT 7.5s | 12.5 TPS | 5/1"
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
	text := RenderStatusPanel(models, checks, time.Now(), time.Now())
	for _, bad := range []string{"·", "—", "…"} {
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
	text := RenderStatusPanel(models, checks, time.Now(), time.Now())
	if !strings.Contains(text, "🔴 <code>grok-4.6</code> - HTTP 503 |") {
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
	text, kb := RenderSubscribePanel(models, map[string]bool{"glm-5.3-prime": true})
	if strings.Contains(text, "gpt-6-astra") || strings.Contains(text, "glm-5.3-prime") {
		t.Fatalf("model list must live in the keyboard, not the text:\n%s", text)
	}
	if !strings.Contains(text, "Выбрано: 1 модель") {
		t.Fatalf("selection counter missing:\n%s", text)
	}
	if len(kb.InlineKeyboard) != 3 { // 2 модели + Сохранить
		t.Fatalf("keyboard rows = %d", len(kb.InlineKeyboard))
	}
	if kb.InlineKeyboard[0][0].Text != "gpt-6-astra" || kb.InlineKeyboard[1][0].Text != "✓ glm-5.3-prime" {
		t.Fatalf("keyboard = %+v", kb.InlineKeyboard)
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
