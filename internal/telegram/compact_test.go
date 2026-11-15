package telegram

import (
	"strings"
	"testing"
	"time"

	"opentw/internal/status"
	"opentw/internal/storage"
)

// Панель обязана помещаться в одну строку десктопного Telegram (~60 знаков):
// разделители «·», одна десятая секунды, токены «in/out» без слов.
func TestPanelCompactOneLine(t *testing.T) {
	models := []storage.Model{{ID: "deepseek-v4.1-flash-1017", Family: "DeepSeek"}}
	checks := map[string]storage.Check{
		"deepseek-v4.1-flash-1017": {Status: status.Operational, HTTPStatus: 200,
			LatencyUS: ptr1(7_531_000), TTFTUS: ptr1(7_530_000), GenUS: ptr1(1_000), TPS: ptrF(12.53),
			InputTokens: ptr1(5), OutputTokens: ptr1(1)},
	}
	text := RenderStatusPanel(models, checks, time.Now(), time.Now())
	want := "🟢 deepseek-v4.1-flash-1017 — 7.5s·TTFT 7.5s·12.5 TPS·5/1"
	if !strings.Contains(text, want) {
		t.Fatalf("compact line:\nwant: %q\ngot:  %q", want, text)
	}
	if lineLen := len([]rune(want)); lineLen > 62 {
		t.Fatalf("worst-case line still too long: %d", lineLen)
	}
	if strings.Contains(text, "in /") || strings.Contains(text, " out") {
		t.Fatalf("verbose tokens must be gone:\n%s", text)
	}
}

// Строка ошибки тоже обязана влезать: сообщение провайдера аккуратно
// сокращается (полный текст — в /status model-id, ТЗ §7).
func TestPanelErrorLineCompact(t *testing.T) {
	models := []storage.Model{{ID: "grok-4.6", Family: "Grok"}}
	checks := map[string]storage.Check{
		"grok-4.6": {Status: status.Down, HTTPStatus: 503, ErrorType: "http",
			ErrorMessage: "No available accounts. Please retry after 02:01 MSK."},
	}
	text := RenderStatusPanel(models, checks, time.Now(), time.Now())
	for _, l := range strings.Split(text, "\n") {
		if !strings.HasPrefix(l, "🔴") {
			continue
		}
		if len([]rune(l)) > 62 {
			t.Fatalf("error line too long for desktop (%d): %q", len([]rune(l)), l)
		}
	}
	if !strings.Contains(text, "🔴 grok-4.6 — HTTP 503·") {
		t.Fatalf("error head wrong:\n%s", text)
	}
}

// Меню подписок: полный список моделей живёт в кнопках; текст не дублирует
// его — иначе длинное сообщение приходится скроллить до клавиатуры.
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
	if !strings.Contains(text, "OpenTW — Настройка уведомлений") {
		t.Fatalf("header missing:\n%s", text)
	}
	if len(kb.InlineKeyboard) != 3 { // 2 модели + Сохранить
		t.Fatalf("keyboard rows = %d", len(kb.InlineKeyboard))
	}
	if kb.InlineKeyboard[0][0].Text != "gpt-6-astra" || kb.InlineKeyboard[1][0].Text != "✓ glm-5.3-prime" {
		t.Fatalf("keyboard = %+v", kb.InlineKeyboard)
	}
}
