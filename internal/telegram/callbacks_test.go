package telegram

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"opentw/internal/storage"
)

// Перестановка списка моделей между созданием клавиатуры и нажатием
// кнопки не должна подменять модель: колбэк несёт ID модели, а не позицию.
func TestCallbackToggleSurvivesConfigReorder(t *testing.T) {
	m := newMockTG(t)
	b, _ := newBot(t, m)
	// клавиатура создана при порядке [gpt-6-astra, glm-5.3];
	// после перезапуска конфигурация — в обратном порядке
	b.models = []storage.Model{{ID: "glm-5.3", Family: "GLM"}, {ID: "gpt-6-astra", Family: "GPT"}}
	if err := b.HandleUpdate(context.Background(), cbUpdate(100, 1, "s:gpt-6-astra")); err != nil {
		t.Fatal(err)
	}
	body := m.lastBody("editMessageText")
	text, markup := fmt.Sprint(body["text"]), fmt.Sprint(body["reply_markup"])
	if !strings.Contains(text, "<b>GPT</b>") || !strings.Contains(markup, "+ gpt-6-astra") {
		t.Fatalf("toggle must affect gpt-6-astra after reorder, text=%q markup=%s", text, markup)
	}
}

// Идентификатора больше нет в конфигурации: кнопка вежливо отвергается
// и ничего не переключает — ни модель, ни семейство.
func TestStaleCallbackRejected(t *testing.T) {
	m := newMockTG(t)
	b, _ := newBot(t, m)
	for _, data := range []string{"s:removed-model", "f:RemovedFamily"} {
		if err := b.HandleUpdate(context.Background(), cbUpdate(100, 1, data)); err != nil {
			t.Fatal(err)
		}
		ack := fmt.Sprint(m.lastBody("answerCallbackQuery")["text"])
		if !strings.Contains(ack, "устарела") {
			t.Fatalf("stale button must warn the user, got %q", ack)
		}
	}
	if m.callCount("editMessageText") != 0 {
		t.Fatal("stale button must not edit the message")
	}
}

// ID длиннее лимита Telegram callback_data (64 байта) кодируется стабильным
// хэшем и всё равно переключает правильную модель.
func TestCallbackDataEncodesLongIDs(t *testing.T) {
	m := newMockTG(t)
	long := strings.Repeat("m", 100)
	models := []storage.Model{{ID: long, Family: "Long"}}
	_, kb := RenderSubscribeFamily("Long", models, nil, nil)
	data := kb.InlineKeyboard[0][0].CallbackData
	if !strings.HasPrefix(data, "s:h:") || len(data) > 64 {
		t.Fatalf("long id must become a short stable hash: %q", data)
	}
	b, _ := newBot(t, m)
	b.models = models
	if err := b.HandleUpdate(context.Background(), cbUpdate(100, 1, data)); err != nil {
		t.Fatal(err)
	}
	markup := fmt.Sprint(m.lastBody("editMessageText")["reply_markup"])
	if !strings.Contains(markup, "+ "+long) {
		t.Fatalf("hashed callback must toggle the long-id model: %s", markup)
	}
}
