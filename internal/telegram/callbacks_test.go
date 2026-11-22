package telegram

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"opentw/internal/status"
	"opentw/internal/storage"
)

// seedModelDetail — a model with a check, so the st: card is complete.
func seedModelDetail(t *testing.T, b *Bot, modelID string, st status.Status, httpStatus int, errType, response string) {
	t.Helper()
	at := time.Now()
	if err := b.store.SetModelState(modelID, st, errType, "err", at); err != nil {
		t.Fatal(err)
	}
	if err := b.store.InsertCheck(storage.Check{ModelID: modelID, At: at, Status: st,
		HTTPStatus: httpStatus, ErrorType: errType, Response: response}); err != nil {
		t.Fatal(err)
	}
}

// An st: button from a private chat: answerCallbackQuery before the heavy
// work, the card goes as a NEW rich message to the same chat, the panel is
// not edited.
func TestStatusCallbackOpensCardPrivate(t *testing.T) {
	m := newMockTG(t)
	b, _ := newBot(t, m)
	seedModelDetail(t, b, "gpt-6-astra", status.Down, 429, "http", "upstream")
	ctx := context.Background()

	if err := b.HandleUpdate(ctx, cbUpdate(100, 7, "st:gpt-6-astra")); err != nil {
		t.Fatal(err)
	}
	if n := m.callCount("answerCallbackQuery"); n != 1 {
		t.Fatalf("callback must be answered, got %d", n)
	}
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("card must be sent as a new rich message, got %d", n)
	}
	// the answer comes before the card (drop the loading indicator)
	if !(callIndex(m, "answerCallbackQuery") < callIndex(m, "sendRichMessage")) {
		t.Fatalf("answerCallbackQuery must precede the card: %v", m.callList())
	}
	if body := m.lastBody("answerCallbackQuery"); body["text"] != "" {
		t.Fatalf("known model needs no toast, got %q", body["text"])
	}
	body := m.lastBody("sendRichMessage")
	if body["chat_id"].(float64) != 100 {
		t.Fatalf("card must go to the callback chat, got %v", body["chat_id"])
	}
	html := richHTML(t, body)
	for _, want := range []string{"🔴 <code>gpt-6-astra</code> - Недоступна", "<b>Ответ API</b>"} {
		if !strings.Contains(html, want) {
			t.Errorf("card missing %q:\n%s", want, html)
		}
	}
	if m.callCount("editMessageText") != 0 {
		t.Fatal("callback must not edit the source message")
	}
}

// Callbacks from a group and a channel: the card goes to the pressing chat.
func TestStatusCallbackFromGroupAndChannel(t *testing.T) {
	m := newMockTG(t)
	b, _ := newBot(t, m)
	seedModelDetail(t, b, "gpt-6-astra", status.Down, 429, "http", "upstream")
	ctx := context.Background()

	// group
	if err := b.HandleUpdate(ctx, cbUpdateGroup(-200, 7, "st:gpt-6-astra")); err != nil {
		t.Fatal(err)
	}
	if body := m.lastBody("sendRichMessage"); body["chat_id"].(float64) != -200 {
		t.Fatalf("group card must go to -200, got %v", body["chat_id"])
	}
	// channel
	cb := cbUpdate(-300, 7, "st:gpt-6-astra")
	cb.CallbackQuery.Message.Chat.Type = "channel"
	if err := b.HandleUpdate(ctx, cb); err != nil {
		t.Fatal(err)
	}
	if body := m.lastBody("sendRichMessage"); body["chat_id"].(float64) != -300 {
		t.Fatalf("channel card must go to -300, got %v", body["chat_id"])
	}
	if m.callCount("getChatMember") != 0 {
		// st: buttons are not the subscribe menu: no permission check needed
		t.Fatalf("st: callback must not require admin checks: %v", m.callList())
	}
}

// A model removed from the config and a garbage payload — a polite toast,
// no card. An unavailable but configured model (other tests open it with
// Down) is NOT a stale button.
func TestStatusCallbackRemovedOrInvalid(t *testing.T) {
	m := newMockTG(t)
	b, _ := newBot(t, m)
	seedModelDetail(t, b, "gpt-6-astra", status.Down, 429, "http", "upstream")
	ctx := context.Background()
	for _, data := range []string{
		"st:removed-model",
		"st:garbage",
		"st:",
		"st:h:0000000000", // a hash matching no model
	} {
		if err := b.HandleUpdate(ctx, cbUpdate(100, 7, data)); err != nil {
			t.Fatalf("data=%q: err = %v", data, err)
		}
		ack := fmt.Sprint(m.lastBody("answerCallbackQuery")["text"])
		if ack != "Модель больше не отслеживается." {
			t.Fatalf("data=%q: toast = %q", data, ack)
		}
	}
	if n := m.callCount("sendRichMessage"); n != 0 {
		t.Fatalf("no card for unknown models, got %d", n)
	}
}

// A callback without a message (a revoked keyboard) — a dedicated toast,
// no panic.
func TestStatusCallbackWithoutMessageOnlyToast(t *testing.T) {
	m := newMockTG(t)
	b, _ := newBot(t, m)
	seedModelDetail(t, b, "gpt-6-astra", status.Down, 429, "http", "upstream")
	cb := &Update{CallbackQuery: &CallbackQuery{ID: "cb1", From: User{ID: 7}, Data: "st:gpt-6-astra"}}
	if err := b.HandleUpdate(context.Background(), cb); err != nil {
		t.Fatal(err)
	}
	if n := m.callCount("answerCallbackQuery"); n != 1 {
		t.Fatalf("toast expected, got %d", n)
	}
	if ack := fmt.Sprint(m.lastBody("answerCallbackQuery")["text"]); ack != "Не удалось открыть карточку модели." {
		t.Fatalf("no message context toast = %q", ack)
	}
	if n := m.callCount("sendRichMessage"); n != 0 {
		t.Fatalf("no chat_id — no card, got %d", n)
	}
}

// The hash form for long ids: st:h:<hash> opens the right model's card.
func TestStatusCallbackHashFormLongID(t *testing.T) {
	m := newMockTG(t)
	long := strings.Repeat("m", 100)
	models := []storage.Model{{ID: long, Family: "Long"}}
	b, _ := newBotAt(t, m, t.TempDir()+"/bot.db", models)
	seedModelDetail(t, b, long, status.Down, 500, "http", "boom")
	data := stButtonData(long) // st:h:<hash>
	if !strings.HasPrefix(data, "st:h:") {
		t.Fatalf("long id must produce the hash form: %q", data)
	}
	if err := b.HandleUpdate(context.Background(), cbUpdate(100, 1, data)); err != nil {
		t.Fatal(err)
	}
	if m.callCount("sendRichMessage") != 1 {
		t.Fatalf("hashed callback must open the card, got %d rich sends", m.callCount("sendRichMessage"))
	}
	if html := lastRichHTML(t, m, "sendRichMessage"); !strings.Contains(html, "<code>"+strings.Repeat("m", 100)+"</code>") {
		t.Fatalf("card must name the long-id model:\n%s", html)
	}
}

// An st: button press on the pinned panel: the card is a separate message,
// the panel is not edited or recreated.
func TestStatusCallbackDoesNotTouchPanel(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	seedModelDetail(t, b, "gpt-6-astra", status.Down, 429, "http", "upstream")
	ctx := context.Background()
	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/status")); err != nil {
		t.Fatal(err)
	}
	sm, _ := store.StatusMessage(100)
	if sm == nil {
		t.Fatal("panel binding expected")
	}
	m.resetCalls()

	if err := b.HandleUpdate(ctx, cbUpdate(100, 1, "st:gpt-6-astra")); err != nil {
		t.Fatal(err)
	}
	if n := m.callCount("editMessageText"); n != 0 {
		t.Fatalf("panel must not be re-edited by a button press: %d", n)
	}
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("exactly the card must be sent, got %d", n)
	}
	sm2, _ := store.StatusMessage(100)
	if sm2 == nil || sm2.MessageID != sm.MessageID {
		t.Fatalf("panel binding must not change: %+v -> %+v", sm, sm2)
	}
}

// Several buttons in one message: every press opens its own card.
func TestStatusCallbackMultipleButtons(t *testing.T) {
	m := newMockTG(t)
	b, _ := newBot(t, m)
	seedModelDetail(t, b, "gpt-6-astra", status.Down, 429, "http", "a")
	seedModelDetail(t, b, "glm-5.3", status.Down, 503, "http", "b")
	ctx := context.Background()
	for _, id := range []string{"gpt-6-astra", "glm-5.3"} {
		if err := b.HandleUpdate(ctx, cbUpdate(100, 1, "st:"+id)); err != nil {
			t.Fatal(err)
		}
	}
	if n := m.callCount("sendRichMessage"); n != 2 {
		t.Fatalf("each button must produce its own card, got %d", n)
	}
	if n := m.callCount("answerCallbackQuery"); n != 2 {
		t.Fatalf("each press must be answered, got %d", n)
	}
}

// Opening a card touches neither subscriptions nor the menu draft.
func TestStatusCallbackDoesNotTouchSubscriptions(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	seedModelDetail(t, b, "gpt-6-astra", status.Down, 429, "http", "upstream")
	if err := store.SetSubscription(100, "glm-5.3", true); err != nil {
		t.Fatal(err)
	}
	// an open menu draft: a model turned on, not saved
	if err := b.HandleUpdate(context.Background(), cbUpdate(100, 1, "s:gpt-6-astra")); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	draftBefore := fmt.Sprint(b.drafts[100])
	b.mu.Unlock()
	if err := b.HandleUpdate(context.Background(), cbUpdate(100, 1, "st:gpt-6-astra")); err != nil {
		t.Fatal(err)
	}
	subs, _ := store.Subscriptions(100)
	if len(subs) != 1 || subs[0] != "glm-5.3" {
		t.Fatalf("st: callback must not write subscriptions: %v", subs)
	}
	b.mu.Lock()
	draftAfter := fmt.Sprint(b.drafts[100])
	b.mu.Unlock()
	if draftAfter != draftBefore {
		t.Fatalf("draft must be untouched by the st: callback: %s -> %s", draftBefore, draftAfter)
	}
}

// A card read failure (the DB is down): the callback is still answered,
// the process does not crash (always answerCallbackQuery).
func TestStatusCallbackReadErrorStillAnswered(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	seedModelDetail(t, b, "gpt-6-astra", status.Down, 429, "http", "upstream")
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.HandleUpdate(context.Background(), cbUpdate(100, 1, "st:gpt-6-astra")); err != nil {
		t.Fatal(err)
	}
	if n := m.callCount("answerCallbackQuery"); n != 1 {
		t.Fatalf("callback must be answered even on a read error, got %d", n)
	}
	if n := m.callCount("sendRichMessage"); n != 0 {
		t.Fatalf("no card on a read error, got %d", n)
	}
}

// Reordering the model list between keyboard creation and a button press
// must not swap the model: the callback carries a model ID, not a position.
func TestCallbackToggleSurvivesConfigReorder(t *testing.T) {
	m := newMockTG(t)
	b, _ := newBot(t, m)
	// the keyboard was created with the order [gpt-6-astra, glm-5.3];
	// after a restart the configuration is in the reverse order
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

// The id is no longer in the configuration: the button is politely rejected
// and toggles nothing — no model, no family.
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

// An id longer than the Telegram callback_data limit (64 bytes) is encoded
// as a stable hash and still toggles the right model.
func TestCallbackDataEncodesLongIDs(t *testing.T) {
	m := newMockTG(t)
	long := strings.Repeat("m", 100)
	models := []storage.Model{{ID: long, Family: "Long"}}
	_, kb := RenderSubscribeFamily("Long", models, nil, nil)
	data := kb.InlineKeyboard[0][0].CallbackData
	if !strings.HasPrefix(data, "s:h:") || len(data) > 64 {
		t.Fatalf("long id must become a short stable hash: %q", data)
	}
	b, _ := newBotAt(t, m, t.TempDir()+"/bot.db", models)
	if err := b.HandleUpdate(context.Background(), cbUpdate(100, 1, data)); err != nil {
		t.Fatal(err)
	}
	markup := fmt.Sprint(m.lastBody("editMessageText")["reply_markup"])
	if !strings.Contains(markup, "+ "+long) {
		t.Fatalf("hashed callback must toggle the long-id model: %s", markup)
	}
}
