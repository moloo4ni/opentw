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

// The panel must keep the config's model order (families and entries within
// families), not the database's alphabetical order.
func TestPanelKeepsConfigOrder(t *testing.T) {
	m := newMockTG(t)
	store, err := storage.Open(t.TempDir() + "/order.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	// the order as in config.example.yaml: gpt-6-astra, gpt-6.1-sol, gpt-6-sol
	models := []storage.Model{
		{ID: "gpt-6-astra", Family: "GPT"},
		{ID: "gpt-6.1-sol", Family: "GPT"},
		{ID: "gpt-6-sol", Family: "GPT"},
		{ID: "glm-5.3", Family: "GLM"},
	}
	if err := store.SyncModels(models); err != nil {
		t.Fatal(err)
	}
	b := NewBot(BotCfg{API: &Client{Token: "T", BaseURL: m.url}, Store: store, Models: models})
	if err := b.HandleUpdate(context.Background(), msgUpdate(100, 1, "private", "/status")); err != nil {
		t.Fatal(err)
	}
	html := lastRichHTML(t, m, "sendRichMessage")
	iAstra := strings.Index(html, "gpt-6-astra")
	iDot := strings.Index(html, "gpt-6.1-sol")
	iSol := strings.Index(html, "gpt-6-sol")
	if iAstra < 0 || iDot < 0 || iSol < 0 {
		t.Fatalf("missing models in panel:\n%s", html)
	}
	if !(iAstra < iDot && iDot < iSol) {
		t.Fatalf("panel must keep config order (astra, 6.1-sol, 6-sol), got:\n%s", html)
	}
}

// In a channel, /subscribe publishes the panel with buttons: a channel_post
// carries no author, so rights are checked on button press (the author is
// known there).
func TestChannelSubscribePublishesKeyboard(t *testing.T) {
	m := newMockTG(t)
	b, _ := newBot(t, m)
	ctx := context.Background()
	upd := &Update{UpdateID: 4, ChannelPost: &Message{MessageID: 700, Chat: Chat{ID: -300, Type: "channel"}, Text: "/subscribe"}}
	if err := b.HandleUpdate(ctx, upd); err != nil {
		t.Fatal(err)
	}
	body := m.lastBody("sendMessage")
	if body["chat_id"].(float64) != -300 {
		t.Fatalf("must publish to the channel, got %v", body["chat_id"])
	}
	markup, _ := body["reply_markup"].(map[string]any)
	if markup == nil {
		t.Fatal("channel subscribe must include an inline keyboard")
	}
	if !strings.Contains(fmt.Sprint(markup), "f:GPT") {
		t.Fatalf("keyboard must carry family buttons: %v", markup)
	}
	if !strings.Contains(fmt.Sprint(body["text"]), "Настройка уведомлений") {
		t.Fatalf("text = %v", body["text"])
	}
}

// A button press in a channel: a channel admin may change the channel
// chat's subscriptions.
func TestChannelCallbackAdminToggles(t *testing.T) {
	m := newMockTG(t)
	m.handler = func(method string, body []byte) (any, int) {
		if method == "getChatMember" {
			return map[string]any{"ok": true, "result": map[string]any{"status": "administrator", "user": map[string]any{"id": 55}}}, 200
		}
		if method == "sendMessage" {
			return map[string]any{"ok": true, "result": map[string]any{"message_id": m.nextMessageID()}}, 200
		}
		return map[string]any{"ok": true, "result": true}, 200
	}
	b, store := newBot(t, m)
	ctx := context.Background()
	if err := b.HandleUpdate(ctx, &Update{UpdateID: 5, ChannelPost: &Message{MessageID: 700, Chat: Chat{ID: -300, Type: "channel"}, Text: "/subscribe"}}); err != nil {
		t.Fatal(err)
	}
	cb := &Update{UpdateID: 6, CallbackQuery: &CallbackQuery{
		ID: "cb9", From: User{ID: 55}, Data: "s:gpt-6-astra",
		Message: &Message{MessageID: 700, Chat: Chat{ID: -300, Type: "channel"}},
	}}
	if err := b.HandleUpdate(ctx, cb); err != nil {
		t.Fatal(err)
	}
	save := &Update{UpdateID: 7, CallbackQuery: &CallbackQuery{
		ID: "cb10", From: User{ID: 55}, Data: "save",
		Message: &Message{MessageID: 700, Chat: Chat{ID: -300, Type: "channel"}},
	}}
	if err := b.HandleUpdate(ctx, save); err != nil {
		t.Fatal(err)
	}
	subs, _ := store.Subscriptions(-300)
	if len(subs) != 1 || subs[0] != "gpt-6-astra" {
		t.Fatalf("channel subscriptions = %v; admin must be able to configure them", subs)
	}
}

// The subscribe panel in a channel must not show model statuses.
func TestChannelSubscribePanelHasNoStatuses(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetModelState("glm-5.3", status.Down, "http", "boom", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := b.HandleUpdate(context.Background(), &Update{UpdateID: 8, ChannelPost: &Message{MessageID: 1, Chat: Chat{ID: -300, Type: "channel"}, Text: "/subscribe"}}); err != nil {
		t.Fatal(err)
	}
	text := fmt.Sprint(m.lastBody("sendMessage")["text"])
	for _, forbidden := range []string{"🟢", "🔴", "🟡", "⚪", "TPS", "HTTP"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("subscribe panel must not show statuses/metrics, found %q:\n%s", forbidden, text)
		}
	}
}
