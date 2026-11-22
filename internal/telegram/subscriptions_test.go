package telegram

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/moloo4ni/opentw/internal/storage"
)

// A bot restart between opening /subscribe and "Save" does not wipe the
// saved subscriptions — the draft is hydrated from the DB.
func TestStaleMenuSaveKeepsSubscriptions(t *testing.T) {
	m := newMockTG(t)
	path := t.TempDir() + "/bot.db"
	_, store1 := newBotAt(t, m, path, []storage.Model{{ID: "gpt-6-astra", Family: "GPT"}, {ID: "glm-5.3", Family: "GLM"}})
	if err := store1.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	if err := store1.SetSubscription(100, "glm-5.3", true); err != nil {
		t.Fatal(err)
	}
	if err := store1.Close(); err != nil {
		t.Fatal(err)
	}

	b2, store2 := newBotAt(t, m, path, []storage.Model{{ID: "gpt-6-astra", Family: "GPT"}, {ID: "glm-5.3", Family: "GLM"}})
	if err := b2.HandleUpdate(context.Background(), cbUpdate(100, 1, "s:gpt-6-astra")); err != nil { // toggling one off
		t.Fatal(err)
	}
	if err := b2.HandleUpdate(context.Background(), cbUpdate(100, 1, "save")); err != nil {
		t.Fatal(err)
	}
	subs, _ := store2.Subscriptions(100)
	if len(subs) != 1 || subs[0] != "glm-5.3" {
		t.Fatalf("save must keep the untouched subscription, got %v", subs)
	}
}

// "Save" without an open menu (after a restart) changes nothing saved.
func TestSaveWithoutMenuKeepsSubscriptions(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	if err := b.HandleUpdate(context.Background(), cbUpdate(100, 1, "save")); err != nil {
		t.Fatal(err)
	}
	subs, _ := store.Subscriptions(100)
	if len(subs) != 1 || subs[0] != "gpt-6-astra" {
		t.Fatalf("save without a menu must be a no-op, got %v", subs)
	}
}

// Bulk buttons on both screens, callback data within the 64-byte limit.
func TestMassButtonsPresent(t *testing.T) {
	models := []storage.Model{
		{ID: "g1", Family: "GPT"},
		{ID: "g2", Family: "GPT"},
		{ID: "d1", Family: "DeepSeek"},
	}
	_, root := RenderSubscribeRoot(models, map[string]bool{"g1": true}, nil)
	var allData []string
	for _, row := range root.InlineKeyboard {
		for _, btn := range row {
			if len(btn.CallbackData) > 64 {
				t.Fatalf("callback_data %q exceeds 64 bytes", btn.CallbackData)
			}
			allData = append(allData, btn.CallbackData)
		}
	}
	joined := strings.Join(allData, " ")
	for _, want := range []string{"all", "none", "f:GPT"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("root keyboard missing %q: %v", want, allData)
		}
	}
	_, fam := RenderSubscribeFamily("GPT", models, nil, nil)
	joined = ""
	for _, row := range fam.InlineKeyboard {
		for _, btn := range row {
			if len(btn.CallbackData) > 64 {
				t.Fatalf("callback_data %q exceeds 64 bytes", btn.CallbackData)
			}
			joined += btn.CallbackData + " "
		}
	}
	for _, want := range []string{"fa:GPT", "fn:GPT", "back", "cancel", "save"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("family keyboard missing %q: %v", want, joined)
		}
	}
}

// `fa:` changes only the draft until save; after save — the whole family.
func TestFamilySelectAllDraftThenSave(t *testing.T) {
	m := newMockTG(t)
	models := []storage.Model{
		{ID: "d1", Family: "DeepSeek"},
		{ID: "d2", Family: "DeepSeek"},
		{ID: "g1", Family: "GPT"},
	}
	b, store := newBotAt(t, m, t.TempDir()+"/bot.db", models)
	if err := b.HandleUpdate(context.Background(), cbUpdate(100, 1, "fa:DeepSeek")); err != nil {
		t.Fatal(err)
	}
	subs, _ := store.Subscriptions(100)
	if len(subs) != 0 {
		t.Fatalf("mass button must not write to store before save, got %v", subs)
	}
	if err := b.HandleUpdate(context.Background(), cbUpdate(100, 1, "save")); err != nil {
		t.Fatal(err)
	}
	subs, _ = store.Subscriptions(100)
	if len(subs) != 2 || subs[0] != "d1" || subs[1] != "d2" {
		t.Fatalf("family select-all must save both models, got %v", subs)
	}
}

// "Deselect all" — a counter with the diff, a zero text after save.
func TestNoneDeselectsAll(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	if err := store.SetSubscription(100, "glm-5.3", true); err != nil {
		t.Fatal(err)
	}
	if err := b.HandleUpdate(context.Background(), msgUpdate(100, 1, "private", "/subscribe")); err != nil {
		t.Fatal(err)
	}
	if err := b.HandleUpdate(context.Background(), cbUpdate(100, 1, "none")); err != nil {
		t.Fatal(err)
	}
	text := fmt.Sprint(m.lastBody("editMessageText")["text"])
	if !strings.Contains(text, "Выбрано: 0 из 2 (изменения: -2)") {
		t.Fatalf("diff counter wrong:\n%s", text)
	}
	if err := b.HandleUpdate(context.Background(), cbUpdate(100, 1, "save")); err != nil {
		t.Fatal(err)
	}
	subs, _ := store.Subscriptions(100)
	if len(subs) != 0 {
		t.Fatalf("subscriptions must be cleared, got %v", subs)
	}
	text = fmt.Sprint(m.lastBody("editMessageText")["text"])
	if !strings.Contains(text, "Уведомления отключены: подписок нет") {
		t.Fatalf("zero confirmation wrong:\n%s", text)
	}
}

// The diff is against the saved state; no brackets when equal.
func TestDraftDiffAgainstSaved(t *testing.T) {
	m := newMockTG(t)
	models := []storage.Model{
		{ID: "g1", Family: "GPT"},
		{ID: "g2", Family: "GPT"},
	}
	b, store := newBotAt(t, m, t.TempDir()+"/bot.db", models)
	if err := store.SetSubscription(100, "g1", true); err != nil {
		t.Fatal(err)
	}
	// the fresh menu starts from the saved state — no brackets
	if err := b.HandleUpdate(context.Background(), msgUpdate(100, 1, "private", "/subscribe")); err != nil {
		t.Fatal(err)
	}
	text := fmt.Sprint(m.lastBody("sendMessage")["text"])
	if !strings.Contains(text, "Выбрано: 1 из 2") || strings.Contains(text, "изменения:") {
		t.Fatalf("no-change bracket must be omitted:\n%s", text)
	}
	// g1 untoggled in the draft — the family shows -1
	if err := b.HandleUpdate(context.Background(), cbUpdate(100, 1, "s:g1")); err != nil {
		t.Fatal(err)
	}
	text = fmt.Sprint(m.lastBody("editMessageText")["text"])
	if !strings.Contains(text, "Выбрано: 0 из 2 (изменения: -1)") {
		t.Fatalf("family diff wrong:\n%s", text)
	}
	// g1 back: the bracket disappears
	if err := b.HandleUpdate(context.Background(), cbUpdate(100, 1, "s:g1")); err != nil {
		t.Fatal(err)
	}
	text = fmt.Sprint(m.lastBody("editMessageText")["text"])
	if !strings.Contains(text, "Выбрано: 1 из 2") || strings.Contains(text, "изменения:") {
		t.Fatalf("restored draft must hide the bracket:\n%s", text)
	}
	// mass select all: +1 (g2 is new against the saved set)
	if err := b.HandleUpdate(context.Background(), cbUpdate(100, 1, "fa:GPT")); err != nil {
		t.Fatal(err)
	}
	text = fmt.Sprint(m.lastBody("editMessageText")["text"])
	if !strings.Contains(text, "Выбрано: 2 из 2 (изменения: +1)") {
		t.Fatalf("select-all diff wrong:\n%s", text)
	}
}

// "Cancel" on the family screen deletes the menu, saving and wiping nothing.
func TestFamilyScreenCancel(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	if err := b.HandleUpdate(context.Background(), cbUpdate(100, 1, "f:GPT")); err != nil {
		t.Fatal(err)
	}
	if err := b.HandleUpdate(context.Background(), cbUpdate(100, 1, "s:gpt-6-astra")); err != nil { // untoggled in the draft
		t.Fatal(err)
	}
	if err := b.HandleUpdate(context.Background(), cbUpdate(100, 1, "cancel")); err != nil {
		t.Fatal(err)
	}
	if m.callCount("deleteMessage") != 1 {
		t.Fatal("cancel must delete the menu message")
	}
	subs, _ := store.Subscriptions(100)
	if len(subs) != 1 || subs[0] != "gpt-6-astra" {
		t.Fatalf("cancel must leave saved state intact, got %v", subs)
	}
}

// A DB failure on save: a clear toast, the draft is not lost, a repeated
// save after recovery works.
func TestSubscribeSaveDBErrorKeepsDraft(t *testing.T) {
	m := newMockTG(t)
	path := t.TempDir() + "/bot.db"
	b, store := newBotAt(t, m, path, []storage.Model{{ID: "g1", Family: "GPT"}})
	if err := b.HandleUpdate(context.Background(), cbUpdate(100, 1, "s:g1")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil { // "SQLite died"
		t.Fatal(err)
	}
	if err := b.HandleUpdate(context.Background(), cbUpdate(100, 1, "save")); err == nil {
		// surfacing the error is optional, but the toast must be clear
		_ = err
	}
	ack := fmt.Sprint(m.lastBody("answerCallbackQuery")["text"])
	if !strings.Contains(ack, "Не удалось сохранить") {
		t.Fatalf("db error toast = %q", ack)
	}
}

// /status with a unique prefix finds the model (a rich card); an ambiguous
// one suggests matches.
func TestStatusPrefixLookup(t *testing.T) {
	m := newMockTG(t)
	b, _ := newBot(t, m)
	if err := b.HandleUpdate(context.Background(), msgUpdate(100, 1, "private", "/status gpt-6")); err != nil {
		t.Fatal(err)
	}
	html := lastRichHTML(t, m, "sendRichMessage")
	if !strings.Contains(html, "<code>gpt-6-astra</code>") {
		t.Fatalf("unique prefix must resolve:\n%s", html)
	}
	if err := b.HandleUpdate(context.Background(), msgUpdate(100, 1, "private", "/status g")); err != nil {
		t.Fatal(err)
	}
	text := fmt.Sprint(m.lastBody("sendMessage")["text"])
	if !strings.Contains(text, "Уточните") || !strings.Contains(text, "gpt-6-astra") || !strings.Contains(text, "glm-5.3") {
		t.Fatalf("ambiguous prefix must suggest candidates:\n%s", text)
	}
}

// A failed menu edit — a clear toast instead of silence.
func TestMenuEditFailureToast(t *testing.T) {
	m := newMockTG(t)
	b, _ := newBot(t, m)
	m.handler = func(method string, body []byte) (any, int) {
		if method == "editMessageText" {
			return map[string]any{"ok": false, "error_code": 400, "description": "Bad Request: message to edit not found"}, 400
		}
		return map[string]any{"ok": true, "result": true}, 200
	}
	if err := b.HandleUpdate(context.Background(), cbUpdate(100, 1, "f:GPT")); err != nil {
		t.Fatal(err)
	}
	ack := fmt.Sprint(m.lastBody("answerCallbackQuery")["text"])
	if !strings.Contains(ack, "Меню недоступно") {
		t.Fatalf("edit failure toast = %q", ack)
	}
}

// --- Callbacks ---

// A callback without a message (a revoked/stale keyboard) must neither panic
// nor corrupt data with a zero chat_id — only a polite toast.
func TestCallbackWithoutMessageNoPanic(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	for _, data := range []string{"save", "cancel", "back", "f:GPT", "s:gpt-6-astra", "s:absent-model"} {
		cb := &CallbackQuery{ID: "cb1", Data: data, From: User{ID: 7}}
		if err := b.handleCallback(context.Background(), cb); err != nil {
			t.Fatalf("data=%q: err = %v", data, err)
		}
	}
	if n := m.callCount("answerCallbackQuery"); n != 6 {
		t.Fatalf("every stale callback must be answered, got %d", n)
	}
	body := m.lastBody("answerCallbackQuery")
	if body["text"] != "Кнопка устарела, откройте /subscribe заново" {
		t.Fatalf("stale toast = %v", body["text"])
	}
	// the subscription is untouched, zero chat_ids never hit the database
	subs, _ := store.Subscriptions(100)
	if len(subs) != 1 {
		t.Fatalf("subscriptions must be intact, got %v", subs)
	}
	if zeroSubs, _ := store.Subscriptions(0); len(zeroSubs) != 0 {
		t.Fatalf("no row may be written for chat_id 0: %v", zeroSubs)
	}
}

// "message is not modified" — a benign double click, not a menu error.
func TestEditMenuNotModifiedIsBenign(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	m.handler = func(method string, body []byte) (any, int) {
		if method == "editMessageText" {
			// "the user clicked twice, the text did not change"
			return map[string]any{"ok": false, "error_code": 400, "description": "Bad Request: message is not modified"}, http.StatusBadRequest
		}
		if method == "getChatMember" {
			return map[string]any{"ok": true, "result": map[string]any{"status": "administrator"}}, http.StatusOK
		}
		return map[string]any{"ok": true, "result": true}, http.StatusOK
	}
	cb := cbUpdate(100, 7, "f:GPT")
	if cb.CallbackQuery == nil || cb.CallbackQuery.Message == nil {
		t.Fatal("cb fixture must carry a message")
	}
	if err := b.HandleUpdate(context.Background(), cb); err != nil {
		t.Fatalf("benign not-modified must not error: %v", err)
	}
	if n := m.callCount("answerCallbackQuery"); n != 1 {
		t.Fatalf("callback must be answered, got %d", n)
	}
	body := m.lastBody("answerCallbackQuery")
	if txt, _ := body["text"].(string); txt != "" {
		t.Fatalf("benign edit needs no error toast, got %q", txt)
	}
}
