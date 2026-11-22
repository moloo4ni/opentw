package telegram

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/moloo4ni/opentw/internal/monitor"
	"github.com/moloo4ni/opentw/internal/status"
	"github.com/moloo4ni/opentw/internal/storage"
)

func newBot(t *testing.T, m *mockTG) (*Bot, *storage.Store) {
	t.Helper()
	return newBotAt(t, m, t.TempDir()+"/bot.db", []storage.Model{
		{ID: "gpt-6-astra", Family: "GPT"},
		{ID: "glm-5.3", Family: "GLM"},
	})
}

func newBotAt(t *testing.T, m *mockTG, path string, models []storage.Model) (*Bot, *storage.Store) {
	t.Helper()
	store, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	if err := store.SyncModels(models); err != nil {
		t.Fatal(err)
	}
	bot := NewBot(BotCfg{
		API:    &Client{Token: "T", BaseURL: m.url, HTTP: &http.Client{Timeout: 5 * time.Second}},
		Store:  store,
		Models: models,
	})
	return bot, store
}

func msgUpdate(chatID int64, userID int64, chatType, text string) *Update {
	return &Update{
		UpdateID: 1,
		Message: &Message{
			MessageID: 10,
			Chat:      Chat{ID: chatID, Type: chatType},
			From:      &User{ID: userID},
			Text:      text,
		},
	}
}

func cbUpdate(chatID int64, userID int64, data string) *Update {
	return &Update{
		UpdateID: 2,
		CallbackQuery: &CallbackQuery{
			ID:      "cb1",
			From:    User{ID: userID},
			Message: &Message{MessageID: 55, Chat: Chat{ID: chatID, Type: "private"}},
			Data:    data,
		},
	}
}

func cbUpdateGroup(chatID int64, userID int64, data string) *Update {
	u := cbUpdate(chatID, userID, data)
	u.CallbackQuery.Message.Chat.Type = "supergroup"
	return u
}

func cycle(checks map[string]storage.Check, transitions ...monitor.Transition) monitor.CycleResult {
	return monitor.CycleResult{
		CycleAt:     time.Now(),
		NextAt:      time.Now().Add(15 * time.Minute),
		Checks:      checks,
		Transitions: transitions,
	}
}

// check is a successful check "now"; error fields are tuned in place.
func check(id string, st status.Status) storage.Check {
	return storage.Check{ModelID: id, At: time.Now(), Status: st, HTTPStatus: 200}
}

// commitCycle replays the runner's cycle commit: the bot prepares the
// events, and they are committed together with the states by one ApplyCycle.
func commitCycle(t *testing.T, b *Bot, res monitor.CycleResult) {
	t.Helper()
	events, err := b.OnCycle(context.Background(), res)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.store.ApplyCycle(res.Checks, events); err != nil {
		t.Fatal(err)
	}
}

// seedCheck writes the cycle's checks to the DB — as the production monitor
// does before OnCycle (the worker renders panels from store.LastChecks()).
func seedCheck(t *testing.T, b *Bot, checks map[string]storage.Check) {
	t.Helper()
	for _, c := range checks {
		if err := b.store.InsertCheck(c); err != nil {
			t.Fatal(err)
		}
	}
}

// callIndex returns the index of a method's first occurrence in the call
// sequence.
func callIndex(m *mockTG, method string) int {
	for i, c := range m.callList() {
		if c == method {
			return i
		}
	}
	return -1
}

// Scenario: /status sends a rich panel and pins it.
func TestStatusCommandPinsRichPanel(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	ctx := context.Background()

	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/status")); err != nil {
		t.Fatal(err)
	}
	if m.callCount("sendRichMessage") != 1 {
		t.Fatalf("sendRichMessage calls = %d", m.callCount("sendRichMessage"))
	}
	if m.callCount("sendMessage") != 0 {
		t.Fatal("panel must be a rich message, not classic sendMessage")
	}
	if m.callCount("pinChatMessage") != 1 {
		t.Fatalf("panel must be pinned")
	}
	sm, _ := store.StatusMessage(100)
	if sm == nil || sm.MessageID != 1 {
		t.Fatalf("binding = %+v", sm)
	}
	html := lastRichHTML(t, m, "sendRichMessage")
	for _, want := range []string{"<b>OpenTW</b>", panelLegend(), "<code>gpt-6-astra</code>"} {
		if !strings.Contains(html, want) {
			t.Errorf("panel html missing %q:\n%s", want, html)
		}
	}
	if strings.Contains(html, "Модели:") {
		t.Errorf("summary line must be gone:\n%s", html)
	}
}

// Scenario: /start is a full synonym of /status.
func TestStartIsStatusSynonym(t *testing.T) {
	m := newMockTG(t)
	b, _ := newBot(t, m)
	ctx := context.Background()
	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/start")); err != nil {
		t.Fatal(err)
	}
	if m.callCount("sendRichMessage") != 1 || m.callCount("pinChatMessage") != 1 {
		t.Fatalf("start must behave like status: rich=%d pin=%d", m.callCount("sendRichMessage"), m.callCount("pinChatMessage"))
	}
	if html := lastRichHTML(t, m, "sendRichMessage"); !strings.Contains(html, "OpenTW") || !strings.Contains(html, "Первый цикл проверки выполняется") {
		t.Fatalf("start must render the status panel, got %q", html)
	}
}

// Scenario 20: a repeated /status — the new panel is pinned, the old one is
// unpinned only AFTER that; the binding updates.
func TestRepeatStatusReplacesPanel(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	ctx := context.Background()
	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/status")); err != nil {
		t.Fatal(err)
	}
	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/status")); err != nil {
		t.Fatal(err)
	}
	if m.callCount("sendRichMessage") != 2 {
		t.Fatalf("two panels expected, got %d rich sends", m.callCount("sendRichMessage"))
	}
	if m.callCount("pinChatMessage") != 2 {
		t.Fatalf("new panel must be pinned: %d", m.callCount("pinChatMessage"))
	}
	if m.callCount("unpinChatMessage") != 1 {
		t.Fatalf("old panel must be unpinned: %d", m.callCount("unpinChatMessage"))
	}
	unpinBody := m.lastBody("unpinChatMessage")
	if unpinBody["message_id"].(float64) != 1 {
		t.Fatalf("must unpin the OLD message, got %v", unpinBody["message_id"])
	}
	// the new pin comes before the old unpin: never two pinned panels
	if !(callIndex(m, "pinChatMessage") < callIndex(m, "unpinChatMessage")) {
		t.Fatalf("new panel must be pinned before the old one is unpinned: %v", m.callList())
	}
	sm, _ := store.StatusMessage(100)
	if sm == nil || sm.MessageID != 2 {
		t.Fatalf("binding must point to the new message, got %+v", sm)
	}
}

// Pining the new panel failed — the old one is NOT unpinned (there is
// never no pinned panel), the bot tries to re-confirm it.
func TestPinFailureKeepsOldPanelPinned(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	ctx := context.Background()
	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/status")); err != nil {
		t.Fatal(err)
	}
	m.resetCalls()
	m.handler = func(method string, body []byte) (any, int) {
		if method == "pinChatMessage" {
			return map[string]any{"ok": false, "error_code": 400, "description": "Bad Request: not enough rights to manage pinned messages in the chat"}, http.StatusBadRequest
		}
		if method == "sendRichMessage" {
			return map[string]any{"ok": true, "result": map[string]any{"message_id": m.nextMessageID()}}, http.StatusOK
		}
		return map[string]any{"ok": true, "result": true}, http.StatusOK
	}
	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/status")); err != nil {
		t.Fatal(err)
	}
	if m.callCount("unpinChatMessage") != 0 {
		t.Fatalf("old panel must stay pinned while the new one is not: %v", m.callList())
	}
	// the old panel is re-confirmed by a repeated pin attempt
	if m.callCount("pinChatMessage") != 2 { // new (failed) + old (re-try)
		t.Fatalf("pin calls = %d: %v", m.callCount("pinChatMessage"), m.callList())
	}
	sm, _ := store.StatusMessage(100)
	if sm == nil || sm.MessageID != 2 {
		t.Fatalf("binding must still move to the new message: %+v", sm)
	}
}

// Scenario 21: a monitoring cycle edits the existing panel with rich text.
func TestCycleEditsPanel(t *testing.T) {
	m := newMockTG(t)
	b, _ := newBot(t, m)
	ctx := context.Background()
	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/status")); err != nil {
		t.Fatal(err)
	}
	at := time.Now()
	checks := map[string]storage.Check{
		"gpt-6-astra": {ModelID: "gpt-6-astra", At: at, Status: status.Operational, HTTPStatus: 200, LatencyUS: ptr1(2_000_000)},
		"glm-5.3":     {ModelID: "glm-5.3", At: at, Status: status.Operational, HTTPStatus: 200, LatencyUS: ptr1(3_100_000)},
	}
	seedCheck(t, b, checks)
	commitCycle(t, b, cycle(checks))
	b.deliveryTick(ctx)
	if m.callCount("editMessageText") != 1 {
		t.Fatalf("panel must be edited, got %d", m.callCount("editMessageText"))
	}
	if m.callCount("sendRichMessage") != 1 {
		t.Fatal("no new messages must be sent on a cycle update")
	}
	html := lastRichHTML(t, m, "editMessageText")
	if !strings.Contains(html, "🟢 <code>gpt-6-astra</code> - 2.0s") {
		t.Fatalf("edited panel html wrong:\n%s", html)
	}
}

// The text did not change — no needless edit is performed.
func TestCycleSkipsUnchangedText(t *testing.T) {
	m := newMockTG(t)
	b, _ := newBot(t, m)
	ctx := context.Background()
	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/status")); err != nil {
		t.Fatal(err)
	}
	checks := map[string]storage.Check{
		"gpt-6-astra": check("gpt-6-astra", status.Operational),
	}
	seedCheck(t, b, checks)
	commitCycle(t, b, cycle(checks))
	b.deliveryTick(ctx)
	commitCycle(t, b, cycle(checks))
	b.deliveryTick(ctx)
	if m.callCount("editMessageText") != 1 {
		t.Fatalf("unchanged text must not be re-edited: %d", m.callCount("editMessageText"))
	}
}

// "message is not modified" on a panel edit is benign (a double cycle).
func TestPanelEditNotModifiedIsBenign(t *testing.T) {
	m := newMockTG(t)
	b, _ := newBot(t, m)
	ctx := context.Background()
	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/status")); err != nil {
		t.Fatal(err)
	}
	m.handler = func(method string, body []byte) (any, int) {
		if method == "editMessageText" {
			return map[string]any{"ok": false, "error_code": 400, "description": "Bad Request: message is not modified"}, http.StatusBadRequest
		}
		if method == "sendRichMessage" {
			return map[string]any{"ok": true, "result": map[string]any{"message_id": m.nextMessageID()}}, http.StatusOK
		}
		return map[string]any{"ok": true, "result": true}, http.StatusOK
	}
	checks := map[string]storage.Check{"gpt-6-astra": check("gpt-6-astra", status.Operational)}
	seedCheck(t, b, checks)
	commitCycle(t, b, cycle(checks))
	b.deliveryTick(ctx)
	if m.callCount("sendRichMessage") != 1 {
		t.Fatalf("not-modified must not recreate the panel: %v", m.callList())
	}
	if m.callCount("editMessageText") != 1 {
		t.Fatal("edit must have been attempted")
	}
}

// Scenario 22: the panel was deleted (edit → 400 not found) — the bot
// creates a new one and pins it.
func TestDeletedPanelRecreated(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	ctx := context.Background()
	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/status")); err != nil {
		t.Fatal(err)
	}
	editFails := false
	m.handler = func(method string, body []byte) (any, int) {
		if method == "editMessageText" && editFails {
			return map[string]any{"ok": false, "error_code": 400, "description": "Bad Request: message to edit not found"}, http.StatusBadRequest
		}
		if method == "sendRichMessage" {
			return map[string]any{"ok": true, "result": map[string]any{"message_id": m.nextMessageID()}}, http.StatusOK
		}
		return map[string]any{"ok": true, "result": true}, http.StatusOK
	}
	editFails = true
	checks := map[string]storage.Check{"gpt-6-astra": check("gpt-6-astra", status.Operational)}
	seedCheck(t, b, checks)
	commitCycle(t, b, cycle(checks))
	b.deliveryTick(ctx)
	if m.callCount("sendRichMessage") != 2 {
		t.Fatalf("deleted panel must be re-sent: %d", m.callCount("sendRichMessage"))
	}
	if m.callCount("pinChatMessage") != 2 {
		t.Fatal("re-created panel must be pinned")
	}
	sm, _ := store.StatusMessage(100)
	if sm == nil || sm.MessageID != 2 {
		t.Fatalf("binding must be refreshed: %+v", sm)
	}
}

// Scenario 19: after a restart the panel is restored from SQLite and edited.
func TestPanelRestoredAfterRestart(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetStatusMessage(100, 77); err != nil { // the pre-restart state
		t.Fatal(err)
	}
	ctx := context.Background()
	checks := map[string]storage.Check{"glm-5.3": check("glm-5.3", status.Operational)}
	seedCheck(t, b, checks)
	commitCycle(t, b, cycle(checks))
	b.deliveryTick(ctx)
	if m.callCount("editMessageText") != 1 {
		t.Fatalf("restored panel must be edited: %d", m.callCount("editMessageText"))
	}
	body := m.lastBody("editMessageText")
	if body["message_id"].(float64) != 77 {
		t.Fatalf("must edit the restored message, got %v", body["message_id"])
	}
	if m.callCount("sendRichMessage") != 0 {
		t.Fatal("restore must not send new messages")
	}
}

// The bot has no pin right — the message is kept, the user is informed.
func TestPinForbiddenStillBinds(t *testing.T) {
	m := newMockTG(t)
	m.handler = func(method string, body []byte) (any, int) {
		if method == "pinChatMessage" {
			return map[string]any{"ok": false, "error_code": 400, "description": "Bad Request: not enough rights to manage pinned messages in the chat"}, http.StatusBadRequest
		}
		if method == "sendRichMessage" {
			return map[string]any{"ok": true, "result": map[string]any{"message_id": m.nextMessageID()}}, http.StatusOK
		}
		return map[string]any{"ok": true, "result": true}, http.StatusOK
	}
	b, store := newBot(t, m)
	ctx := context.Background()
	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/status")); err != nil {
		t.Fatal(err)
	}
	sm, _ := store.StatusMessage(100)
	if sm == nil {
		t.Fatal("message must be bound even without pin rights")
	}
	warnText := ""
	for _, b := range m.bodies {
		var payload map[string]any
		if json.Unmarshal(b, &payload) == nil {
			if s, ok := payload["text"].(string); ok && strings.Contains(s, "закреп") {
				warnText = s
			}
		}
	}
	if warnText == "" {
		t.Fatal("must inform about the pin limitation")
	}
}

// Scenario: /status model-id — a rich card, not pinned; an unknown ID — a
// message.
func TestStatusModelDetail(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	at := time.Now()
	if err := store.SetModelState("glm-5.3", status.Operational, "", "", at); err != nil {
		t.Fatal(err)
	}
	if err := store.InsertCheck(storage.Check{ModelID: "glm-5.3", At: at, HTTPStatus: 200, Status: status.Operational,
		LatencyUS: ptr1(2_000_000), Response: "OK"}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/status glm-5.3")); err != nil {
		t.Fatal(err)
	}
	if m.callCount("sendRichMessage") != 1 || m.callCount("sendMessage") != 0 {
		t.Fatalf("card must be a rich message: rich=%d classic=%d", m.callCount("sendRichMessage"), m.callCount("sendMessage"))
	}
	if m.callCount("pinChatMessage") != 0 {
		t.Fatal("card must not be pinned")
	}
	html := lastRichHTML(t, m, "sendRichMessage")
	for _, want := range []string{"🟢 <code>glm-5.3</code> - Работает", "<b>Ответ модели</b></p><p>OK</p>"} {
		if !strings.Contains(html, want) {
			t.Errorf("card missing %q:\n%s", want, html)
		}
	}

	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/status nope")); err != nil {
		t.Fatal(err)
	}
	text := fmt.Sprint(m.lastBody("sendMessage")["text"])
	if !strings.Contains(text, "Модель не найдена") {
		t.Fatalf("unknown model: %q", text)
	}
}

// A card without check rows is built from the model state (last_check_at).
func TestModelDetailFallsBackToModelState(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	at := time.Now()
	if err := store.SetModelState("gpt-6-astra", status.Down, "http", "boom", at); err != nil {
		t.Fatal(err)
	}
	if err := b.HandleUpdate(context.Background(), msgUpdate(100, 1, "private", "/status gpt-6-astra")); err != nil {
		t.Fatal(err)
	}
	html := lastRichHTML(t, m, "sendRichMessage")
	for _, want := range []string{"<code>gpt-6-astra</code>", "🔴 <code>gpt-6-astra</code> - Недоступна"} {
		if !strings.Contains(html, want) {
			t.Errorf("fallback card missing %q:\n%s", want, html)
		}
	}
	if strings.Contains(html, "За последние 24 часа") {
		t.Errorf("no history must omit the 24h block:\n%s", html)
	}
}

// A long model response: the card fits the rich-message limit, the tail goes
// as the next rich message; no data is lost or silently truncated.
func TestModelCardLongResponseSentAsFollowups(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	at := time.Now()
	resp := strings.Repeat("ж", 40000) // never appears in the card template itself
	if err := store.SetModelState("gpt-6-astra", status.Down, "http", "upstream boom", at); err != nil {
		t.Fatal(err)
	}
	if err := store.InsertCheck(storage.Check{ModelID: "gpt-6-astra", At: at, Status: status.Down,
		HTTPStatus: 500, ErrorType: "http", Response: resp}); err != nil {
		t.Fatal(err)
	}
	if err := b.HandleUpdate(context.Background(), msgUpdate(100, 1, "private", "/status gpt-6-astra")); err != nil {
		t.Fatal(err)
	}
	if n := m.callCount("sendRichMessage"); n < 2 {
		t.Fatalf("long answer must be split into follow-up rich messages, got %d", n)
	}
	total := 0
	for _, body := range m.callBodies("sendRichMessage") {
		var p map[string]any
		if err := json.Unmarshal(body, &p); err != nil {
			t.Fatal(err)
		}
		html := richHTML(t, p)
		if l := len([]rune(html)); l > maxRichMessage {
			t.Fatalf("each rich message must fit the limit, got %d runes", l)
		}
		total += strings.Count(html, "ж")
	}
	if total != 40000 {
		t.Fatalf("response data lost: %d of 40000 runes", total)
	}
}

// Regression: a long response of escapable characters ("&", ×5 growth) —
// every sent rich message must fit the 32768 limit, no data lost.
func TestModelCardEscapableLongResponseFollowups(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	at := time.Now()
	resp := strings.Repeat("&", 40000)
	if err := store.SetModelState("gpt-6-astra", status.Down, "http", "upstream boom", at); err != nil {
		t.Fatal(err)
	}
	if err := store.InsertCheck(storage.Check{ModelID: "gpt-6-astra", At: at, Status: status.Down,
		HTTPStatus: 500, ErrorType: "http", Response: resp}); err != nil {
		t.Fatal(err)
	}
	if err := b.HandleUpdate(context.Background(), msgUpdate(100, 1, "private", "/status gpt-6-astra")); err != nil {
		t.Fatal(err)
	}
	if n := m.callCount("sendRichMessage"); n < 2 {
		t.Fatalf("escapable long answer must be split, got %d rich messages", n)
	}
	amp := 0
	for _, body := range m.callBodies("sendRichMessage") {
		var p map[string]any
		if err := json.Unmarshal(body, &p); err != nil {
			t.Fatal(err)
		}
		html := richHTML(t, p)
		if l := len([]rune(html)); l > maxRichMessage {
			t.Fatalf("each rich message must fit the limit after escaping, got %d runes", l)
		}
		amp += strings.Count(html, "&amp;")
	}
	if amp != 40000 {
		t.Fatalf("response data lost: %d of 40000 runes", amp)
	}
}

// Scenario 23: in groups only administrators change subscriptions.
func TestSubscribeAdminOnlyInGroups(t *testing.T) {
	m := newMockTG(t)
	m.handler = func(method string, body []byte) (any, int) {
		if method == "getChatMember" {
			var payload map[string]any
			json.Unmarshal(body, &payload)
			if payload["user_id"].(float64) == 55 {
				return map[string]any{"ok": true, "result": map[string]any{"status": "administrator", "user": map[string]any{"id": 55}}}, http.StatusOK
			}
			return map[string]any{"ok": true, "result": map[string]any{"status": "member", "user": map[string]any{"id": 56}}}, http.StatusOK
		}
		if method == "sendMessage" {
			return map[string]any{"ok": true, "result": map[string]any{"message_id": m.nextMessageID()}}, http.StatusOK
		}
		return map[string]any{"ok": true, "result": true}, http.StatusOK
	}
	b, _ := newBot(t, m)
	ctx := context.Background()

	// a regular member cannot open the setup
	if err := b.HandleUpdate(ctx, msgUpdate(-200, 56, "supergroup", "/subscribe")); err != nil {
		t.Fatal(err)
	}
	last := fmt.Sprint(m.lastBody("sendMessage")["text"])
	if !strings.Contains(last, "администратор") {
		t.Fatalf("non-admin refusal expected: %q", last)
	}

	// an admin opens it
	if err := b.HandleUpdate(ctx, msgUpdate(-200, 55, "supergroup", "/subscribe")); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(fmt.Sprint(m.lastBody("sendMessage")["text"]), "Настройка уведомлений") {
		t.Fatalf("admin must see the panel: %q", m.lastBody("sendMessage")["text"])
	}

	// a callback from a non-admin is rejected
	if err := b.HandleUpdate(ctx, cbUpdateGroup(-200, 56, "s:gpt-6-astra")); err != nil {
		t.Fatal(err)
	}
	answered := fmt.Sprint(m.lastBody("answerCallbackQuery")["text"])
	if !strings.Contains(answered, "администратор") {
		t.Fatalf("non-admin callback must be rejected: %q", answered)
	}

	// an admin toggles and saves
	if err := b.HandleUpdate(ctx, cbUpdateGroup(-200, 55, "s:gpt-6-astra")); err != nil {
		t.Fatal(err)
	}
	editMarkup := fmt.Sprint(m.lastBody("editMessageText")["reply_markup"])
	if !strings.Contains(editMarkup, "+ gpt-6-astra") {
		t.Fatalf("toggle must be reflected in the keyboard: %q", editMarkup)
	}
	if err := b.HandleUpdate(ctx, cbUpdateGroup(-200, 55, "s:1")); err != nil {
		t.Fatal(err)
	}
	if err := b.HandleUpdate(ctx, cbUpdateGroup(-200, 55, "save")); err != nil {
		t.Fatal(err)
	}
}

// Saving subscriptions via /subscribe.
func TestSubscribeSavePersists(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	ctx := context.Background()
	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/subscribe")); err != nil {
		t.Fatal(err)
	}
	if err := b.HandleUpdate(ctx, cbUpdate(100, 1, "s:gpt-6-astra")); err != nil {
		t.Fatal(err)
	}
	if err := b.HandleUpdate(ctx, cbUpdate(100, 1, "s:glm-5.3")); err != nil {
		t.Fatal(err)
	}
	if err := b.HandleUpdate(ctx, cbUpdate(100, 1, "save")); err != nil {
		t.Fatal(err)
	}
	subs, _ := store.Subscriptions(100)
	if len(subs) != 2 || subs[0] != "glm-5.3" || subs[1] != "gpt-6-astra" {
		t.Fatalf("saved subscriptions = %v", subs)
	}
	// reopening shows the current state
	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/subscribe")); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(fmt.Sprint(m.lastBody("sendMessage")["text"]), "Выбрано: 2 из 2") {
		t.Fatalf("reopened panel must show current config: %q", m.lastBody("sendMessage")["text"])
	}
}

// Scenario: reopening /subscribe without interaction uses the saved state.
func TestSubscribeToggleIdempotent(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	ctx := context.Background()
	if err := store.SetSubscription(100, "glm-5.3", true); err != nil {
		t.Fatal(err)
	}
	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/subscribe")); err != nil {
		t.Fatal(err)
	}
	// a repeated toggle of the same button removes the selection
	if err := b.HandleUpdate(ctx, cbUpdate(100, 1, "s:glm-5.3")); err != nil {
		t.Fatal(err)
	}
	if err := b.HandleUpdate(ctx, cbUpdate(100, 1, "save")); err != nil {
		t.Fatal(err)
	}
	subs, _ := store.Subscriptions(100)
	if len(subs) != 0 {
		t.Fatalf("untoggled subscription must be removed, got %v", subs)
	}
}

// Channels: /status posts a rich panel into the channel and pins it.
func TestChannelStatusPanel(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	ctx := context.Background()
	upd := &Update{
		UpdateID: 3,
		ChannelPost: &Message{
			MessageID: 500,
			Chat:      Chat{ID: -300, Type: "channel"},
			Text:      "/status",
		},
	}
	if err := b.HandleUpdate(ctx, upd); err != nil {
		t.Fatal(err)
	}
	if m.callCount("sendRichMessage") != 1 || m.callCount("pinChatMessage") != 1 {
		t.Fatalf("channel post must create+pin rich panel, got rich=%d pin=%d", m.callCount("sendRichMessage"), m.callCount("pinChatMessage"))
	}
	body := m.lastBody("sendRichMessage")
	if body["chat_id"].(float64) != -300 {
		t.Fatalf("must post to the channel chat, got %v", body["chat_id"])
	}
	sm, _ := store.StatusMessage(-300)
	if sm == nil {
		t.Fatal("channel binding must be stored")
	}
}

func TestHelpCommand(t *testing.T) {
	m := newMockTG(t)
	b, _ := newBot(t, m)
	if err := b.HandleUpdate(context.Background(), msgUpdate(100, 1, "private", "/help")); err != nil {
		t.Fatal(err)
	}
	if m.callCount("sendMessage") != 1 {
		t.Fatal("help must reply")
	}
	if m.callCount("pinChatMessage") != 0 {
		t.Fatal("help must not be pinned")
	}
}

// The "Cancel" button: the menu message is deleted, the draft is dropped,
// saved subscriptions are untouched; a repeated /subscribe starts from the
// saved state.
func TestSubscribeCancel(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	ctx := context.Background()

	// an already saved subscription must survive the cancel
	if err := store.SetSubscription(100, "glm-5.3", true); err != nil {
		t.Fatal(err)
	}
	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/subscribe")); err != nil {
		t.Fatal(err)
	}
	if err := b.HandleUpdate(ctx, cbUpdate(100, 1, "s:gpt-6-astra")); err != nil { // gpt-6-astra turned on in the draft
		t.Fatal(err)
	}
	if err := b.HandleUpdate(ctx, cbUpdate(100, 1, "cancel")); err != nil {
		t.Fatal(err)
	}
	answered := fmt.Sprint(m.lastBody("answerCallbackQuery")["text"])
	if !strings.Contains(answered, "Отменено") {
		t.Fatalf("ack = %q", answered)
	}
	delBody := m.lastBody("deleteMessage")
	if delBody["chat_id"].(float64) != 100 || delBody["message_id"].(float64) != 55 {
		t.Fatalf("cancel must delete the menu message: %v", delBody)
	}
	subs, _ := store.Subscriptions(100)
	if len(subs) != 1 || subs[0] != "glm-5.3" {
		t.Fatalf("cancel must not touch saved subscriptions: %v", subs)
	}
	// reopening: the draft starts anew from the saved state
	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/subscribe")); err != nil {
		t.Fatal(err)
	}
	reopened := fmt.Sprint(m.lastBody("sendMessage")["text"])
	if !strings.Contains(reopened, "Выбрано: 1 из 2") {
		t.Fatalf("reopened draft must equal saved state: %q", reopened)
	}
}

// Subscribe menu navigation: f:<family> opens the family screen, back — the root.
func TestSubscribeFamilyNavigation(t *testing.T) {
	m := newMockTG(t)
	b, _ := newBot(t, m)
	ctx := context.Background()
	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/subscribe")); err != nil {
		t.Fatal(err)
	}
	if err := b.HandleUpdate(ctx, cbUpdate(100, 1, "f:GPT")); err != nil { // the GPT family
		t.Fatal(err)
	}
	editText := fmt.Sprint(m.lastBody("editMessageText")["text"])
	if !strings.Contains(editText, "<b>GPT</b>") || !strings.Contains(editText, "0 из 1") {
		t.Fatalf("family screen wrong: %q", editText)
	}
	toggleMarkup := fmt.Sprint(m.lastBody("editMessageText")["reply_markup"])
	if !strings.Contains(toggleMarkup, "s:gpt-6-astra") || !strings.Contains(toggleMarkup, "back") {
		t.Fatalf("family keyboard wrong: %s", toggleMarkup)
	}
	if err := b.HandleUpdate(ctx, cbUpdate(100, 1, "back")); err != nil {
		t.Fatal(err)
	}
	rootText := fmt.Sprint(m.lastBody("editMessageText")["text"])
	if !strings.Contains(rootText, "Настройка уведомлений") {
		t.Fatalf("back must return to the root screen: %q", rootText)
	}
}

// /status <long argument>: the reply is truncated to 64 runes so that it
// does not exceed the Telegram limit after escaping.
func TestStatusUnknownModelLongArgTruncated(t *testing.T) {
	m := newMockTG(t)
	b, _ := newBot(t, m)
	arg := strings.Repeat("z", 300)
	if err := b.HandleUpdate(context.Background(), msgUpdate(100, 1, "private", "/status "+arg)); err != nil {
		t.Fatal(err)
	}
	text := fmt.Sprint(m.lastBody("sendMessage")["text"])
	if !strings.Contains(text, "Модель не найдена: ") {
		t.Fatalf("must answer 'not found'.")
	}
	if strings.Contains(text, strings.Repeat("z", 65)) {
		t.Fatalf("argument must be truncated to 64 runes")
	}
}
