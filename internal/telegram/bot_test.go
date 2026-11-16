package telegram

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"opentw/internal/monitor"
	"opentw/internal/status"
	"opentw/internal/storage"
)

func newBot(t *testing.T, m *mockTG) (*Bot, *storage.Store) {
	t.Helper()
	store, err := storage.Open(t.TempDir() + "/bot.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	models := []storage.Model{
		{ID: "gpt-6-astra", Family: "GPT"},
		{ID: "glm-5.3", Family: "GLM"},
	}
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

func check(id string, st status.Status) storage.Check {
	return storage.Check{ModelID: id, At: time.Now(), Status: st, HTTPStatus: 200}
}

// Сценарий: /status создаёт и закрепляет панель.
func TestStatusCommandPinsPanel(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	ctx := context.Background()

	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/status")); err != nil {
		t.Fatal(err)
	}
	if m.callCount("sendMessage") != 1 {
		t.Fatalf("sendMessage calls = %d", m.callCount("sendMessage"))
	}
	if m.callCount("pinChatMessage") != 1 {
		t.Fatalf("panel must be pinned")
	}
	sm, _ := store.StatusMessage(100)
	if sm == nil || sm.MessageID != 1 {
		t.Fatalf("binding = %+v", sm)
	}
	body := m.lastBody("sendMessage")
	if !strings.Contains(fmt.Sprint(body["text"]), "OpenTW") {
		t.Fatalf("panel text = %v", body["text"])
	}
}

// Сценарий: /start — полный синоним /status.
func TestStartIsStatusSynonym(t *testing.T) {
	m := newMockTG(t)
	b, _ := newBot(t, m)
	ctx := context.Background()
	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/start")); err != nil {
		t.Fatal(err)
	}
	if m.callCount("sendMessage") != 1 || m.callCount("pinChatMessage") != 1 {
		t.Fatalf("start must behave like status: send=%d pin=%d", m.callCount("sendMessage"), m.callCount("pinChatMessage"))
	}
	text := fmt.Sprint(m.lastBody("sendMessage")["text"])
	if !strings.Contains(text, "OpenTW") || !strings.Contains(text, "Следующая проверка") {
		t.Fatalf("start must render the status panel, got %q", text)
	}
}

// Сценарий 20: повторный /status открепляет старое, закрепляет новое, обновляет message_id.
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
	if m.callCount("sendMessage") != 2 {
		t.Fatalf("two panels expected, got %d sends", m.callCount("sendMessage"))
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
	sm, _ := store.StatusMessage(100)
	if sm == nil || sm.MessageID != 2 {
		t.Fatalf("binding must point to the new message, got %+v", sm)
	}
}

// Сценарий 21: цикл мониторинга редактирует существующую панель.
func TestCycleEditsPanel(t *testing.T) {
	m := newMockTG(t)
	b, _ := newBot(t, m)
	ctx := context.Background()
	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/status")); err != nil {
		t.Fatal(err)
	}
	checks := map[string]storage.Check{
		"gpt-6-astra": check("gpt-6-astra", status.Operational),
		"glm-5.3":     check("glm-5.3", status.Operational),
	}
	b.OnCycle(ctx, cycle(checks))
	if m.callCount("editMessageText") != 1 {
		t.Fatalf("panel must be edited, got %d", m.callCount("editMessageText"))
	}
	if m.callCount("sendMessage") != 1 {
		t.Fatal("no new messages must be sent on a cycle update")
	}
}

// Текст не изменился — лишнее редактирование не выполняется.
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
	b.OnCycle(ctx, cycle(checks))
	b.OnCycle(ctx, cycle(checks))
	if m.callCount("editMessageText") != 1 {
		t.Fatalf("unchanged text must not be re-edited: %d", m.callCount("editMessageText"))
	}
}

// Сценарий 22: панель удалена (edit → 400) — бот создаёт новую и закрепляет.
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
		if method == "sendMessage" {
			return map[string]any{"ok": true, "result": map[string]any{"message_id": m.nextMessageID()}}, http.StatusOK
		}
		return map[string]any{"ok": true, "result": true}, http.StatusOK
	}
	editFails = true
	b.OnCycle(ctx, cycle(map[string]storage.Check{"gpt-6-astra": check("gpt-6-astra", status.Operational)}))
	if m.callCount("sendMessage") != 2 {
		t.Fatalf("deleted panel must be re-sent: %d", m.callCount("sendMessage"))
	}
	if m.callCount("pinChatMessage") != 2 {
		t.Fatal("re-created panel must be pinned")
	}
	sm, _ := store.StatusMessage(100)
	if sm == nil || sm.MessageID != 2 {
		t.Fatalf("binding must be refreshed: %+v", sm)
	}
}

// Сценарий 19: после рестарта панель восстанавливается из SQLite и редактируется.
func TestPanelRestoredAfterRestart(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetStatusMessage(100, 77); err != nil { // состояние «до рестарта»
		t.Fatal(err)
	}
	ctx := context.Background()
	b.OnCycle(ctx, cycle(map[string]storage.Check{"glm-5.3": check("glm-5.3", status.Operational)}))
	if m.callCount("editMessageText") != 1 {
		t.Fatalf("restored panel must be edited: %d", m.callCount("editMessageText"))
	}
	body := m.lastBody("editMessageText")
	if body["message_id"].(float64) != 77 {
		t.Fatalf("must edit the restored message, got %v", body["message_id"])
	}
	if m.callCount("sendMessage") != 0 {
		t.Fatal("restore must not send new messages")
	}
}

// Сценарий: у бота нет права закреплять — сообщение сохраняется, пользователь уведомляется один раз.
func TestPinForbiddenStillBinds(t *testing.T) {
	m := newMockTG(t)
	m.handler = func(method string, body []byte) (any, int) {
		if method == "pinChatMessage" {
			return map[string]any{"ok": false, "error_code": 400, "description": "Bad Request: not enough rights to manage pinned messages in the chat"}, http.StatusBadRequest
		}
		if method == "sendMessage" {
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

// Сценарий: /status model-id — детальная информация; неизвестный ID — сообщение.
func TestStatusModelDetail(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetModelState("glm-5.3", status.Operational, "", "", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.InsertCheck(storage.Check{ModelID: "glm-5.3", At: time.Now(), HTTPStatus: 200, Status: status.Operational}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/status glm-5.3")); err != nil {
		t.Fatal(err)
	}
	text := fmt.Sprint(m.lastBody("sendMessage")["text"])
	if !strings.Contains(text, "<b>glm-5.3</b> - 🟢 работает") {
		t.Fatalf("detail wrong: %q", text)
	}
	if m.callCount("pinChatMessage") != 0 {
		t.Fatal("detail reply must not be pinned")
	}

	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/status nope")); err != nil {
		t.Fatal(err)
	}
	text = fmt.Sprint(m.lastBody("sendMessage")["text"])
	if !strings.Contains(text, "не найдена") {
		t.Fatalf("unknown model: %q", text)
	}
}

// Сценарий 23: в группах подписки меняют только администраторы.
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

	// обычный участник не может открыть настройки
	if err := b.HandleUpdate(ctx, msgUpdate(-200, 56, "supergroup", "/subscribe")); err != nil {
		t.Fatal(err)
	}
	last := fmt.Sprint(m.lastBody("sendMessage")["text"])
	if !strings.Contains(last, "администратор") {
		t.Fatalf("non-admin refusal expected: %q", last)
	}

	// администратор открывает
	if err := b.HandleUpdate(ctx, msgUpdate(-200, 55, "supergroup", "/subscribe")); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(fmt.Sprint(m.lastBody("sendMessage")["text"]), "Настройка уведомлений") {
		t.Fatalf("admin must see the panel: %q", m.lastBody("sendMessage")["text"])
	}

	// колбэк от не-админа отклоняется
	if err := b.HandleUpdate(ctx, cbUpdateGroup(-200, 56, "s:0")); err != nil {
		t.Fatal(err)
	}
	answered := fmt.Sprint(m.lastBody("answerCallbackQuery")["text"])
	if !strings.Contains(answered, "администратор") {
		t.Fatalf("non-admin callback must be rejected: %q", answered)
	}

	// администратор переключает и сохраняет
	if err := b.HandleUpdate(ctx, cbUpdateGroup(-200, 55, "s:0")); err != nil {
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

// Сохранение подписок через /subscribe.
func TestSubscribeSavePersists(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	ctx := context.Background()
	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/subscribe")); err != nil {
		t.Fatal(err)
	}
	cb := cbUpdate(100, 1, "s:0")
	if err := b.HandleUpdate(ctx, cb); err != nil {
		t.Fatal(err)
	}
	if err := b.HandleUpdate(ctx, cbUpdate(100, 1, "s:1")); err != nil {
		t.Fatal(err)
	}
	if err := b.HandleUpdate(ctx, cbUpdate(100, 1, "save")); err != nil {
		t.Fatal(err)
	}
	subs, _ := store.Subscriptions(100)
	if len(subs) != 2 || subs[0] != "glm-5.3" || subs[1] != "gpt-6-astra" {
		t.Fatalf("saved subscriptions = %v", subs)
	}
	// повторное открытие показывает текущее состояние
	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/subscribe")); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(fmt.Sprint(m.lastBody("sendMessage")["text"]), "Выбрано: 2 модели") {
		t.Fatalf("reopened panel must show current config: %q", m.lastBody("sendMessage")["text"])
	}
}

// Сценарий: повторное открытие /subscribe без взаимодействия использует сохранённое состояние.
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
	// повторный тогл той же кнопки снимает выбор
	if err := b.HandleUpdate(ctx, cbUpdate(100, 1, "s:1")); err != nil {
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

// Каналы: /status постит панель в канал и закрепляет её.
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
	if m.callCount("sendMessage") != 1 || m.callCount("pinChatMessage") != 1 {
		t.Fatalf("channel post must create+pin panel, got send=%d pin=%d", m.callCount("sendMessage"), m.callCount("pinChatMessage"))
	}
	body := m.lastBody("sendMessage")
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

// Кнопка «Отменить»: сообщение меню удаляется, черновик сбрасывается,
// сохранённые подписки не трогаются; повторное /subscribe — от сохранённого.
func TestSubscribeCancel(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	ctx := context.Background()

	// уже сохранённая подписка — должна пережить отмену
	if err := store.SetSubscription(100, "glm-5.3", true); err != nil {
		t.Fatal(err)
	}
	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/subscribe")); err != nil {
		t.Fatal(err)
	}
	if err := b.HandleUpdate(ctx, cbUpdate(100, 1, "s:0")); err != nil { // включили gpt-6-astra в черновике
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
	// повторное открытие: черновик начинается заново от сохранённого состояния
	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/subscribe")); err != nil {
		t.Fatal(err)
	}
	reopened := fmt.Sprint(m.lastBody("sendMessage")["text"])
	if !strings.Contains(reopened, "Выбрано: 1 модель") {
		t.Fatalf("reopened draft must equal saved state: %q", reopened)
	}
}

// Навигация меню подписок: f:<i> открывает экран семейства, back — корень.
func TestSubscribeFamilyNavigation(t *testing.T) {
	m := newMockTG(t)
	b, _ := newBot(t, m)
	ctx := context.Background()
	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/subscribe")); err != nil {
		t.Fatal(err)
	}
	if err := b.HandleUpdate(ctx, cbUpdate(100, 1, "f:0")); err != nil { // GPT
		t.Fatal(err)
	}
	editText := fmt.Sprint(m.lastBody("editMessageText")["text"])
	if !strings.Contains(editText, "<b>GPT</b>") || !strings.Contains(editText, "0 из 1") {
		t.Fatalf("family screen wrong: %q", editText)
	}
	toggleMarkup := fmt.Sprint(m.lastBody("editMessageText")["reply_markup"])
	if !strings.Contains(toggleMarkup, "s:0") || !strings.Contains(toggleMarkup, "back") {
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
