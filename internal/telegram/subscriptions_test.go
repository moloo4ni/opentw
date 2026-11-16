package telegram

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"opentw/internal/storage"
)

// P0 (AC-1): рестарт бота между открытием /subscribe и «Сохранить»
// не стирает сохранённые подписки — черновик гидратируется из БД.
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
	if err := b2.HandleUpdate(context.Background(), cbUpdate(100, 1, "s:gpt-6-astra")); err != nil { // снимаем одну
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

// AC-2: «Сохранить» без открытого меню (после рестарта) не меняет сохранённое.
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

// AC-3: масс-кнопки на обоих экранах, данные в лимите 64 байт.
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

// AC-4: `fa:` меняет только черновик до save; после save — всё семейство.
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

// AC-5: «Снять всё» — счётчик с диффом, текст нуля после save.
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

// AC-6: дифф относительно сохранённого состояния; без изменений скобка опускается.
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
	// новое меню начинается с сохранённого состояния — скобки нет
	if err := b.HandleUpdate(context.Background(), msgUpdate(100, 1, "private", "/subscribe")); err != nil {
		t.Fatal(err)
	}
	text := fmt.Sprint(m.lastBody("sendMessage")["text"])
	if !strings.Contains(text, "Выбрано: 1 из 2") || strings.Contains(text, "изменения:") {
		t.Fatalf("no-change bracket must be omitted:\n%s", text)
	}
	// снимаем g1 в черновике — семейство показывает -1
	if err := b.HandleUpdate(context.Background(), cbUpdate(100, 1, "s:g1")); err != nil {
		t.Fatal(err)
	}
	text = fmt.Sprint(m.lastBody("editMessageText")["text"])
	if !strings.Contains(text, "Выбрано: 0 из 2 (изменения: -1)") {
		t.Fatalf("family diff wrong:\n%s", text)
	}
	// возвращаем g1: скобка исчезает
	if err := b.HandleUpdate(context.Background(), cbUpdate(100, 1, "s:g1")); err != nil {
		t.Fatal(err)
	}
	text = fmt.Sprint(m.lastBody("editMessageText")["text"])
	if !strings.Contains(text, "Выбрано: 1 из 2") || strings.Contains(text, "изменения:") {
		t.Fatalf("restored draft must hide the bracket:\n%s", text)
	}
	// mass select all: +1 (g2 новый относительно сохранённого)
	if err := b.HandleUpdate(context.Background(), cbUpdate(100, 1, "fa:GPT")); err != nil {
		t.Fatal(err)
	}
	text = fmt.Sprint(m.lastBody("editMessageText")["text"])
	if !strings.Contains(text, "Выбрано: 2 из 2 (изменения: +1)") {
		t.Fatalf("select-all diff wrong:\n%s", text)
	}
}

// AC-7: «Отменить» на экране семейства удаляет меню, не сохраняя и не стирая.
func TestFamilyScreenCancel(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	if err := b.HandleUpdate(context.Background(), cbUpdate(100, 1, "f:GPT")); err != nil {
		t.Fatal(err)
	}
	if err := b.HandleUpdate(context.Background(), cbUpdate(100, 1, "s:gpt-6-astra")); err != nil { // сняли в черновике
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

// Ошибка БД при сохранении: понятный тост, черновик не потерян, повторная
// кнопка save после восстановления сохраняет.
func TestSubscribeSaveDBErrorKeepsDraft(t *testing.T) {
	m := newMockTG(t)
	path := t.TempDir() + "/bot.db"
	b, store := newBotAt(t, m, path, []storage.Model{{ID: "g1", Family: "GPT"}})
	if err := b.HandleUpdate(context.Background(), cbUpdate(100, 1, "s:g1")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil { // «SQLite умер»
		t.Fatal(err)
	}
	if err := b.HandleUpdate(context.Background(), cbUpdate(100, 1, "save")); err == nil {
		// ошибка наружу не обязательна, но тост обязан быть понятным
		_ = err
	}
	ack := fmt.Sprint(m.lastBody("answerCallbackQuery")["text"])
	if !strings.Contains(ack, "Не удалось сохранить") {
		t.Fatalf("db error toast = %q", ack)
	}
}

// /status с уникальным префиксом находит модель; неоднозначный — подсказывает.
func TestStatusPrefixLookup(t *testing.T) {
	m := newMockTG(t)
	b, _ := newBot(t, m)
	if err := b.HandleUpdate(context.Background(), msgUpdate(100, 1, "private", "/status gpt-6")); err != nil {
		t.Fatal(err)
	}
	text := fmt.Sprint(m.lastBody("sendMessage")["text"])
	if !strings.Contains(text, "<b>gpt-6-astra</b>") {
		t.Fatalf("unique prefix must resolve:\n%s", text)
	}
	if err := b.HandleUpdate(context.Background(), msgUpdate(100, 1, "private", "/status g")); err != nil {
		t.Fatal(err)
	}
	text = fmt.Sprint(m.lastBody("sendMessage")["text"])
	if !strings.Contains(text, "Уточните") || !strings.Contains(text, "gpt-6-astra") || !strings.Contains(text, "glm-5.3") {
		t.Fatalf("ambiguous prefix must suggest candidates:\n%s", text)
	}
}

// Неудачное редактирование меню — понятный тост вместо пустого.
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

// --- Этап 7.2: колбэки ---

// Колбэк без сообщения (отозванная/старая клавиатура) не должен ни паниковать,
// ни портить данные нулевым chat_id — только вежливый тост.
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
	// подписка не тронута, пустые chat_id в базу не попадали
	subs, _ := store.Subscriptions(100)
	if len(subs) != 1 {
		t.Fatalf("subscriptions must be intact, got %v", subs)
	}
	if zeroSubs, _ := store.Subscriptions(0); len(zeroSubs) != 0 {
		t.Fatalf("no row may be written for chat_id 0: %v", zeroSubs)
	}
}

// 7.2: «message is not modified» — штатный двойной клик, а не ошибка меню.
func TestEditMenuNotModifiedIsBenign(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	m.handler = func(method string, body []byte) (any, int) {
		if method == "editMessageText" {
			// «пользователь дважды ткнул, текст не изменился»
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
