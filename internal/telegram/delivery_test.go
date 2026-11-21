package telegram

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"opentw/internal/monitor"
	"opentw/internal/status"
	"opentw/internal/storage"
)

// cycleAt — цикл с заданным временем (уникальные dedupe-ключи строк).
func cycleAt(at time.Time, trs ...monitor.Transition) monitor.CycleResult {
	return monitor.CycleResult{CycleAt: at, NextAt: at.Add(15 * time.Minute), Transitions: trs}
}

// firstPendingRow возвращает старейшую pending-строку (для манипуляций).
func firstPendingRow(t *testing.T, store *storage.Store) storage.OutboxRow {
	t.Helper()
	rows, err := store.PendingOutbox(10)
	if err != nil || len(rows) == 0 {
		t.Fatalf("pending row expected: %v %v", rows, err)
	}
	return rows[0]
}

// enqueuePending — синтетическая pending-строка с событием модели (для
// прямой проверки сверки актуальности §9).
func enqueuePending(t *testing.T, store *storage.Store, chatID int64, modelIDs ...string) {
	t.Helper()
	payload, err := json.Marshal(deliveryPayload{Trs: trsOf(modelIDs...)})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnqueueOutbox(chatID, "single", fmt.Sprintf("test-%d", time.Now().UnixNano()), payload); err != nil {
		t.Fatal(err)
	}
}

func trsOf(ids ...string) []monitor.Transition {
	out := make([]monitor.Transition, 0, len(ids))
	for _, id := range ids {
		out = append(out, monitor.Transition{ModelID: id, To: status.Down, Check: storage.Check{ModelID: id, Status: status.Down}})
	}
	return out
}

// OnCycle не ходит в сеть: событие ложится в outbox (D3).
func TestOnCycleDoesNotSendDirectly(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	checks := downChecks("gpt-6-astra", 503, "http")
	commitCycle(t, b, cycle(checks, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	if n := m.callCount("sendRichMessage"); n != 0 {
		t.Fatalf("OnCycle must not send, got %d calls", n)
	}
	rows, err := store.PendingOutbox(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ChatID != 100 {
		t.Fatalf("outbox must hold one row for chat 100: %+v", rows)
	}
}

// Много моделей в одном цикле — одно rich-сообщение на чат, строки только
// по его подпискам, порядок — конфиг (§8).
func TestManyModelsOneMessageSubscribedOnly(t *testing.T) {
	m := newMockTG(t)
	models := []storage.Model{
		{ID: "a", Family: "A"},
		{ID: "b", Family: "A"},
		{ID: "c", Family: "B"},
	}
	b, store := newBotAt(t, m, t.TempDir()+"/bot.db", models)
	if err := store.SetSubscription(100, "a", true); err != nil {
		t.Fatal(err)
	}
	if err := store.SetSubscription(100, "b", true); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	checks := map[string]storage.Check{
		"a": {ModelID: "a", At: now, Status: status.Down, HTTPStatus: 503, ErrorType: "http"},
		"b": {ModelID: "b", At: now, Status: status.Down, HTTPStatus: 429, ErrorType: "http"},
		"c": {ModelID: "c", At: now, Status: status.Down, HTTPStatus: 500, ErrorType: "http"},
	}
	seedCheck(t, b, checks)
	commitCycle(t, b, cycle(checks,
		transition("a", status.Operational, status.Down, now, nil),
		transition("b", status.Operational, status.Down, now, nil),
		transition("c", status.Operational, status.Down, now, nil))) // не подписан
	b.deliveryTick(context.Background())

	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("one message expected, got %d", n)
	}
	html := lastRichHTML(t, m, "sendRichMessage")
	if strings.Count(html, "<p>") != 2 {
		t.Fatalf("two lines expected:\n%s", html)
	}
	if !strings.Contains(html, "<p>🔴 <code>a</code> - "+stBtn("a", "HTTP 503")+"</p>") {
		t.Errorf("line a missing:\n%s", html)
	}
	if !strings.Contains(html, "<p>🔴 <code>b</code> - "+stBtn("b", "HTTP 429")+"</p>") {
		t.Errorf("line b missing:\n%s", html)
	}
	if strings.Contains(html, "<code>c</code>") {
		t.Errorf("unsubscribed model leaked into chat message:\n%s", html)
	}
	// порядок строк — порядок конфига: a раньше b
	if !(strings.Index(html, "<code>a</code>") < strings.Index(html, "<code>b</code>")) {
		t.Errorf("config order broken:\n%s", html)
	}
}

// Сценарий A (§9): падение и восстановление во время простоя доставки при
// notified=operational — тишина, строки помечаются sent.
func TestDeliveryScenarioAFullReturnToKnownState(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	if err := store.SetChatNotifiedState(100, "gpt-6-astra", status.Operational); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	down := downChecks("gpt-6-astra", 503, "http")
	seedCheck(t, b, down)
	commitCycle(t, b, cycle(down, transition("gpt-6-astra", status.Operational, status.Down, now, nil)))
	commitCycle(t, b, cycle(map[string]storage.Check{"gpt-6-astra": {ModelID: "gpt-6-astra", At: now, Status: status.Operational, HTTPStatus: 200}},
		transition("gpt-6-astra", status.Down, status.Operational, now.Add(time.Minute), nil)))

	b.deliveryTick(context.Background())
	if n := m.callCount("sendRichMessage"); n != 0 {
		t.Fatalf("cur == notified — тишина, got %d sends", n)
	}
	if rows, _ := store.PendingOutbox(10); len(rows) != 0 {
		t.Fatalf("rows must be marked sent: %+v", rows)
	}
	// состояние уведомлённости не изменилось
	st, ok, _ := store.ChatNotifiedState(100, "gpt-6-astra")
	if !ok || st != status.Operational {
		t.Fatalf("chat_notified must stay operational, got %q ok=%v", st, ok)
	}
}

// Сценарий B (§9): сбой держится — одна красная строка; повторная строка
// при cur == notified молчит.
func TestDeliveryScenarioBDownPersistsOneLine(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	checks := downChecks("gpt-6-astra", 429, "http")
	seedCheck(t, b, checks)
	commitCycle(t, b, cycle(checks, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	b.deliveryTick(context.Background())

	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("one line expected, got %d sends", n)
	}
	html := lastRichHTML(t, m, "sendRichMessage")
	if !strings.Contains(html, "<p>🔴 <code>gpt-6-astra</code> - "+stBtn("gpt-6-astra", "HTTP 429")+"</p>") {
		t.Fatalf("down line wrong:\n%s", html)
	}
	if rows, _ := store.PendingOutbox(10); len(rows) != 0 {
		t.Fatalf("rows must be drained: %+v", rows)
	}
	st, ok, _ := store.ChatNotifiedState(100, "gpt-6-astra")
	if !ok || st != status.Down {
		t.Fatalf("chat_notified must be down, got %q ok=%v", st, ok)
	}

	// пользователь уже знает: новая pending-строка той же модели молчит
	enqueuePending(t, store, 100, "gpt-6-astra")
	b.deliveryTick(context.Background())
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("cur == notified must stay silent, got %d sends", n)
	}
	if rows, _ := store.PendingOutbox(10); len(rows) != 0 {
		t.Fatalf("known row must be marked sent: %+v", rows)
	}
}

// Сценарий C (§9): несколько переключений — ОДНА строка по ТЕКУЩЕМУ
// состоянию, label из ПОСЛЕДНЕЙ проверки (не из исторической в payload).
func TestDeliveryScenarioCOneLineFromCurrentState(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	// историческая проверка — 503, последняя — 429: в строке должен быть 429
	if err := b.store.InsertCheck(storage.Check{ModelID: "gpt-6-astra", At: now.Add(-30 * time.Minute),
		Status: status.Down, HTTPStatus: 503, ErrorType: "http"}); err != nil {
		t.Fatal(err)
	}
	fresh := storage.Check{ModelID: "gpt-6-astra", At: now, Status: status.Down, HTTPStatus: 429, ErrorType: "http"}
	if err := b.store.InsertCheck(fresh); err != nil {
		t.Fatal(err)
	}
	// исторический payload несёт 503 — строка строится по актуальному check
	tr := transition("gpt-6-astra", status.Operational, status.Down, now.Add(-30*time.Minute), nil)
	commitCycle(t, b, cycleAt(now.Add(-30*time.Minute), tr))
	commitCycle(t, b, cycleAt(now.Add(-15*time.Minute),
		transition("gpt-6-astra", status.Down, status.Operational, now.Add(-15*time.Minute), nil)))
	// финальное состояние — снова down (check 429)
	final := map[string]storage.Check{"gpt-6-astra": fresh}
	commitCycle(t, b, cycle(final, transition("gpt-6-astra", status.Operational, status.Down, now, nil)))

	b.deliveryTick(context.Background())
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("one actual line expected, got %d sends", n)
	}
	html := lastRichHTML(t, m, "sendRichMessage")
	if strings.Count(html, "<p>") != 1 {
		t.Fatalf("exactly one line expected:\n%s", html)
	}
	if !strings.Contains(html, stBtn("gpt-6-astra", "HTTP 429")) {
		t.Fatalf("label must come from the LATEST check (429), not payload (503):\n%s", html)
	}
	if strings.Contains(html, "HTTP 503") {
		t.Fatalf("historical payload label must not surface:\n%s", html)
	}
	if strings.Contains(html, "📬") || strings.Contains(html, "дайджест") {
		t.Fatalf("digest must not exist:\n%s", html)
	}
	st, ok, _ := store.ChatNotifiedState(100, "gpt-6-astra")
	if !ok || st != status.Down {
		t.Fatalf("chat_notified must be the current state, got %q ok=%v", st, ok)
	}
}

// Сценарий D (§9/OPW-01): текущее состояние Unknown — тишина без
// уничтожения свидетельства: строки НЕ помечаются sent, chat_notified НЕ
// обновляется; вернувшееся достоверное состояние доезжает уведомлением.
func TestDeliveryScenarioDUnknownSilent(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	checks := downChecks("gpt-6-astra", 503, "http")
	commitCycle(t, b, cycle(checks, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	// к моменту доставки монитор потерял зрение: last_status = Unknown
	if err := store.SetModelState("gpt-6-astra", status.Unknown, "invalid", "unparseable", time.Now()); err != nil {
		t.Fatal(err)
	}
	b.deliveryTick(context.Background())
	if n := m.callCount("sendRichMessage"); n != 0 {
		t.Fatalf("cur Unknown — тишина, got %d sends", n)
	}
	// OPW-01: неопределённость не имеет права поглощать единственное
	// свидетельство о недоставленном падении — строка обязана остаться pending
	if rows, _ := store.PendingOutbox(10); len(rows) != 1 {
		t.Fatalf("row must stay pending while cur is Unknown: %+v", rows)
	}
	if rows, _ := store.PendingOutbox(10); rows[0].NextAttemptAt == nil {
		t.Fatal("stuck head must be parked with a short retry, not hot-loop every tick")
	}
	if _, ok, _ := store.ChatNotifiedState(100, "gpt-6-astra"); ok {
		t.Fatal("Unknown must not update chat_notified")
	}
	// достоверное состояние вернулось — уведомление о падении доезжает
	if err := store.SetModelState("gpt-6-astra", status.Down, "http", "HTTP 503", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.DeferOutboxRetry(firstPendingRow(t, store).ID, -time.Hour); err != nil {
		t.Fatal(err)
	}
	b.deliveryTick(context.Background())
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("down notification must arrive once cur is definite, got %d sends", n)
	}
	html := lastRichHTML(t, m, "sendRichMessage")
	if !strings.Contains(html, "<p>🔴 <code>gpt-6-astra</code>") {
		t.Fatalf("down line expected:\n%s", html)
	}
	if rows, _ := store.PendingOutbox(10); len(rows) != 0 {
		t.Fatalf("rows must drain after delivery: %+v", rows)
	}
	st, ok, _ := store.ChatNotifiedState(100, "gpt-6-astra")
	if !ok || st != status.Down {
		t.Fatalf("chat_notified must record the delivered state, got %q ok=%v", st, ok)
	}
}

// Отписка во время простоя — строка выбрасывается без отправки (§9).
func TestDeliveryUnsubscribeDuringOutageDropsRows(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	checks := downChecks("gpt-6-astra", 503, "http")
	commitCycle(t, b, cycle(checks, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	if err := store.SetSubscription(100, "gpt-6-astra", false); err != nil {
		t.Fatal(err)
	}
	b.deliveryTick(context.Background())
	if n := m.callCount("sendRichMessage"); n != 0 {
		t.Fatalf("unsubscribed chat must not receive the backlog, got %d sends", n)
	}
	if rows, _ := store.PendingOutbox(10); len(rows) != 0 {
		t.Fatalf("rows must be dropped: %+v", rows)
	}
}

// Повторный OnCycle с тем же временем цикла не дублирует строку (идемпотентность).
func TestOutboxDedupeSameCycle(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	checks := downChecks("gpt-6-astra", 503, "http")
	seedCheck(t, b, checks)
	res := cycle(checks, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil))
	commitCycle(t, b, res)
	commitCycle(t, b, res)
	rows, _ := store.PendingOutbox(10)
	if len(rows) != 1 {
		t.Fatalf("dedupe failed: %+v", rows)
	}
	b.deliveryTick(context.Background())
	b.deliveryTick(context.Background())
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("message must be sent once, got %d", n)
	}
}

// Событие переживает рестарт процесса: тот же файл БД, новый Bot.
func TestOutboxSurvivesRestart(t *testing.T) {
	m := newMockTG(t)
	path := t.TempDir() + "/bot.db"
	models := []storage.Model{{ID: "gpt-6-astra", Family: "GPT"}, {ID: "glm-5.3", Family: "GLM"}}
	b1, store1 := newBotAt(t, m, path, models)
	if err := store1.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	checks := downChecks("gpt-6-astra", 503, "http")
	seedCheck(t, b1, checks)
	commitCycle(t, b1, cycle(checks, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	if err := store1.Close(); err != nil {
		t.Fatal(err)
	}

	b2, _ := newBotAt(t, m, path, models)
	b2.deliveryTick(context.Background())
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("notification must survive restart, got %d sends", n)
	}
	html := lastRichHTML(t, m, "sendRichMessage")
	if !strings.Contains(html, "<p>🔴 <code>gpt-6-astra</code>") {
		t.Fatalf("text = %q", html)
	}
	b2.deliveryTick(context.Background())
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("delivered row must not resend, got %d", n)
	}
}

// D1: новое событие чата, закоммиченное ВО ВРЕМЯ отправки его свёртки, не
// помечается отправленным вместе с backlog и доезжает отдельным сообщением.
func TestNewRowDuringSendStaysPending(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	for _, id := range []string{"gpt-6-astra", "glm-5.3"} {
		if err := store.SetSubscription(100, id, true); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now()
	down := map[string]storage.Check{"gpt-6-astra": {ModelID: "gpt-6-astra", At: now, Status: status.Down, HTTPStatus: 503, ErrorType: "http"}}
	seedCheck(t, b, down)
	commitCycle(t, b, cycle(down, transition("gpt-6-astra", status.Operational, status.Down, now, nil)))

	var sent []string
	pendingAtSeparateSend := -1
	m.handler = func(method string, body []byte) (any, int) {
		if method != "sendRichMessage" {
			return map[string]any{"ok": true, "result": true}, http.StatusOK
		}
		var p map[string]any
		json.Unmarshal(body, &p)
		if len(sent) == 0 {
			// первая отправка в полёте: цикл коммитит новое событие того же чата
			up := map[string]storage.Check{"glm-5.3": {ModelID: "glm-5.3", At: time.Now(), Status: status.Operational, HTTPStatus: 200}}
			commitCycle(t, b, cycle(up, transition("glm-5.3", status.Down, status.Operational, time.Now(), nil)))
		} else if pendingAtSeparateSend < 0 {
			rows, _ := store.PendingOutbox(10)
			pendingAtSeparateSend = len(rows)
		}
		sent = append(sent, richHTML(t, p))
		return map[string]any{"ok": true, "result": map[string]any{"message_id": m.nextMessageID()}}, http.StatusOK
	}
	b.deliveryTick(context.Background())

	if len(sent) != 2 {
		t.Fatalf("collapsed + separate delivery expected, got %d sends: %v", len(sent), sent)
	}
	// первое сообщение — только старое событие
	if !strings.Contains(sent[0], "<p>🔴 <code>gpt-6-astra</code>") || strings.Contains(sent[0], "glm-5.3") {
		t.Fatalf("first message must hold only the old backlog:\n%s", sent[0])
	}
	// второе — отдельное уведомление о новом событии
	if !strings.Contains(sent[1], "<p>🟢 <code>glm-5.3</code> - Восстановилась</p>") {
		t.Fatalf("new row must be delivered separately:\n%s", sent[1])
	}
	if pendingAtSeparateSend != 1 {
		t.Fatalf("the new row must still be pending when its own send starts, got %d rows", pendingAtSeparateSend)
	}
	if rows, _ := store.PendingOutbox(10); len(rows) != 0 {
		t.Fatalf("everything must be delivered after the tick: %+v", rows)
	}
}

// §9.4: «отправка есть — отчёта нет» (MarkOutboxSent не прошёл) — очередь
// не фризится: повтор доезжает дубликатом (at-least-once), БД не ломается.
func TestSentButReportLostRecovers(t *testing.T) {
	m := newMockTG(t)
	path := t.TempDir() + "/bot.db"
	models := []storage.Model{{ID: "gpt-6-astra", Family: "GPT"}, {ID: "glm-5.3", Family: "GLM"}}
	b, store := newBotAt(t, m, path, models)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	var closed atomic.Bool
	m.handler = func(method string, body []byte) (any, int) {
		if method == "sendRichMessage" {
			if closed.CompareAndSwap(false, true) {
				_ = store.Close() // отчёт о доставке теряется сразу после отправки
			}
			return map[string]any{"ok": true, "result": map[string]any{"message_id": m.nextMessageID()}}, http.StatusOK
		}
		return map[string]any{"ok": true, "result": true}, http.StatusOK
	}
	checks := downChecks("gpt-6-astra", 503, "http")
	seedCheck(t, b, checks)
	commitCycle(t, b, cycle(checks, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	b.deliveryTick(context.Background())

	// тот же файл БД, новый процесс: строка всё ещё pending и доезжает повтором
	b2, store2 := newBotAt(t, m, path, models)
	if rows, _ := store2.PendingOutbox(10); len(rows) != 1 {
		t.Fatalf("undelivered row must survive a lost report: %+v", rows)
	}
	// коммит после отправки не прошёл — ничего не зафиксировано: ни пометок
	// строк, ни уведомлённого состояния (единая транзакция, §4.1)
	if _, ok, _ := store2.ChatNotifiedState(100, "gpt-6-astra"); ok {
		t.Fatal("failed commit must not record chat_notified")
	}
	b2.deliveryTick(context.Background())
	if rows, _ := store2.PendingOutbox(10); len(rows) != 0 {
		t.Fatalf("row must eventually be marked sent: %+v", rows)
	}
	if n := m.callCount("sendRichMessage"); n < 2 {
		t.Fatalf("at-least-once: a duplicate send is expected, got %d", n)
	}
	st, ok, _ := store2.ChatNotifiedState(100, "gpt-6-astra")
	if !ok || st != status.Down {
		t.Fatalf("notified state must be recorded on the retry: %q ok=%v", st, ok)
	}
}

// Временный сбой Telegram: строка остаётся, ретраится не раньше backoff (6.1).
func TestTelegramTransientFailurePostpones(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	var fail atomic.Bool
	fail.Store(true)
	m.handler = func(method string, body []byte) (any, int) {
		if method == "sendRichMessage" && fail.Load() {
			return map[string]any{"ok": false, "error_code": 500, "description": "Internal Server Error"}, http.StatusInternalServerError
		}
		return map[string]any{"ok": true, "result": map[string]any{"message_id": m.nextMessageID()}}, http.StatusOK
	}
	checks := downChecks("gpt-6-astra", 503, "http")
	seedCheck(t, b, checks)
	commitCycle(t, b, cycle(checks, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	b.deliveryTick(context.Background())
	if n := m.callCount("sendRichMessage"); n == 0 {
		t.Fatal("send attempt expected")
	}
	rows, _ := store.PendingOutbox(10)
	if len(rows) != 1 {
		t.Fatalf("failed row must stay pending: %+v", rows)
	}
	// до истечения backoff — никакого повторного запроса
	fail.Store(false)
	m.handler = nil
	b.deliveryTick(context.Background())
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("row must not be retried before the backoff elapses, sends = %d", n)
	}
	// срок настал — строка доезжает
	if err := store.DeferOutboxAttempt(rows[0].ID, -time.Hour); err != nil {
		t.Fatal(err)
	}
	b.deliveryTick(context.Background())
	if n := m.callCount("sendRichMessage"); n < 2 {
		t.Fatalf("row must be retried after maturity, sends = %d", n)
	}
	rows, _ = store.PendingOutbox(10)
	if len(rows) != 0 {
		t.Fatalf("delivered row must be marked: %+v", rows)
	}
}

// 429 исчерпал ретраи — событие не теряется, доезжает после нормализации.
func Test429ExhaustionKeepsRow(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	var limited atomic.Bool
	limited.Store(true)
	m.handler = func(method string, body []byte) (any, int) {
		if method == "sendRichMessage" && limited.Load() {
			return map[string]any{"ok": false, "error_code": 429, "description": "Too Many Requests",
				"parameters": map[string]any{"retry_after": 0}}, http.StatusTooManyRequests
		}
		return map[string]any{"ok": true, "result": map[string]any{"message_id": m.nextMessageID()}}, http.StatusOK
	}
	checks := downChecks("gpt-6-astra", 503, "http")
	seedCheck(t, b, checks)
	commitCycle(t, b, cycle(checks, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	b.deliveryTick(context.Background())
	rows, _ := store.PendingOutbox(10)
	if len(rows) != 1 {
		t.Fatalf("row must survive 429 exhaustion: %+v", rows)
	}
	if rows[0].NextAttemptAt == nil {
		t.Fatal("429 must defer the row until retry_after elapses (P1-4)")
	}
	limited.Store(false)
	m.handler = nil
	// повтор не раньше retry_after (floor 1с) и после снятия паузы батча
	time.Sleep(time.Until(*rows[0].NextAttemptAt) + 20*time.Millisecond)
	b.floodUntil.Store(0)
	b.deliveryTick(context.Background())
	rows, _ = store.PendingOutbox(10)
	if len(rows) != 0 {
		t.Fatalf("row must be delivered after rate limit lifted: %+v", rows)
	}
}

// Завершение во время рассылки: недоставленное остаётся в БД.
func TestDeliveryShutdownKeepsPending(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	blocking := make(chan struct{})
	m.handler = func(method string, body []byte) (any, int) {
		if method == "sendRichMessage" {
			<-blocking
		}
		return map[string]any{"ok": true, "result": map[string]any{"message_id": m.nextMessageID()}}, http.StatusOK
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.RunDelivery(ctx) }()
	checks := downChecks("gpt-6-astra", 503, "http")
	seedCheck(t, b, checks)
	commitCycle(t, b, cycle(checks, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	deadline := time.Now().Add(2 * time.Second)
	for m.callCount("sendRichMessage") == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunDelivery must exit on ctx cancel")
	}
	close(blocking)
	rows, _ := store.PendingOutbox(10)
	if len(rows) != 1 {
		t.Fatalf("undelivered notification must persist: %+v", rows)
	}
}

// Панели обновляются воркером из LastChecks: неполный цикл не затирает
// известное состояние «нет данных» (D1).
func TestPanelsSyncedInWorkerFromLastChecks(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	ctx := context.Background()
	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/status")); err != nil {
		t.Fatal(err)
	}
	at := time.Now()
	if err := store.InsertCheck(storage.Check{ModelID: "gpt-6-astra", At: at, Status: status.Operational, HTTPStatus: 200, LatencyUS: ptr1(2_000_000)}); err != nil {
		t.Fatal(err)
	}
	if err := store.InsertCheck(storage.Check{ModelID: "glm-5.3", At: at, Status: status.Operational, HTTPStatus: 200, LatencyUS: ptr1(2_000_000)}); err != nil {
		t.Fatal(err)
	}
	// «цикл», в котором проверка glm-5.3 отменена: в res.Checks её нет
	commitCycle(t, b, cycle(map[string]storage.Check{
		"gpt-6-astra": {ModelID: "gpt-6-astra", At: at, Status: status.Operational, HTTPStatus: 200, LatencyUS: ptr1(2_000_000)},
	}))
	b.deliveryTick(ctx)
	if n := m.callCount("editMessageText"); n != 1 {
		t.Fatalf("panel edit expected, got %d", n)
	}
	html := lastRichHTML(t, m, "editMessageText")
	if !strings.Contains(html, "<p><b>GLM</b><br/>🟢 <code>glm-5.3</code> - 2.0s</p>") {
		t.Fatalf("known state must survive a partial cycle (D1):\n%s", html)
	}
	if strings.Contains(html, "Нет данных</tg-button>") {
		t.Fatalf("no-data must not replace a known state:\n%s", html)
	}
}

// 6.1: канонический график нарастающего backoff: 30с, 1м, 2м, 4м, 8м,
// потолок 15м (и дальше — потолок).
func TestDeferBackoffSchedule(t *testing.T) {
	want := []time.Duration{30 * time.Second, 60 * time.Second, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 15 * time.Minute, 15 * time.Minute}
	for i, w := range want {
		if got := deferBackoff(i); got != w {
			t.Errorf("deferBackoff(%d) = %v, want %v", i, got, w)
		}
	}
}

// 6.1: временная ошибка доставки → нарастающий backoff, строка не удаляется
// и не ретраится раньше срока.
func TestTemporaryErrorBackoff(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	var fail atomic.Bool
	fail.Store(true)
	m.handler = func(method string, body []byte) (any, int) {
		if method == "sendRichMessage" && fail.Load() {
			return map[string]any{"ok": false, "error_code": 500, "description": "Internal Server Error"}, http.StatusInternalServerError
		}
		return map[string]any{"ok": true, "result": map[string]any{"message_id": m.nextMessageID()}}, http.StatusOK
	}
	checks := downChecks("gpt-6-astra", 503, "http")
	seedCheck(t, b, checks)
	commitCycle(t, b, cycle(checks, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))

	// первая неудача: пауза ~30с
	b.deliveryTick(context.Background())
	row := firstPendingRow(t, store)
	if row.NextAttemptAt == nil {
		t.Fatal("temporary failure must defer the row (backoff)")
	}
	d := time.Until(*row.NextAttemptAt)
	if d < 25*time.Second || d > 35*time.Second {
		t.Fatalf("first backoff = %v, want ~30s", d)
	}
	// до истечения backoff строка не ретраится
	b.deliveryTick(context.Background())
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("row must not be retried before the backoff elapses, sends = %d", n)
	}
	// «прошло 30 секунд»: вторая неудача — пауза строго больше первой
	if err := store.DeferOutboxAttempt(row.ID, -time.Hour); err != nil {
		t.Fatal(err)
	}
	b.deliveryTick(context.Background())
	row = firstPendingRow(t, store)
	if row.NextAttemptAt == nil {
		t.Fatal("second failure must defer again")
	}
	d2 := time.Until(*row.NextAttemptAt)
	if d2 < 2*d {
		t.Fatalf("backoff must grow: first = %v, second = %v", d, d2)
	}
	// строка дожила и доехала: backoff не удаляет события
	fail.Store(false)
	m.handler = nil
	if err := store.DeferOutboxAttempt(row.ID, -time.Hour); err != nil {
		t.Fatal(err)
	}
	b.deliveryTick(context.Background())
	if rows, _ := store.PendingOutbox(10); len(rows) != 0 {
		t.Fatalf("row must be delivered after recovery: %+v", rows)
	}
}

// 6.2: недоступные чаты не занимают слоты батча — живой чат получает
// уведомление немедленно, сколько бы ни стояло за головами мёртвых чатов.
func TestUnavailableChatsDoNotStarveLiveOnes(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil { // «мёртвый» чат
		t.Fatal(err)
	}
	if err := store.SetSubscription(200, "glm-5.3", true); err != nil { // живой чат
		t.Fatal(err)
	}
	m.handler = func(method string, body []byte) (any, int) {
		if method == "sendRichMessage" {
			var p map[string]any
			json.Unmarshal(body, &p)
			if chatID, _ := p["chat_id"].(float64); int64(chatID) == 100 {
				return map[string]any{"ok": false, "error_code": 500, "description": "Internal Server Error"}, http.StatusInternalServerError
			}
		}
		return map[string]any{"ok": true, "result": map[string]any{"message_id": m.nextMessageID()}}, http.StatusOK
	}
	// 55 строк мёртвому чату (больше одного батча) + одна живому
	// каждый цикл фиксирует и состояние модели (ApplyCycle), чтобы
	// доставщик видел cur=Down
	down := downChecks("gpt-6-astra", 503, "http")
	glmDown := downChecks("glm-5.3", 503, "http")
	for i := 0; i < 55; i++ {
		commitCycle(t, b, cycle(down, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	}
	commitCycle(t, b, cycle(glmDown, transition("glm-5.3", status.Operational, status.Down, time.Now(), nil)))
	seedCheck(t, b, down)
	seedCheck(t, b, glmDown)
	m.resetCalls()

	b.deliveryTick(context.Background())
	sent := 0
	for _, c := range m.sentChatIDs("sendRichMessage") {
		if c == 200 {
			sent++
		}
	}
	if sent != 1 {
		t.Fatalf("live chat must get its notification immediately, got %d", sent)
	}
	if n := m.callCount("sendRichMessage"); n > 2 {
		t.Fatalf("dead chat must not be hammered, sends = %d", n)
	}
	if rows, _ := store.PendingOutbox(100); len(rows) != 55 {
		t.Fatalf("dead chat rows must stay queued: %d", len(rows))
	}
}

// 6.3 (инвариант FIFO в новой семантике): сообщения чата не дробятся по
// строкам — свёртка атомарна; пока голова чата ждёт backoff, новых
// сообщений чату нет; после созревания — одно сообщение с обеими строками.
func TestChatCollapseAtomicAndFIFOAfterBackoff(t *testing.T) {
	m := newMockTG(t)
	models := []storage.Model{{ID: "a", Family: "A"}, {ID: "b", Family: "B"}}
	b, store := newBotAt(t, m, t.TempDir()+"/bot.db", models)
	for _, id := range []string{"a", "b"} {
		if err := store.SetSubscription(100, id, true); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now()
	downA := downChecks("a", 503, "http")
	downB := downChecks("b", 429, "http")
	seedCheck(t, b, downA)
	seedCheck(t, b, downB)
	commitCycle(t, b, cycle(downA, transition("a", status.Operational, status.Down, now.Add(-2*time.Minute), nil)))
	commitCycle(t, b, cycle(downB, transition("b", status.Operational, status.Down, now.Add(-time.Minute), nil)))

	var failFirst atomic.Bool
	failFirst.Store(true)
	m.handler = func(method string, body []byte) (any, int) {
		if method == "sendRichMessage" && failFirst.CompareAndSwap(true, false) {
			return map[string]any{"ok": false, "error_code": 500, "description": "boom"}, http.StatusInternalServerError
		}
		return map[string]any{"ok": true, "result": map[string]any{"message_id": m.nextMessageID()}}, http.StatusOK
	}
	b.deliveryTick(context.Background())
	// попытка была одна, сообщения нет — свёртка чата не дробится по строкам
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("one collapsed attempt expected, got %d", n)
	}
	// пока голова ждёт backoff — никаких сообщений чату
	b.deliveryTick(context.Background())
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("deferred chat must stay silent, got %d sends", n)
	}
	// созревание: обе строки доезжают одним сообщением
	row := firstPendingRow(t, store)
	if err := store.DeferOutboxAttempt(row.ID, -time.Hour); err != nil {
		t.Fatal(err)
	}
	b.deliveryTick(context.Background())
	if n := m.callCount("sendRichMessage"); n != 2 {
		t.Fatalf("collapsed delivery after maturity expected, got %d", n)
	}
	html := lastRichHTML(t, m, "sendRichMessage")
	if strings.Count(html, "<p>") != 2 {
		t.Fatalf("both lines must fit into one message:\n%s", html)
	}
	if !strings.Contains(html, stBtn("a", "HTTP 503")) || !strings.Contains(html, stBtn("b", "HTTP 429")) {
		t.Fatalf("lines a and b expected:\n%s", html)
	}
	if rows, _ := store.PendingOutbox(10); len(rows) != 0 {
		t.Fatalf("all rows must be marked sent: %+v", rows)
	}
}

// P1-3/P1-4: глобальный 429 останавливает весь батч, а не только строку —
// флуд-лимит бота общий, и повтор строки не раньше retry_after.
func Test429PausesWholeBatch(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	if err := store.SetSubscription(200, "glm-5.3", true); err != nil {
		t.Fatal(err)
	}
	gptDown := downChecks("gpt-6-astra", 503, "http")
	glmDown := downChecks("glm-5.3", 503, "http")
	seedCheck(t, b, gptDown)
	seedCheck(t, b, glmDown)
	commitCycle(t, b, cycle(gptDown, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	commitCycle(t, b, cycle(glmDown, transition("glm-5.3", status.Operational, status.Down, time.Now(), nil)))
	var limited atomic.Bool
	limited.Store(true)
	m.handler = func(method string, body []byte) (any, int) {
		if method == "sendRichMessage" && limited.Load() {
			return map[string]any{"ok": false, "error_code": 429, "description": "Too Many Requests",
				"parameters": map[string]any{"retry_after": 0}}, http.StatusTooManyRequests
		}
		return map[string]any{"ok": true, "result": map[string]any{"message_id": m.nextMessageID()}}, http.StatusOK
	}
	b.deliveryTick(context.Background())
	limited.Store(false)
	m.handler = nil
	// чат 1: 3 запроса (in-request ретраи), затем пауза батча; чат 2 не тронут
	if n := m.callCount("sendRichMessage"); n != maxRetries+1 {
		t.Fatalf("only the first chat may be attempted, got %d", n)
	}
	rows, _ := store.PendingOutbox(10)
	if len(rows) != 2 {
		t.Fatalf("both rows must stay queued: %+v", rows)
	}
	if rows[0].NextAttemptAt == nil {
		t.Fatal("429 must defer the failed row (P1-4)")
	}
	// тик во время паузы ничего не отправляет
	b.deliveryTick(context.Background())
	if n := m.callCount("sendRichMessage"); n != maxRetries+1 {
		t.Fatalf("flood pause must hold the batch, got %d sends", n)
	}
	// пауза снята: отложенная строка ещё ждёт retry_after, свежая — уходит
	b.floodUntil.Store(0)
	b.deliveryTick(context.Background())
	if n := m.callCount("sendRichMessage"); n != maxRetries+2 {
		t.Fatalf("fresh row must be delivered while deferred one waits, got %d sends", n)
	}
	if rows, _ = store.PendingOutbox(10); len(rows) != 1 || rows[0].ChatID != 100 {
		t.Fatalf("only the deferred row may remain: %+v", rows)
	}
}

// 6.4: мёртвый чат — очистка памяти: lastText, pinWarn, drafts и привязка
// status_messages, а не только подписки и очередь.
func TestDeadChatCleanupMemory(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	if err := store.SetStatusMessage(100, 777); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	b.lastText[100] = "панель"
	b.pinWarn[100] = true
	b.drafts[100] = map[string]bool{"gpt-6-astra": true}
	b.mu.Unlock()

	m.handler = func(method string, body []byte) (any, int) {
		return map[string]any{"ok": false, "error_code": 403, "description": "Forbidden: bot was blocked by the user"}, http.StatusForbidden
	}
	checks := downChecks("gpt-6-astra", 503, "http")
	seedCheck(t, b, checks)
	commitCycle(t, b, cycle(checks, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	b.deliveryTick(context.Background())

	if sm, err := store.StatusMessage(100); err != nil || sm != nil {
		t.Fatalf("panel binding of the dead chat must be dropped: %v %v", sm, err)
	}
	b.mu.Lock()
	lt, pw := b.lastText[100], b.pinWarn[100]
	dr := b.drafts[100]
	b.mu.Unlock()
	if lt != "" || pw || dr != nil {
		t.Fatalf("in-memory state of the dead chat must be cleaned: lastText=%q pinWarn=%v drafts=%v", lt, pw, dr)
	}
}

// 6.2: write-forbidden (бот заглушён в группе) — не «мёртвый» чат:
// подписки и строка остаются, доставка откладывается backoff'ом.
func TestWriteForbiddenKeepsSubscriptions(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	m.handler = func(method string, body []byte) (any, int) {
		return map[string]any{"ok": false, "error_code": 400, "description": "Bad Request: chat_write_forbidden"}, http.StatusBadRequest
	}
	checks := downChecks("gpt-6-astra", 503, "http")
	seedCheck(t, b, checks)
	commitCycle(t, b, cycle(checks, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	b.deliveryTick(context.Background())
	subs, _ := store.Subscriptions(100)
	if len(subs) != 1 {
		t.Fatalf("write-forbidden is not a confirmed permanent exclusion, subs = %v", subs)
	}
	rows, _ := store.PendingOutbox(10)
	if len(rows) != 1 {
		t.Fatalf("row must stay queued, got %+v", rows)
	}
	if rows[0].NextAttemptAt == nil {
		t.Fatal("row must be deferred by backoff")
	}
}
