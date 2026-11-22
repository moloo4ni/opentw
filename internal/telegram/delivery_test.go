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

// cycleAt is a cycle with the given time (unique row dedupe keys).
func cycleAt(at time.Time, trs ...monitor.Transition) monitor.CycleResult {
	return monitor.CycleResult{CycleAt: at, NextAt: at.Add(15 * time.Minute), Transitions: trs}
}

// firstPendingRow returns the oldest pending row (for manipulation).
func firstPendingRow(t *testing.T, store *storage.Store) storage.OutboxRow {
	t.Helper()
	rows, err := store.PendingOutbox(10)
	if err != nil || len(rows) == 0 {
		t.Fatalf("pending row expected: %v %v", rows, err)
	}
	return rows[0]
}

// enqueuePending creates a synthetic pending row with a model event (for
// directly testing the freshness reconciliation).
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

// OnCycle does not touch the network: the event lands in the outbox.
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

// Many models in one cycle — one rich message per chat, lines only for its
// subscriptions, config order.
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
		transition("c", status.Operational, status.Down, now, nil))) // not subscribed
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
	// line order follows the config order: a before b
	if !(strings.Index(html, "<code>a</code>") < strings.Index(html, "<code>b</code>")) {
		t.Errorf("config order broken:\n%s", html)
	}
}

// Scenario A: a fall and recovery during a delivery downtime with
// notified=operational — silence, rows are marked sent.
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
		t.Fatalf("cur == notified — silence, got %d sends", n)
	}
	if rows, _ := store.PendingOutbox(10); len(rows) != 0 {
		t.Fatalf("rows must be marked sent: %+v", rows)
	}
	// the notified state did not change
	st, ok, _ := store.ChatNotifiedState(100, "gpt-6-astra")
	if !ok || st != status.Operational {
		t.Fatalf("chat_notified must stay operational, got %q ok=%v", st, ok)
	}
}

// Scenario B: the outage persists — one red line; a repeated row at
// cur == notified stays silent.
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

	// the user already knows: a new pending row of the same model stays silent
	enqueuePending(t, store, 100, "gpt-6-astra")
	b.deliveryTick(context.Background())
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("cur == notified must stay silent, got %d sends", n)
	}
	if rows, _ := store.PendingOutbox(10); len(rows) != 0 {
		t.Fatalf("known row must be marked sent: %+v", rows)
	}
}

// Scenario C: several flips — ONE line for the CURRENT state, the label
// from the LATEST check (not the historical one in the payload).
func TestDeliveryScenarioCOneLineFromCurrentState(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	// the historical check is 503, the latest is 429: the line must show 429
	if err := b.store.InsertCheck(storage.Check{ModelID: "gpt-6-astra", At: now.Add(-30 * time.Minute),
		Status: status.Down, HTTPStatus: 503, ErrorType: "http"}); err != nil {
		t.Fatal(err)
	}
	fresh := storage.Check{ModelID: "gpt-6-astra", At: now, Status: status.Down, HTTPStatus: 429, ErrorType: "http"}
	if err := b.store.InsertCheck(fresh); err != nil {
		t.Fatal(err)
	}
	// the historical payload carries 503 — the line is built from the current check
	tr := transition("gpt-6-astra", status.Operational, status.Down, now.Add(-30*time.Minute), nil)
	commitCycle(t, b, cycleAt(now.Add(-30*time.Minute), tr))
	commitCycle(t, b, cycleAt(now.Add(-15*time.Minute),
		transition("gpt-6-astra", status.Down, status.Operational, now.Add(-15*time.Minute), nil)))
	// the final state is down again (the 429 check)
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

// Scenario D: the current state is Unknown — silence without destroying the
// evidence: rows are NOT marked sent, chat_notified is NOT updated; a
// returning definitive state delivers the notification.
func TestDeliveryScenarioDUnknownSilent(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	checks := downChecks("gpt-6-astra", 503, "http")
	commitCycle(t, b, cycle(checks, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	// by delivery time the monitor lost sight: last_status = Unknown
	if err := store.SetModelState("gpt-6-astra", status.Unknown, "invalid", "unparseable", time.Now()); err != nil {
		t.Fatal(err)
	}
	b.deliveryTick(context.Background())
	if n := m.callCount("sendRichMessage"); n != 0 {
		t.Fatalf("cur Unknown — silence, got %d sends", n)
	}
	// uncertainty must not swallow the only evidence of an undelivered fall:
	// the row must stay pending
	if rows, _ := store.PendingOutbox(10); len(rows) != 1 {
		t.Fatalf("row must stay pending while cur is Unknown: %+v", rows)
	}
	if rows, _ := store.PendingOutbox(10); rows[0].NextAttemptAt == nil {
		t.Fatal("stuck head must be parked with a short retry, not hot-loop every tick")
	}
	if _, ok, _ := store.ChatNotifiedState(100, "gpt-6-astra"); ok {
		t.Fatal("Unknown must not update chat_notified")
	}
	// the definitive state returned — the down notification arrives
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

// Unsubscribe during downtime — the row is dropped without sending.
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

// A repeated OnCycle with the same cycle time does not duplicate the row
// (idempotence).
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

// An event survives a process restart: the same DB file, a new Bot.
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

// A new chat event committed DURING the send of its backlog collapse is not
// marked sent with the backlog and is delivered as a separate message.
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
			// the first send is in flight: the cycle commits a new event of the same chat
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
	// the first message — only the old event
	if !strings.Contains(sent[0], "<p>🔴 <code>gpt-6-astra</code>") || strings.Contains(sent[0], "glm-5.3") {
		t.Fatalf("first message must hold only the old backlog:\n%s", sent[0])
	}
	// the second — a separate notification of the new event
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

// "send done — report lost" (MarkOutboxSent failed) — the queue does not
// freeze: the retry arrives as a duplicate (at-least-once), the DB is intact.
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
				_ = store.Close() // the delivery report is lost right after the send
			}
			return map[string]any{"ok": true, "result": map[string]any{"message_id": m.nextMessageID()}}, http.StatusOK
		}
		return map[string]any{"ok": true, "result": true}, http.StatusOK
	}
	checks := downChecks("gpt-6-astra", 503, "http")
	seedCheck(t, b, checks)
	commitCycle(t, b, cycle(checks, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	b.deliveryTick(context.Background())

	// the same DB file, a new process: the row is still pending and redelivers
	b2, store2 := newBotAt(t, m, path, models)
	if rows, _ := store2.PendingOutbox(10); len(rows) != 1 {
		t.Fatalf("undelivered row must survive a lost report: %+v", rows)
	}
	// the post-send commit failed — nothing is recorded: neither row marks,
	// nor the notified state (one transaction)
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

// A temporary Telegram failure: the row stays and is not retried before
// the backoff.
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
	// no repeat until the backoff elapses
	fail.Store(false)
	m.handler = nil
	b.deliveryTick(context.Background())
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("row must not be retried before the backoff elapses, sends = %d", n)
	}
	// the deadline arrived — the row delivers
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

// The 429 exhausted its retries — the event is not lost, it delivers after
// things normalize.
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
	// a repeat not before retry_after (1s floor) and after the batch pause lifts
	time.Sleep(time.Until(*rows[0].NextAttemptAt) + 20*time.Millisecond)
	b.floodUntil.Store(0)
	b.deliveryTick(context.Background())
	rows, _ = store.PendingOutbox(10)
	if len(rows) != 0 {
		t.Fatalf("row must be delivered after rate limit lifted: %+v", rows)
	}
}

// A shutdown during a delivery burst: undelivered work stays in the DB.
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

// Panels are refreshed by the worker from LastChecks: an incomplete cycle
// does not erase a known state.
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
	// a "cycle" where glm-5.3's check was cancelled: it is absent from res.Checks
	commitCycle(t, b, cycle(map[string]storage.Check{
		"gpt-6-astra": {ModelID: "gpt-6-astra", At: at, Status: status.Operational, HTTPStatus: 200, LatencyUS: ptr1(2_000_000)},
	}))
	b.deliveryTick(ctx)
	if n := m.callCount("editMessageText"); n != 1 {
		t.Fatalf("panel edit expected, got %d", n)
	}
	html := lastRichHTML(t, m, "editMessageText")
	if !strings.Contains(html, "<p><b>GLM</b><br/>🟢 <code>glm-5.3</code> - 2.0s</p>") {
		t.Fatalf("known state must survive a partial cycle:\n%s", html)
	}
	if strings.Contains(html, "Нет данных</tg-button>") {
		t.Fatalf("no-data must not replace a known state:\n%s", html)
	}
}

// The canonical growing-backoff schedule: 30s, 1m, 2m, 4m, 8m, capped at
// 15m (and stays capped).
func TestDeferBackoffSchedule(t *testing.T) {
	want := []time.Duration{30 * time.Second, 60 * time.Second, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 15 * time.Minute, 15 * time.Minute}
	for i, w := range want {
		if got := deferBackoff(i); got != w {
			t.Errorf("deferBackoff(%d) = %v, want %v", i, got, w)
		}
	}
}

// A temporary delivery error → a growing backoff; the row is neither
// deleted nor retried early.
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

	// the first failure: a ~30s pause
	b.deliveryTick(context.Background())
	row := firstPendingRow(t, store)
	if row.NextAttemptAt == nil {
		t.Fatal("temporary failure must defer the row (backoff)")
	}
	d := time.Until(*row.NextAttemptAt)
	if d < 25*time.Second || d > 35*time.Second {
		t.Fatalf("first backoff = %v, want ~30s", d)
	}
	// the row is not retried before the backoff elapses
	b.deliveryTick(context.Background())
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("row must not be retried before the backoff elapses, sends = %d", n)
	}
	// "30 seconds passed": the second failure — a pause strictly longer than the first
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
	// the row survived and delivered: the backoff does not delete events
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

// Unavailable chats do not take batch slots — a live chat receives its
// notification immediately, no matter how many dead chats queue ahead.
func TestUnavailableChatsDoNotStarveLiveOnes(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil { // the "dead" chat
		t.Fatal(err)
	}
	if err := store.SetSubscription(200, "glm-5.3", true); err != nil { // the live chat
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
	// 55 rows to the dead chat (more than one batch) + one to the live one;
	// each cycle also commits the model state (ApplyCycle), so the delivery
	// worker sees cur=Down
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

// FIFO invariant under the new semantics: a chat's messages are not split
// per row — the collapse is atomic; while the head waits out a backoff the
// chat gets no new messages; after maturity both rows arrive in one message.
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
	// one attempt, no message — the chat's collapse is not split per row
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("one collapsed attempt expected, got %d", n)
	}
	// while the head waits out the backoff — no messages to the chat
	b.deliveryTick(context.Background())
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("deferred chat must stay silent, got %d sends", n)
	}
	// maturity: both rows arrive in one message
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

// A global 429 stops the whole batch, not only the row — the bot's flood
// limit is shared, and the retry is not earlier than retry_after.
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
	// chat 1: 3 requests (in-request retries), then the batch pauses; chat 2 untouched
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
	// a tick during the pause sends nothing
	b.deliveryTick(context.Background())
	if n := m.callCount("sendRichMessage"); n != maxRetries+1 {
		t.Fatalf("flood pause must hold the batch, got %d sends", n)
	}
	// the pause lifted: the deferred row still waits out retry_after, the fresh one leaves
	b.floodUntil.Store(0)
	b.deliveryTick(context.Background())
	if n := m.callCount("sendRichMessage"); n != maxRetries+2 {
		t.Fatalf("fresh row must be delivered while deferred one waits, got %d sends", n)
	}
	if rows, _ = store.PendingOutbox(10); len(rows) != 1 || rows[0].ChatID != 100 {
		t.Fatalf("only the deferred row may remain: %+v", rows)
	}
}

// A dead chat — in-memory cleanup: lastText, pinWarn, drafts and the
// status_messages binding, not only subscriptions and the queue.
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

// write-forbidden (the bot is muted in the group) is NOT a dead chat:
// subscriptions and the row stay, delivery is postponed by the backoff.
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
