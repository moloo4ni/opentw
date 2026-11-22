package telegram

// Regression tests for a lost-notification defect: a down notification was
// lost forever when the model's status became Unknown between the event and
// delivery. Delivery marked the row sent (no message, no chat_notified),
// and the later Unknown→Down with a live incident created no new event.
//
// All scenarios run through the components' real methods: monitor.RunCycle
// with scriptedUpstream (real classification, incidents, ApplyCycle), real
// deliverChat delivery and a real SQLite database. The actual DB state
// (outbox, chat_notified) and the actually sent messages are asserted.

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"opentw/internal/api"
	"opentw/internal/monitor"
	"opentw/internal/status"
	"opentw/internal/storage"
)

// scriptedUpstream is a deterministic upstream provider for a real runner:
// ListModels is always alive (no catalogIssue masking), Generate returns
// the scripted result for the model.
type scriptedUpstream struct {
	mu      sync.Mutex
	results map[string]*api.RawResult
}

func newScriptedUpstream(ids ...string) *scriptedUpstream {
	u := &scriptedUpstream{results: map[string]*api.RawResult{}}
	for _, id := range ids {
		u.setUp(id)
	}
	return u
}

func (u *scriptedUpstream) ListModels(ctx context.Context) ([]string, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make([]string, 0, len(u.results))
	for id := range u.results {
		out = append(out, id)
	}
	return out, nil
}

func (u *scriptedUpstream) Generate(ctx context.Context, modelID string, maxTokens int) *api.RawResult {
	u.mu.Lock()
	defer u.mu.Unlock()
	if r := u.results[modelID]; r != nil {
		return r
	}
	return &api.RawResult{ModelID: modelID, Timestamp: time.Now(), ErrorKind: "invalid", ErrorMessage: "not scripted", Response: "not scripted", LatencyUS: 1_000}
}

func (u *scriptedUpstream) set(id string, r *api.RawResult) {
	u.mu.Lock()
	u.results[id] = r
	u.mu.Unlock()
}

func (u *scriptedUpstream) setUp(id string)   { u.set(id, upstreamUp(id)) }
func (u *scriptedUpstream) setDown(id string) { u.set(id, upstreamDown(id)) }
func (u *scriptedUpstream) setUnknown(id string) {
	// 401 — the monitor's own key problem: Classify yields Unknown,
	// the catalog stays alive, so no catalogIssue masking.
	u.set(id, &api.RawResult{ModelID: id, Timestamp: time.Now(), HTTPStatus: 401,
		ErrorKind: "http", ErrorMessage: "Unauthorized", Response: "Unauthorized", LatencyUS: 20_000})
}

func upstreamUp(id string) *api.RawResult {
	return &api.RawResult{
		ModelID: id, Timestamp: time.Now(), HTTPStatus: 200,
		LatencyUS: 1_000_000, TTFTUS: ptr1(300_000), GenUS: ptr1(700_000),
		TPS: ptrF(40), InputTokens: ptr1(10), OutputTokens: ptr1(8),
		Content: "OK", Response: "OK", Streamed: true,
	}
}

func upstreamDown(id string) *api.RawResult {
	return &api.RawResult{
		ModelID: id, Timestamp: time.Now(), HTTPStatus: 503,
		ErrorKind: "http", ErrorMessage: "Service Unavailable",
		Response: "Service Unavailable", LatencyUS: 40_000,
	}
}

// newOPWBot assembles the production wiring: a real monitoring runner + a
// real delivery bot on one SQLite database (as in cmd/opentw/main.go).
func newOPWBot(t *testing.T, m *mockTG, u *scriptedUpstream, models []storage.Model) (*Bot, *storage.Store, *monitor.Runner) {
	t.Helper()
	store, err := storage.Open(t.TempDir() + "/bot.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	if err := store.SyncModels(models); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := NewBot(BotCfg{
		API:    &Client{Token: "T", BaseURL: m.url, HTTP: &http.Client{Timeout: 5 * time.Second}},
		Store:  store,
		Models: models,
		Logger: log,
	})
	r := monitor.NewRunner(monitor.RunnerCfg{
		API:         u,
		Store:       store,
		Models:      models,
		Concurrency: 2,
		MaxTokens:   8,
		Interval:    15 * time.Minute,
		OnCycle: func(res monitor.CycleResult) ([]storage.OutboxEvent, error) {
			return b.OnCycle(context.Background(), res)
		},
		Logger: log,
	})
	return b, store, r
}

func mustCycle(t *testing.T, r *monitor.Runner) *monitor.CycleResult {
	t.Helper()
	res, err := r.RunCycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func pendingRows(t *testing.T, store *storage.Store) []storage.OutboxRow {
	t.Helper()
	rows, err := store.PendingOutbox(100)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func mustNotified(t *testing.T, store *storage.Store, chatID int64, modelID string, want status.Status) {
	t.Helper()
	st, ok, err := store.ChatNotifiedState(chatID, modelID)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || st != want {
		t.Fatalf("chat_notified(%d, %s) = %q ok=%v, want %q", chatID, modelID, st, ok, want)
	}
}

// seedNotifiedUp brings a chat–model pair to the routine state: last_status=Up,
// chat_notified=Up (via a really delivered recovery notification), empty
// queue, no open incidents.
func seedNotifiedUp(t *testing.T, m *mockTG, u *scriptedUpstream, b *Bot, r *monitor.Runner, chatID int64, modelIDs ...string) {
	t.Helper()
	for _, id := range modelIDs {
		u.setDown(id)
	}
	mustCycle(t, r) // first sighting of Down — the baseline, no event
	for _, id := range modelIDs {
		u.setUp(id)
	}
	mustCycle(t, r) // recovery: an event for every subscribed chat
	b.deliveryTick(context.Background())
	if n := m.callCount("sendRichMessage"); n == 0 {
		t.Fatal("seed: recovery notification must be delivered")
	}
	if rows := pendingRows(t, b.store); len(rows) != 0 {
		t.Fatalf("seed: queue must be empty: %+v", rows)
	}
	for _, id := range modelIDs {
		mustNotified(t, b.store, chatID, id, status.Operational)
	}
	m.resetCalls()
}

// Main scenario: subscription → chat_notified=Up → the monitor records
// Down (an event in the outbox) → delivery is delayed → the next cycle
// yields Unknown → delivery runs (silence) → the cycle yields Down again
// (the live incident suppresses a new event) → delivery runs → the
// subscriber receives the down notification.
// On the original implementation this test fails: the first delivery
// marked the event sent at Unknown, no second event existed — the
// subscriber never learned about the Down.
func TestOPW01UnknownBetweenCyclesKeepsDownNotification(t *testing.T) {
	m := newMockTG(t)
	u := newScriptedUpstream("gpt-6-astra")
	b, store, r := newOPWBot(t, m, u, []storage.Model{{ID: "gpt-6-astra", Family: "GPT"}})
	ctx := context.Background()
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	seedNotifiedUp(t, m, u, b, r, 100, "gpt-6-astra")

	// cycle 1: Up → Down, the event lands in the outbox
	u.setDown("gpt-6-astra")
	mustCycle(t, r)
	rows := pendingRows(t, store)
	if len(rows) != 1 {
		t.Fatalf("down event must be queued exactly once: %+v", rows)
	}
	downRowID := rows[0].ID

	// cycle 2: Unknown (delivery never happened — backlog/unavailability)
	u.setUnknown("gpt-6-astra")
	mustCycle(t, r)
	if rows := pendingRows(t, store); len(rows) != 1 {
		t.Fatalf("unknown cycle must not touch the queue: %+v", rows)
	}

	// delivery at cur=Unknown: silence, but the evidence must survive
	b.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n != 0 {
		t.Fatalf("cur Unknown must stay silent, got %d sends", n)
	}
	rows = pendingRows(t, store)
	if len(rows) != 1 || rows[0].ID != downRowID {
		t.Fatalf("undelivered down evidence must survive the unknown tick: %+v", rows)
	}
	if rows[0].NextAttemptAt == nil {
		t.Fatal("stuck head must be parked with a short retry, not hot-looped")
	}
	if rows[0].Attempts != 0 {
		t.Fatalf("state uncertainty is not a delivery failure: attempts = %d", rows[0].Attempts)
	}
	mustNotified(t, store, 100, "gpt-6-astra", status.Operational)

	// cycle 3: Down again — the live incident suppresses a repeated event
	u.setDown("gpt-6-astra")
	res := mustCycle(t, r)
	if len(res.Transitions) != 0 {
		t.Fatalf("live incident must suppress a duplicate transition: %+v", res.Transitions)
	}

	// delivery: the down notification arrives
	if err := store.DeferOutboxRetry(downRowID, -time.Hour); err != nil {
		t.Fatal(err)
	}
	b.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("subscriber must receive the down notification, got %d sends", n)
	}
	html := lastRichHTML(t, m, "sendRichMessage")
	if !strings.Contains(html, "<p>🔴 <code>gpt-6-astra</code>") {
		t.Fatalf("down line expected:\n%s", html)
	}
	if rows := pendingRows(t, store); len(rows) != 0 {
		t.Fatalf("queue must drain after delivery: %+v", rows)
	}
	mustNotified(t, store, 100, "gpt-6-astra", status.Down)

	// convergence: further Down cycles grow no rows
	mustCycle(t, r)
	mustCycle(t, r)
	if rows := pendingRows(t, store); len(rows) != 0 {
		t.Fatalf("converged pair must not queue rows: %+v", rows)
	}
}

// TTL: the Unknown period outlives the outbox TTL — the original row
// expired and was dropped, but the current down notification still
// arrives: the chat_notified ↔ last_status reconciliation arms itself.
// No garbage accumulates in the queue. The second phase is the same for
// a lost recovery.
func TestOPW01TTLExpiryStillNotifies(t *testing.T) {
	m := newMockTG(t)
	u := newScriptedUpstream("gpt-6-astra")
	b, store, r := newOPWBot(t, m, u, []storage.Model{{ID: "gpt-6-astra", Family: "GPT"}})
	ctx := context.Background()
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	seedNotifiedUp(t, m, u, b, r, 100, "gpt-6-astra")

	// fall → Unknown → a silent delivery (the row is parked) → TTL expired
	u.setDown("gpt-6-astra")
	mustCycle(t, r)
	u.setUnknown("gpt-6-astra")
	mustCycle(t, r)
	b.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n != 0 {
		t.Fatalf("unknown must stay silent, got %d sends", n)
	}
	if _, err := store.DropStaleOutbox(time.Nanosecond); err != nil { // the original row expired
		t.Fatal(err)
	}
	if rows := pendingRows(t, store); len(rows) != 0 {
		t.Fatalf("expired row must be gone: %+v", rows)
	}

	// prolonged Unknown: the reconciliation does not arm (no definitive
	// state), the queue does not grow
	mustCycle(t, r)
	mustCycle(t, r)
	if rows := pendingRows(t, store); len(rows) != 0 {
		t.Fatalf("unknown must not grow the queue: %+v", rows)
	}

	// the model is Down again: the state is recorded, the event is suppressed
	// by the live incident — but the reconciliation row arms on the next cycle
	u.setDown("gpt-6-astra")
	mustCycle(t, r)
	if rows := pendingRows(t, store); len(rows) != 0 {
		t.Fatalf("fresh divergence arms one cycle later: %+v", rows)
	}
	mustCycle(t, r)
	rows := pendingRows(t, store)
	if len(rows) != 1 || rows[0].Kind != "reconcile" {
		t.Fatalf("exactly one reconcile row expected: %+v", rows)
	}

	// delivery: the user still receives the current notification
	b.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("down must reach the subscriber after TTL expiry, got %d sends", n)
	}
	if html := lastRichHTML(t, m, "sendRichMessage"); !strings.Contains(html, "<p>🔴 <code>gpt-6-astra</code>") {
		t.Fatalf("down line expected:\n%s", html)
	}
	mustNotified(t, store, 100, "gpt-6-astra", status.Down)

	// convergence: the reconciliation grows no rows while the state matches
	mustCycle(t, r)
	mustCycle(t, r)
	if rows := pendingRows(t, store); len(rows) != 0 {
		t.Fatalf("no garbage after convergence: %+v", rows)
	}

	// phase 2: a lost RECOVERY also self-heals
	u.setUnknown("gpt-6-astra")
	mustCycle(t, r)
	u.setUp("gpt-6-astra")
	mustCycle(t, r)                                                   // Unknown→Up: the recovery event was created…
	if _, err := store.DropStaleOutbox(time.Nanosecond); err != nil { // …and expired undelivered
		t.Fatal(err)
	}
	mustCycle(t, r) // reconciliation: last=Up ≠ notified=Down
	rows = pendingRows(t, store)
	if len(rows) != 1 || rows[0].Kind != "reconcile" {
		t.Fatalf("recovery reconcile row expected: %+v", rows)
	}
	b.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n != 2 {
		t.Fatalf("recovery must reach the subscriber after TTL expiry, got %d sends", n)
	}
	if html := lastRichHTML(t, m, "sendRichMessage"); !strings.Contains(html, "<p>🟢 <code>gpt-6-astra</code> - Восстановилась</p>") {
		t.Fatalf("recovery line expected:\n%s", html)
	}
	mustNotified(t, store, 100, "gpt-6-astra", status.Operational)
}

// FIFO and model isolation: A=Unknown with a pending event, B=Down — both
// in one subscribed chat. B is delivered on time, A's evidence survives
// and arrives when A gets a definitive status.
func TestOPW01UnknownModelDoesNotBlockChatPeer(t *testing.T) {
	m := newMockTG(t)
	u := newScriptedUpstream("a", "b")
	b, store, r := newOPWBot(t, m, u, []storage.Model{{ID: "a", Family: "A"}, {ID: "b", Family: "B"}})
	ctx := context.Background()
	for _, id := range []string{"a", "b"} {
		if err := store.SetSubscription(100, id, true); err != nil {
			t.Fatal(err)
		}
	}
	seedNotifiedUp(t, m, u, b, r, 100, "a", "b")

	// A falls, then B falls (different cycles — different rows)
	u.setDown("a")
	mustCycle(t, r)
	u.setDown("b")
	mustCycle(t, r)
	// A loses definitiveness
	u.setUnknown("a")
	mustCycle(t, r)
	if rows := pendingRows(t, store); len(rows) != 2 { // A's event, B's event; the reconciliation does not duplicate live evidence
		t.Fatalf("queue = %+v", rows)
	}

	// delivery: B leaves, A stays
	b.deliveryTick(ctx)
	bodies := m.callBodies("sendRichMessage")
	if len(bodies) != 1 {
		t.Fatalf("B must be delivered on time, got %d sends", len(bodies))
	}
	if html := richHTML(t, parseBody(t, bodies[0])); !strings.Contains(html, "<code>b</code>") || strings.Contains(html, "<code>a</code>") {
		t.Fatalf("first message must carry only B's line:\n%s", html)
	}
	rows := pendingRows(t, store)
	if len(rows) != 1 || rows[0].Attempts != 0 || rows[0].NextAttemptAt == nil {
		t.Fatalf("A's evidence must stay pending, parked, not failed: %+v", rows)
	}
	mustNotified(t, store, 100, "a", status.Operational) // A untouched
	mustNotified(t, store, 100, "b", status.Down)        // B delivered

	// A gets a definitive Down — its notification arrives
	u.setDown("a")
	mustCycle(t, r)
	if rows := pendingRows(t, store); len(rows) != 1 {
		t.Fatalf("converged B and suppressed A must add nothing: %+v", rows)
	}
	if err := store.DeferOutboxRetry(rows[0].ID, -time.Hour); err != nil {
		t.Fatal(err)
	}
	b.deliveryTick(ctx)
	bodies = m.callBodies("sendRichMessage")
	if len(bodies) != 2 {
		t.Fatalf("A must be delivered after certainty returns, got %d sends", len(bodies))
	}
	if html := richHTML(t, parseBody(t, bodies[1])); !strings.Contains(html, "<code>a</code>") || strings.Contains(html, "<code>b</code>") {
		t.Fatalf("second message must carry only A's line:\n%s", html)
	}
	if rows := pendingRows(t, store); len(rows) != 0 {
		t.Fatalf("queue must drain: %+v", rows)
	}
	mustNotified(t, store, 100, "a", status.Down)
	mustNotified(t, store, 100, "b", status.Down)
}

// Freshness collapse: Up→Down→Unknown→Up with notified=Up — no stale
// down notification arrives.
func TestOPW01DownUnknownUpSendsNoStaleHistory(t *testing.T) {
	m := newMockTG(t)
	u := newScriptedUpstream("gpt-6-astra")
	b, store, r := newOPWBot(t, m, u, []storage.Model{{ID: "gpt-6-astra", Family: "GPT"}})
	ctx := context.Background()
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	seedNotifiedUp(t, m, u, b, r, 100, "gpt-6-astra")

	u.setDown("gpt-6-astra")
	mustCycle(t, r)
	u.setUnknown("gpt-6-astra")
	mustCycle(t, r)
	u.setUp("gpt-6-astra")
	mustCycle(t, r) // Down→Unknown→Up: the fall and recovery both passed

	b.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n != 0 {
		t.Fatalf("cur == notified — no stale history is sent, got %d sends", n)
	}
	if rows := pendingRows(t, store); len(rows) != 0 {
		t.Fatalf("rows must resolve without a message: %+v", rows)
	}
	mustNotified(t, store, 100, "gpt-6-astra", status.Operational)
}

// Sequences of several cycles.
func TestOPW01SequenceDownUnknownUnknownDown(t *testing.T) {
	m := newMockTG(t)
	u := newScriptedUpstream("gpt-6-astra")
	b, store, r := newOPWBot(t, m, u, []storage.Model{{ID: "gpt-6-astra", Family: "GPT"}})
	ctx := context.Background()
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	seedNotifiedUp(t, m, u, b, r, 100, "gpt-6-astra")

	u.setDown("gpt-6-astra")
	mustCycle(t, r)
	u.setUnknown("gpt-6-astra")
	mustCycle(t, r)
	mustCycle(t, r)
	u.setDown("gpt-6-astra")
	mustCycle(t, r)

	b.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("one actual down line expected, got %d sends", n)
	}
	if html := lastRichHTML(t, m, "sendRichMessage"); strings.Count(html, "<p>") != 1 || !strings.Contains(html, "<p>🔴 <code>gpt-6-astra</code>") {
		t.Fatalf("exactly one down line expected:\n%s", html)
	}
	if rows := pendingRows(t, store); len(rows) != 0 {
		t.Fatalf("queue must drain: %+v", rows)
	}
	mustNotified(t, store, 100, "gpt-6-astra", status.Down)
}

func TestOPW01SequenceDownUnknownUpDown(t *testing.T) {
	m := newMockTG(t)
	u := newScriptedUpstream("gpt-6-astra")
	b, store, r := newOPWBot(t, m, u, []storage.Model{{ID: "gpt-6-astra", Family: "GPT"}})
	ctx := context.Background()
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	seedNotifiedUp(t, m, u, b, r, 100, "gpt-6-astra")

	for _, st := range []func(string){u.setDown, u.setUnknown, u.setUp, u.setDown} {
		st("gpt-6-astra")
		mustCycle(t, r)
	}

	b.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("final state must collapse into one line, got %d sends", n)
	}
	if html := lastRichHTML(t, m, "sendRichMessage"); strings.Count(html, "<p>") != 1 || !strings.Contains(html, "<p>🔴 <code>gpt-6-astra</code>") {
		t.Fatalf("exactly one down line expected:\n%s", html)
	}
	if rows := pendingRows(t, store); len(rows) != 0 {
		t.Fatalf("queue must drain: %+v", rows)
	}
	mustNotified(t, store, 100, "gpt-6-astra", status.Down)
}

func TestOPW01SequenceDownUnknownUp(t *testing.T) {
	m := newMockTG(t)
	u := newScriptedUpstream("gpt-6-astra")
	b, store, r := newOPWBot(t, m, u, []storage.Model{{ID: "gpt-6-astra", Family: "GPT"}})
	ctx := context.Background()
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	seedNotifiedUp(t, m, u, b, r, 100, "gpt-6-astra")

	u.setDown("gpt-6-astra")
	mustCycle(t, r)
	u.setUnknown("gpt-6-astra")
	mustCycle(t, r)
	u.setUp("gpt-6-astra")
	mustCycle(t, r)

	b.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n != 0 {
		t.Fatalf("model fell and recovered back to the notified state — silence, got %d sends", n)
	}
	if rows := pendingRows(t, store); len(rows) != 0 {
		t.Fatalf("queue must drain: %+v", rows)
	}
	mustNotified(t, store, 100, "gpt-6-astra", status.Operational)
}

// After a successfully delivered Down, a later Unknown→Up produces a
// recovery notification (the "eventually arrives" invariant).
func TestOPW01RecoveryAfterDeliveredDownStillArrives(t *testing.T) {
	m := newMockTG(t)
	u := newScriptedUpstream("gpt-6-astra")
	b, store, r := newOPWBot(t, m, u, []storage.Model{{ID: "gpt-6-astra", Family: "GPT"}})
	ctx := context.Background()
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	seedNotifiedUp(t, m, u, b, r, 100, "gpt-6-astra")

	u.setDown("gpt-6-astra")
	mustCycle(t, r)
	b.deliveryTick(ctx) // the down was delivered
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("down must be delivered, got %d sends", n)
	}
	mustNotified(t, store, 100, "gpt-6-astra", status.Down)
	m.resetCalls()

	u.setUnknown("gpt-6-astra")
	mustCycle(t, r)
	u.setUp("gpt-6-astra")
	mustCycle(t, r)
	b.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("recovery must arrive, got %d sends", n)
	}
	if html := lastRichHTML(t, m, "sendRichMessage"); !strings.Contains(html, "<p>🟢 <code>gpt-6-astra</code> - Восстановилась</p>") {
		t.Fatalf("recovery line expected:\n%s", html)
	}
	mustNotified(t, store, 100, "gpt-6-astra", status.Operational)
}

// Re-delivery: Telegram confirmed the send, but the SQLite commit failed —
// the evidence and chat_notified are not "fictitiously delivered"; the
// retried delivery arrives as a duplicate (the accepted at-least-once
// trade-off) and records the correct state.
func TestOPW01CommitFailureKeepsEvidenceAndRedelivers(t *testing.T) {
	m := newMockTG(t)
	path := t.TempDir() + "/bot.db"
	models := []storage.Model{{ID: "gpt-6-astra", Family: "GPT"}}
	b, store := newBotAt(t, m, path, models)
	ctx := context.Background()
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	if err := store.SetChatNotifiedState(100, "gpt-6-astra", status.Operational); err != nil {
		t.Fatal(err)
	}
	// fall → event; Unknown: the row survives a silent delivery
	checks := downChecks("gpt-6-astra", 503, "http")
	seedCheck(t, b, checks)
	commitCycle(t, b, cycle(checks, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	if err := store.SetModelState("gpt-6-astra", status.Unknown, "invalid", "unparseable", time.Now()); err != nil {
		t.Fatal(err)
	}
	b.deliveryTick(ctx)
	if rows := pendingRows(t, store); len(rows) != 1 {
		t.Fatalf("evidence must survive the unknown tick: %+v", rows)
	}
	// the evidence expired (TTL) → the reconciliation row arms
	if err := store.SetModelState("gpt-6-astra", status.Down, "http", "HTTP 503", time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DropStaleOutbox(time.Nanosecond); err != nil {
		t.Fatal(err)
	}
	commitCycle(t, b, cycle(downChecks("gpt-6-astra", 503, "http")))
	rows := pendingRows(t, store)
	if len(rows) != 1 || rows[0].Kind != "reconcile" {
		t.Fatalf("reconcile row must replace the expired evidence: %+v", rows)
	}

	// the send succeeds, the commit does not: SQLite is closed mid-send
	m.handler = func(method string, body []byte) (any, int) {
		if method == "sendRichMessage" {
			_ = store.Close()
			return map[string]any{"ok": true, "result": map[string]any{"message_id": m.nextMessageID()}}, http.StatusOK
		}
		return map[string]any{"ok": true, "result": true}, http.StatusOK
	}
	b.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("one send must happen, got %d", n)
	}

	// restart on the same file: nothing is recorded — neither marks,
	// nor the notified state
	b2, store2 := newBotAt(t, m, path, models)
	if rows := pendingRows(t, store2); len(rows) != 1 {
		t.Fatalf("failed commit must leave the row pending: %+v", rows)
	}
	mustNotified(t, store2, 100, "gpt-6-astra", status.Operational)

	// the retried delivery: a duplicate (at-least-once) and a correct commit
	m.handler = nil
	b2.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n < 2 {
		t.Fatalf("at-least-once: a duplicate send is expected, got %d", n)
	}
	if rows := pendingRows(t, store2); len(rows) != 0 {
		t.Fatalf("rows must drain on retry: %+v", rows)
	}
	mustNotified(t, store2, 100, "gpt-6-astra", status.Down)
}

// Several chats: a subscribed and an unsubscribed chat, a successful
// delivery and a failure in another — chat states are independent.
func TestOPW01MultiChatIsolation(t *testing.T) {
	m := newMockTG(t)
	u := newScriptedUpstream("gpt-6-astra")
	b, store, r := newOPWBot(t, m, u, []storage.Model{{ID: "gpt-6-astra", Family: "GPT"}})
	ctx := context.Background()
	for _, chat := range []int64{100, 200} {
		if err := store.SetSubscription(chat, "gpt-6-astra", true); err != nil {
			t.Fatal(err)
		}
	}
	seedNotifiedUp(t, m, u, b, r, 100, "gpt-6-astra")
	mustNotified(t, store, 200, "gpt-6-astra", status.Operational)
	m.resetCalls()

	// fall: rows only for subscribed chats
	u.setDown("gpt-6-astra")
	mustCycle(t, r)
	rows := pendingRows(t, store)
	if len(rows) != 2 {
		t.Fatalf("two subscribed chats must hold rows: %+v", rows)
	}
	for _, row := range rows {
		if row.ChatID != 100 && row.ChatID != 200 {
			t.Fatalf("unsubscribed chat must not receive rows: %+v", row)
		}
	}

	// chat 200 errors: chat 100 is delivered independently of processing order
	fail200 := true
	m.handler = func(method string, body []byte) (any, int) {
		if method == "sendRichMessage" && fail200 {
			var p map[string]any
			if err := json.Unmarshal(body, &p); err == nil {
				if chatID, _ := p["chat_id"].(float64); int64(chatID) == 200 {
					return map[string]any{"ok": false, "error_code": 500, "description": "Internal Server Error"}, http.StatusInternalServerError
				}
			}
		}
		return map[string]any{"ok": true, "result": map[string]any{"message_id": m.nextMessageID()}}, http.StatusOK
	}
	b.deliveryTick(ctx)
	mustNotified(t, store, 100, "gpt-6-astra", status.Down) // one chat's success
	rows = pendingRows(t, store)
	if len(rows) != 1 || rows[0].ChatID != 200 {
		t.Fatalf("only the failed chat's row must stay: %+v", rows)
	}
	mustNotified(t, store, 200, "gpt-6-astra", status.Operational) // the failure does not move chat_notified
	c100, c200 := 0, 0
	for _, c := range m.sentChatIDs("sendRichMessage") {
		switch c {
		case 100:
			c100++
		case 200:
			c200++
		}
	}
	if c100 != 1 {
		t.Fatalf("chat 100 must be delivered exactly once, got %d", c100)
	}
	if c200 < 1 {
		t.Fatalf("chat 200 must be attempted, got %d", c200)
	}

	// chat 200 revives: its notification arrives, chat 100 is not touched
	fail200 = false
	m.handler = nil
	m.resetCalls()
	if err := store.DeferOutboxAttempt(rows[0].ID, -time.Hour); err != nil {
		t.Fatal(err)
	}
	b.deliveryTick(ctx)
	chats := m.sentChatIDs("sendRichMessage")
	if len(chats) != 1 || chats[0] != 200 {
		t.Fatalf("only chat 200 must receive the retry, got %v", chats)
	}
	mustNotified(t, store, 200, "gpt-6-astra", status.Down)
	if rows := pendingRows(t, store); len(rows) != 0 {
		t.Fatalf("queue must drain: %+v", rows)
	}
}

// parseBody parses a call body as an object (for one-line reads).
func parseBody(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var p map[string]any
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatal(err)
	}
	return p
}

// A grouped row with TWO models, one of which is Unknown. The row is
// resolved only as a whole (a partial sent mark is impossible), but the
// collapse does not hold back the definitive model: the definitive
// models' lines leave at once (chat_notified records only what was really
// sent), while the row stays pending until all its models are definitive.
func TestOPW01MixedRowWaitsForAllItsModels(t *testing.T) {
	m := newMockTG(t)
	u := newScriptedUpstream("a", "b")
	b, store, r := newOPWBot(t, m, u, []storage.Model{{ID: "a", Family: "A"}, {ID: "b", Family: "B"}})
	ctx := context.Background()
	for _, id := range []string{"a", "b"} {
		if err := store.SetSubscription(100, id, true); err != nil {
			t.Fatal(err)
		}
	}
	seedNotifiedUp(t, m, u, b, r, 100, "a", "b")

	// BOTH models fall in one cycle: one row with two transitions
	u.setDown("a")
	u.setDown("b")
	mustCycle(t, r)
	rows := pendingRows(t, store)
	if len(rows) != 1 {
		t.Fatalf("one grouped row expected: %+v", rows)
	}
	// a loses definitiveness
	u.setUnknown("a")
	mustCycle(t, r)

	// delivery: b's line leaves (definitive), the row itself is NOT resolved —
	// the evidence about a stays alive
	b.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("b's line must be delivered on time, got %d sends", n)
	}
	if html := lastRichHTML(t, m, "sendRichMessage"); strings.Count(html, "<p>") != 1 || !strings.Contains(html, "<code>b</code>") || strings.Contains(html, "<code>a</code>") {
		t.Fatalf("only b's line expected:\n%s", html)
	}
	rows = pendingRows(t, store)
	if len(rows) != 1 || rows[0].Attempts != 0 || rows[0].NextAttemptAt == nil {
		t.Fatalf("mixed row must stay pending, parked: %+v", rows)
	}
	mustNotified(t, store, 100, "a", status.Operational) // a untouched
	mustNotified(t, store, 100, "b", status.Down)        // b really delivered

	// a repeated tick at the same state: no duplicates, no spam
	b.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("no duplicate for the parked row, got %d sends", n)
	}

	// both models definitive: a's line leaves, the row resolves as a whole
	u.setDown("a")
	mustCycle(t, r)
	if rows := pendingRows(t, store); len(rows) != 1 {
		t.Fatalf("suppressed transition must not add rows: %+v", rows)
	}
	if err := store.DeferOutboxRetry(rows[0].ID, -time.Hour); err != nil {
		t.Fatal(err)
	}
	b.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n != 2 {
		t.Fatalf("one collapsed message expected, got %d sends", n)
	}
	html := lastRichHTML(t, m, "sendRichMessage")
	if strings.Count(html, "<p>") != 1 || !strings.Contains(html, "<code>a</code>") || strings.Contains(html, "<code>b</code>") {
		t.Fatalf("only a's line expected (b already notified):\n%s", html)
	}
	if rows := pendingRows(t, store); len(rows) != 0 {
		t.Fatalf("queue must drain: %+v", rows)
	}
	mustNotified(t, store, 100, "a", status.Down)
	mustNotified(t, store, 100, "b", status.Down)
}

// Reconciliation re-arms in a NEW process: the old process's last commit
// recorded last=Down with notified=Up, the original row expired by TTL,
// and the new process's first cycle arms the reconciliation.
func TestOPW01ReconcileArmsAfterRestart(t *testing.T) {
	m := newMockTG(t)
	path := t.TempDir() + "/bot.db"
	models := []storage.Model{{ID: "gpt-6-astra", Family: "GPT"}}
	b, store := newBotAt(t, m, path, models)
	ctx := context.Background()
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	if err := store.SetChatNotifiedState(100, "gpt-6-astra", status.Operational); err != nil {
		t.Fatal(err)
	}
	// the old process: the fall is recorded, the event row expired by TTL
	checks := downChecks("gpt-6-astra", 503, "http")
	seedCheck(t, b, checks)
	commitCycle(t, b, cycle(checks, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	if _, err := store.DropStaleOutbox(time.Nanosecond); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// the new process: the first cycle arms the reconciliation (the event is
	// suppressed by the incident)
	b2, store2 := newBotAt(t, m, path, models)
	commitCycle(t, b2, cycle(downChecks("gpt-6-astra", 503, "http")))
	rows := pendingRows(t, store2)
	if len(rows) != 1 || rows[0].Kind != "reconcile" {
		t.Fatalf("reconcile must arm in the new process: %+v", rows)
	}
	b2.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("down must reach the subscriber after restart, got %d sends", n)
	}
	if html := lastRichHTML(t, m, "sendRichMessage"); !strings.Contains(html, "<p>🔴 <code>gpt-6-astra</code>") {
		t.Fatalf("down line expected:\n%s", html)
	}
	if rows := pendingRows(t, store2); len(rows) != 0 {
		t.Fatalf("queue must drain: %+v", rows)
	}
	mustNotified(t, store2, 100, "gpt-6-astra", status.Down)
}
