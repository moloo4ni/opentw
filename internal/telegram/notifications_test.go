package telegram

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/moloo4ni/opentw/internal/monitor"
	"github.com/moloo4ni/opentw/internal/status"
	"github.com/moloo4ni/opentw/internal/storage"
)

func transition(id string, from, to status.Status, at time.Time, incident *storage.Incident) monitor.Transition {
	c := storage.Check{ModelID: id, At: at, Status: to}
	switch to {
	case status.Operational:
		c.HTTPStatus = 200
		c.LatencyUS = ptr1(2_700_000)
	case status.Down:
		c.HTTPStatus = 503
		c.ErrorType = "http"
		c.ErrorMessage = "Service Unavailable"
	default:
		c.ErrorType = "invalid"
		c.ErrorMessage = "monitor error"
	}
	return monitor.Transition{ModelID: id, From: from, To: to, Check: c, Incident: incident}
}

// downChecks is the DB state after a model fall with the given error.
func downChecks(id string, httpStatus int, errType string) map[string]storage.Check {
	c := storage.Check{ModelID: id, At: time.Now(), Status: status.Down, HTTPStatus: httpStatus, ErrorType: errType}
	return map[string]storage.Check{id: c}
}

// Only a chat subscribed to the model receives the notification.
func TestNotifyOnlySubscribedChats(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	// chat 100 subscribes to gpt-6-astra, chat 200 — to glm-5.3
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	if err := store.SetSubscription(200, "glm-5.3", true); err != nil {
		t.Fatal(err)
	}
	checks := downChecks("gpt-6-astra", 503, "http")
	seedCheck(t, b, checks)
	commitCycle(t, b, cycle(checks, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	b.deliveryTick(context.Background())

	if m.callCount("sendRichMessage") != 1 {
		t.Fatalf("exactly one rich notification expected, got %d", m.callCount("sendRichMessage"))
	}
	body := m.lastBody("sendRichMessage")
	if body["chat_id"].(float64) != 100 {
		t.Fatalf("notification must go to chat 100 only, got %v", body["chat_id"])
	}
	html := richHTML(t, body)
	if !strings.Contains(html, "<p>🔴 <code>gpt-6-astra</code> - "+stBtn("gpt-6-astra", "HTTP 503")+"</p>") {
		t.Fatalf("notification line wrong:\n%s", html)
	}
}

// An unsubscribed chat receives no notifications.
func TestNotifySkipsUnsubscribed(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(200, "glm-5.3", true); err != nil {
		t.Fatal(err)
	}
	checks := downChecks("gpt-6-astra", 503, "http")
	commitCycle(t, b, cycle(checks, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	b.deliveryTick(context.Background())
	if m.callCount("sendRichMessage") != 0 {
		t.Fatalf("no notifications expected, got %d", m.callCount("sendRichMessage"))
	}
}

// The first cycle (a baseline, no transitions) — no message flood: events
// are created only on a state change, prev=="" stays silent.
func TestFirstCycleNoFlood(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	checks := map[string]storage.Check{
		"gpt-6-astra": check("gpt-6-astra", status.Operational),
		"glm-5.3":     check("glm-5.3", status.Operational),
	}
	seedCheck(t, b, checks)
	commitCycle(t, b, cycle(checks))
	b.deliveryTick(context.Background())
	if m.callCount("sendRichMessage") != 0 {
		t.Fatalf("baseline must not notify, got %d", m.callCount("sendRichMessage"))
	}
	if rows, _ := store.PendingOutbox(10); len(rows) != 0 {
		t.Fatalf("baseline must not enqueue events: %+v", rows)
	}
}

// Mixed cycle events — ONE rich message per chat, lines in config order:
// a fall is interactive, a recovery has no button.
func TestMixedTransitionsOneMessageConfigOrder(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	for _, id := range []string{"gpt-6-astra", "glm-5.3"} {
		if err := store.SetSubscription(100, id, true); err != nil {
			t.Fatal(err)
		}
	}
	checks := map[string]storage.Check{
		"gpt-6-astra": {ModelID: "gpt-6-astra", At: time.Now(), Status: status.Down, HTTPStatus: 429, ErrorType: "http"},
		"glm-5.3":     {ModelID: "glm-5.3", At: time.Now(), Status: status.Operational, HTTPStatus: 200},
	}
	seedCheck(t, b, checks)
	commitCycle(t, b, cycle(checks,
		transition("glm-5.3", status.Down, status.Operational, time.Now(), nil),
		transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	b.deliveryTick(context.Background())

	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("one message per chat per cycle, got %d", n)
	}
	html := lastRichHTML(t, m, "sendRichMessage")
	down := "<p>🔴 <code>gpt-6-astra</code> - " + stBtn("gpt-6-astra", "HTTP 429") + "</p>"
	rec := "<p>🟢 <code>glm-5.3</code> - Восстановилась</p>"
	if !strings.Contains(html, down) || !strings.Contains(html, rec) {
		t.Fatalf("both lines expected:\n%s", html)
	}
	// line order follows the config order: gpt-6-astra before glm-5.3
	if !(strings.Index(html, "gpt-6-astra") < strings.Index(html, "glm-5.3")) {
		t.Fatalf("config order broken:\n%s", html)
	}
	if strings.Count(html, "<p>") != 2 {
		t.Fatalf("exactly two lines expected:\n%s", html)
	}
}

// No headers or footers: no "OpenTW", no availability words, no counts,
// no durations, no bulk labels, no "more N" hints.
func TestNotificationNoHeadersOrFooters(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	for _, id := range []string{"gpt-6-astra", "glm-5.3"} {
		if err := store.SetSubscription(100, id, true); err != nil {
			t.Fatal(err)
		}
	}
	checks := map[string]storage.Check{
		"gpt-6-astra": {ModelID: "gpt-6-astra", At: time.Now(), Status: status.Down, HTTPStatus: 500, ErrorType: "http"},
		"glm-5.3":     {ModelID: "glm-5.3", At: time.Now(), Status: status.Operational, HTTPStatus: 200},
	}
	seedCheck(t, b, checks)
	commitCycle(t, b, cycle(checks,
		transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil),
		transition("glm-5.3", status.Down, status.Operational, time.Now(), nil)))
	b.deliveryTick(context.Background())

	html := lastRichHTML(t, m, "sendRichMessage")
	for _, bad := range []string{
		"OpenTW", "недоступна", "Недоступна", "Восстановились", "Проверено",
		"Длительность", "Массовый", "ещё", "POST", "изменени",
	} {
		if strings.Contains(html, bad) {
			t.Fatalf("notification must not contain header/footer %q:\n%s", bad, html)
		}
	}
}

// The notification's button label comes from the model's LATEST check:
// timeout → Timeout.
func TestNotificationTimeoutLabel(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	checks := downChecks("gpt-6-astra", 0, "timeout")
	seedCheck(t, b, checks)
	commitCycle(t, b, cycle(checks, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	b.deliveryTick(context.Background())
	html := lastRichHTML(t, m, "sendRichMessage")
	if !strings.Contains(html, "<p>🔴 <code>gpt-6-astra</code> - "+stBtn("gpt-6-astra", "Timeout")+"</p>") {
		t.Fatalf("timeout label wrong:\n%s", html)
	}
}

// 200 with an unexpected text: the label "Unexpected response".
func TestNotificationUnexpectedResponseLabel(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	checks := downChecks("gpt-6-astra", 200, "response")
	seedCheck(t, b, checks)
	commitCycle(t, b, cycle(checks, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	b.deliveryTick(context.Background())
	html := lastRichHTML(t, m, "sendRichMessage")
	if !strings.Contains(html, "<p>🔴 <code>gpt-6-astra</code> - "+stBtn("gpt-6-astra", "Unexpected response")+"</p>") {
		t.Fatalf("unexpected-response label wrong:\n%s", html)
	}
}

// The notification's down line is the same HTML the panel builds (the shared
// downModelLine); the notification only wraps the line in <p>.
func TestDownLineMatchesModelLine(t *testing.T) {
	m := storage.Model{ID: "gpt-6-astra", Family: "GPT"}
	for _, c := range []storage.Check{
		{ModelID: m.ID, Status: status.Down, HTTPStatus: 429, ErrorType: "http"},
		{ModelID: m.ID, Status: status.Down, ErrorType: "timeout"},
		{ModelID: m.ID, Status: status.Down, HTTPStatus: 200, ErrorType: "response"},
	} {
		got, want := downLine(m.ID, c), "<p>"+modelLine(m, c)+"</p>"
		if got != want {
			t.Errorf("errType=%s: downLine must reuse the panel HTML:\n got: %q\nwant: %q", c.ErrorType, got, want)
		}
	}
}

// The bot can no longer write to the chat (403 blocked) — subscriptions are
// dead; the chat's subscriptions and queue are removed from the store.
func TestNotifyDropsSubscriptionsOfDeadChat(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	if err := store.SetSubscription(100, "glm-5.3", true); err != nil {
		t.Fatal(err)
	}
	m.handler = func(method string, body []byte) (any, int) {
		return map[string]any{"ok": false, "error_code": 403, "description": "Forbidden: bot was blocked by the user"}, http.StatusForbidden
	}
	checks := downChecks("gpt-6-astra", 503, "http")
	seedCheck(t, b, checks)
	commitCycle(t, b, cycle(checks, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	b.deliveryTick(context.Background())
	subs, _ := store.Subscriptions(100)
	if len(subs) != 0 {
		t.Fatalf("dead chat subscriptions must be dropped, got %v", subs)
	}
	if rows, _ := store.PendingOutbox(10); len(rows) != 0 {
		t.Fatalf("dead chat queue must be dropped too, got %+v", rows)
	}
}

// A transient server failure (5xx) must not drop subscriptions.
func TestNotifyKeepsSubscriptionsOnTransientError(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	if err := store.SetSubscription(100, "glm-5.3", true); err != nil {
		t.Fatal(err)
	}
	m.handler = func(method string, body []byte) (any, int) {
		return map[string]any{"ok": false, "error_code": 500, "description": "Internal Server Error"}, http.StatusInternalServerError
	}
	checks := downChecks("gpt-6-astra", 503, "http")
	seedCheck(t, b, checks)
	commitCycle(t, b, cycle(checks, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	b.deliveryTick(context.Background())
	subs, _ := store.Subscriptions(100)
	if len(subs) != 2 {
		t.Fatalf("subscriptions must survive a transient failure, got %v", subs)
	}
}
