package telegram

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"opentw/internal/monitor"
	"opentw/internal/status"
	"opentw/internal/storage"
)

func transition(id string, from, to status.Status, at time.Time, incident *storage.Incident) monitor.Transition {
	c := storage.Check{ModelID: id, At: at, Status: to, HTTPStatus: 200, LatencyUS: ptr1(2_700_000), TPS: ptrF(42.3)}
	if to != status.Operational {
		c.LatencyUS = nil
		c.TPS = nil
		c.TTFTUS = nil
		c.GenUS = nil
		switch to {
		case status.Degraded:
			c.HTTPStatus = 429
			c.ErrorType = "http"
			c.ErrorMessage = "Upstream rate limit"
		case status.Down:
			c.HTTPStatus = 503
			c.ErrorType = "http"
			c.ErrorMessage = "Service Unavailable"
		}
	}
	return monitor.Transition{ModelID: id, From: from, To: to, Check: c, Incident: incident}
}

// Сценарий 14: уведомление получает только подписанный на модель чат.
func TestNotifyOnlySubscribedChats(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	// чат 100 подписан на gpt-6-astra, чат 200 — на glm-5.3
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	if err := store.SetSubscription(200, "glm-5.3", true); err != nil {
		t.Fatal(err)
	}
	res := cycle(nil, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil))
	b.OnCycle(context.Background(), res)

	if m.callCount("sendMessage") != 1 {
		t.Fatalf("exactly one notification expected, got %d", m.callCount("sendMessage"))
	}
	body := m.lastBody("sendMessage")
	if body["chat_id"].(float64) != 100 {
		t.Fatalf("notification must go to chat 100 only, got %v", body["chat_id"])
	}
	text := fmt.Sprint(body["text"])
	for _, want := range []string{"🔴 <code>gpt-6-astra</code> - недоступна", "HTTP 503", "Service Unavailable", "Время:", "MSK"} {
		if !strings.Contains(text, want) {
			t.Errorf("notification missing %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "DOWN") || strings.Contains(text, "—") {
		t.Errorf("old uppercase/english format leaked:\n%s", text)
	}
}

// Сценарий 15: неподписанный чат не получает уведомлений.
func TestNotifySkipsUnsubscribed(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(200, "glm-5.3", true); err != nil {
		t.Fatal(err)
	}
	res := cycle(nil, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil))
	b.OnCycle(context.Background(), res)
	if m.callCount("sendMessage") != 0 {
		t.Fatalf("no notifications expected, got %d", m.callCount("sendMessage"))
	}
}

// Сценарий 16: повторное уведомление при неизменном статусе не отправляется.
func TestNoRepeatNotificationOnSameStatus(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	// переходов нет — событий нет
	b.OnCycle(context.Background(), cycle(nil))
	if m.callCount("sendMessage") != 0 {
		t.Fatalf("unchanged status must not notify, got %d", m.callCount("sendMessage"))
	}
	// два разных перехода — два уведомления
	b.OnCycle(context.Background(), cycle(nil, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	b.OnCycle(context.Background(), cycle(nil, transition("gpt-6-astra", status.Down, status.Operational, time.Now(), nil)))
	if m.callCount("sendMessage") != 2 {
		t.Fatalf("two transitions must notify twice, got %d", m.callCount("sendMessage"))
	}
}

// Сценарий 17: восстановление содержит длительность инцидента.
func TestRecoveryNotificationIncludesDuration(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	inc := &storage.Incident{ModelID: "gpt-6-astra", From: status.Down, To: status.Operational, Duration: 13*time.Minute + 2*time.Second}
	b.OnCycle(context.Background(), cycle(nil, transition("gpt-6-astra", status.Down, status.Operational, time.Now(), inc)))
	text := fmt.Sprint(m.lastBody("sendMessage")["text"])
	for _, want := range []string{"🟢 <code>gpt-6-astra</code> - восстановилась", "Latency: 2.70s", "TPS: 42.3", "Время:", "Длительность сбоя: 13м 02с"} {
		if !strings.Contains(text, want) {
			t.Errorf("recovery missing %q:\n%s", want, text)
		}
	}
}

// Down → Degraded — частичное восстановление.
func TestPartialRecoveryNotification(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	inc := &storage.Incident{ModelID: "gpt-6-astra", From: status.Down, To: status.Degraded, Duration: 5 * time.Minute}
	b.OnCycle(context.Background(), cycle(nil, transition("gpt-6-astra", status.Down, status.Degraded, time.Now(), inc)))
	text := fmt.Sprint(m.lastBody("sendMessage")["text"])
	if !strings.Contains(text, "🟡 <code>gpt-6-astra</code> - частично восстановилась") {
		t.Fatalf("text = %s", text)
	}
	if !strings.Contains(text, "Длительность сбоя: 5м 00с") {
		t.Fatalf("duration missing:\n%s", text)
	}
}

// Деградация Operational → Degraded.
func TestDegradationNotification(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	b.OnCycle(context.Background(), cycle(nil, transition("gpt-6-astra", status.Operational, status.Degraded, time.Now(), nil)))
	text := fmt.Sprint(m.lastBody("sendMessage")["text"])
	for _, want := range []string{"🟡 <code>gpt-6-astra</code> - работает нестабильно", "HTTP 429", "Upstream rate limit", "Время:"} {
		if !strings.Contains(text, want) {
			t.Errorf("degradation missing %q:\n%s", want, text)
		}
	}
}
