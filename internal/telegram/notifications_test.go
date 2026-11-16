package telegram

import (
	"context"
	"fmt"
	"net/http"
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
	commitCycle(t, b, res)
	b.deliveryTick(context.Background())

	if m.callCount("sendMessage") != 1 {
		t.Fatalf("exactly one notification expected, got %d", m.callCount("sendMessage"))
	}
	body := m.lastBody("sendMessage")
	if body["chat_id"].(float64) != 100 {
		t.Fatalf("notification must go to chat 100 only, got %v", body["chat_id"])
	}
	text := fmt.Sprint(body["text"])
	for _, want := range []string{"🔴 <code>gpt-6-astra</code> - недоступна", "HTTP 503", "Service Unavailable", "Проверено:", "MSK"} {
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
	commitCycle(t, b, res)
	b.deliveryTick(context.Background())
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
	commitCycle(t, b, cycle(nil))
	b.deliveryTick(context.Background())
	if m.callCount("sendMessage") != 0 {
		t.Fatalf("unchanged status must not notify, got %d", m.callCount("sendMessage"))
	}
	// два разных перехода — два уведомления
	commitCycle(t, b, cycle(nil, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	b.deliveryTick(context.Background())
	commitCycle(t, b, cycle(nil, transition("gpt-6-astra", status.Down, status.Operational, time.Now(), nil)))
	b.deliveryTick(context.Background())
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
	commitCycle(t, b, cycle(nil, transition("gpt-6-astra", status.Down, status.Operational, time.Now(), inc)))
	b.deliveryTick(context.Background())
	text := fmt.Sprint(m.lastBody("sendMessage")["text"])
	for _, want := range []string{"🟢 <code>gpt-6-astra</code> - восстановилась", "Latency: 2.70s", "TPS: 42.3", "Проверено:", "Длительность сбоя: 13м 02с"} {
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
	commitCycle(t, b, cycle(nil, transition("gpt-6-astra", status.Down, status.Degraded, time.Now(), inc)))
	b.deliveryTick(context.Background())
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
	commitCycle(t, b, cycle(nil, transition("gpt-6-astra", status.Operational, status.Degraded, time.Now(), nil)))
	b.deliveryTick(context.Background())
	text := fmt.Sprint(m.lastBody("sendMessage")["text"])
	for _, want := range []string{"🟡 <code>gpt-6-astra</code> - работает нестабильно", "HTTP 429", "Upstream rate limit", "Проверено:"} {
		if !strings.Contains(text, want) {
			t.Errorf("degradation missing %q:\n%s", want, text)
		}
	}
}

// Бот больше не может писать в чат (403 blocked) — подписки мертвы,
// из хранилища удаляются все остальные подписки этого чата.
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
	res := cycle(nil, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil))
	commitCycle(t, b, res)
	b.deliveryTick(context.Background())
	subs, _ := store.Subscriptions(100)
	if len(subs) != 0 {
		t.Fatalf("dead chat subscriptions must be dropped, got %v", subs)
	}
	if rows, _ := store.PendingOutbox(10); len(rows) != 0 {
		t.Fatalf("dead chat queue must be dropped too, got %+v", rows)
	}
}

// Временный сбой сервера (5xx) не должен выбрасывать подписки.
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
	res := cycle(nil, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil))
	commitCycle(t, b, res)
	b.deliveryTick(context.Background())
	subs, _ := store.Subscriptions(100)
	if len(subs) != 2 {
		t.Fatalf("subscriptions must survive a transient failure, got %v", subs)
	}
}

// D2: падение, обнаруженное после слепого окна монитора, честно помечается
// в одиночном уведомлении; обычное падение пометки не имеет.
func TestBlindFallNotificationHonestNote(t *testing.T) {
	blind := transition("gpt-6-astra", status.Unknown, status.Down, time.Now(), nil)
	text := RenderNotification(blind)
	if !strings.Contains(text, "недоступна") {
		t.Fatalf("must be a fall notification:\n%s", text)
	}
	if !strings.Contains(text, "Точное время начала недоступности неизвестно") {
		t.Fatalf("blind fall must honestly mark the unknown start:\n%s", text)
	}
	plain := transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)
	if plainText := RenderNotification(plain); strings.Contains(plainText, "неизвестно") {
		t.Fatalf("plain fall must not claim an unknown start:\n%s", plainText)
	}
}

// D2: групповая сводка помечает слепое падение короткой пометкой.
func TestGroupedLineMarksBlindFall(t *testing.T) {
	trs := []monitor.Transition{
		transition("gpt-6-astra", status.Unknown, status.Down, time.Now(), nil),
	}
	text := RenderGrouped(deliveryPayload{Trs: trs, Total: 2, DownInCycle: 1})
	if !strings.Contains(text, "<code>gpt-6-astra</code> - HTTP 503") ||
		!strings.Contains(text, "(время падения неизвестно)") {
		t.Fatalf("grouped line must mark the blind fall:\n%s", text)
	}
}
