package telegram

import (
	"context"
	"net/http"
	"testing"
	"time"
)

// Неверный BOT_TOKEN — фатальная ошибка конфигурации: бот обязан
// завершиться, а не ретраить getUpdates вечно.
func TestRunFailsFastOnUnauthorized(t *testing.T) {
	m := newMockTG(t)
	m.handler = func(method string, body []byte) (any, int) {
		return map[string]any{"ok": false, "error_code": 401, "description": "Unauthorized"}, http.StatusUnauthorized
	}
	b, _ := newBot(t, m)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Run must return a fatal error on 401")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run kept retrying an invalid bot token instead of failing fast")
	}
}

// Прочие ошибки (например 502) — временные: polling продолжается.
func TestRunRetriesTransientErrors(t *testing.T) {
	m := newMockTG(t)
	var calls int
	m.handler = func(method string, body []byte) (any, int) {
		if method == "getUpdates" {
			calls++
			return map[string]any{"ok": false, "error_code": 502, "description": "Bad Gateway"}, http.StatusBadGateway
		}
		return map[string]any{"ok": true, "result": true}, http.StatusOK
	}
	b, _ := newBot(t, m)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()

	// за время жизни ctx должно быть больше одной попытки
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("transient errors must not fail Run: %v", err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("Run did not finish")
	}
	if calls < 2 {
		t.Fatalf("expected retries on transient errors, got %d calls", calls)
	}
}
