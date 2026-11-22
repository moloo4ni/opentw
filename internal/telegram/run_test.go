package telegram

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

// A wrong BOT_TOKEN is a fatal configuration error: the bot must exit,
// not retry getUpdates forever.
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

// Other errors (e.g. 502) are temporary: polling continues.
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

	// more than one attempt must happen within the ctx lifetime
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

// The long polling offset is persistent — after a restart confirmed
// updates are not delivered again.
func TestOffsetPersistedAcrossRestart(t *testing.T) {
	m := newMockTG(t)
	var first atomic.Bool
	m.handler = func(method string, body []byte) (any, int) {
		if method == "getUpdates" {
			if !first.Swap(true) {
				return map[string]any{"ok": true, "result": []any{
					map[string]any{"update_id": 41, "message": map[string]any{
						"message_id": 10, "chat": map[string]any{"id": 100, "type": "private"}, "text": "/help",
					}},
				}}, http.StatusOK
			}
			return map[string]any{"ok": true, "result": []any{}}, http.StatusOK
		}
		return map[string]any{"ok": true, "result": true}, http.StatusOK
	}
	b, store := newBot(t, m)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = b.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if off, _ := store.BotOffset(); off == 42 {
			cancel()
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("offset 42 (last update_id+1) must be persisted during polling")
}
