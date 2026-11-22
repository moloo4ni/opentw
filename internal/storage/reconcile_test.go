package storage

// Tests of the notification reconciliation mechanism: chat_notified ↔
// last_status divergence checks and parking an uncertainty row without an
// attempt counter.

import (
	"testing"
	"time"

	"opentw/internal/status"
)

// StaleNotifyPairs: a settled state diverging from the notified one is
// a pair; Unknown and matches are not; no chat_notified row — not a pair.
func TestStaleNotifyPairs(t *testing.T) {
	s := openTest(t)
	models := []Model{
		{ID: "m-down", Family: "A"},  // down
		{ID: "m-up", Family: "A"},    // fine
		{ID: "m-unk", Family: "A"},   // not definitive
		{ID: "m-fresh", Family: "A"}, // never notified at all
	}
	if err := s.SyncModels(models); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := s.SetModelState("m-down", status.Down, "http", "503", now); err != nil {
		t.Fatal(err)
	}
	if err := s.SetModelState("m-up", status.Operational, "", "", now); err != nil {
		t.Fatal(err)
	}
	if err := s.SetModelState("m-unk", status.Unknown, "invalid", "x", now); err != nil {
		t.Fatal(err)
	}
	if err := s.SetModelState("m-fresh", status.Down, "http", "503", now); err != nil {
		t.Fatal(err)
	}
	// chat 100 subscribes to everything; notified: m-down=Up (a divergence),
	// m-up=Up (a match), m-unk=Down (not compared), m-fresh never notified
	for _, id := range models {
		if err := s.SetSubscription(100, id.ID, true); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.SetChatNotifiedState(100, "m-down", status.Operational); err != nil {
		t.Fatal(err)
	}
	if err := s.SetChatNotifiedState(100, "m-up", status.Operational); err != nil {
		t.Fatal(err)
	}
	if err := s.SetChatNotifiedState(100, "m-unk", status.Down); err != nil {
		t.Fatal(err)
	}
	pairs, err := s.StaleNotifyPairs()
	if err != nil {
		t.Fatal(err)
	}
	if len(pairs) != 1 {
		t.Fatalf("exactly one stale pair expected, got %+v", pairs)
	}
	p := pairs[0]
	if p.ChatID != 100 || p.ModelID != "m-down" || p.Current != status.Down || p.Notified != status.Operational {
		t.Fatalf("pair = %+v", p)
	}
	if p.NotifiedAt == "" {
		t.Fatal("NotifiedAt epoch must be non-empty (dedupe key basis)")
	}
	// recovery after a delivered Down is also a divergence
	if err := s.SetModelState("m-down", status.Operational, "", "", now); err != nil {
		t.Fatal(err)
	}
	if err := s.SetChatNotifiedState(100, "m-down", status.Down); err != nil {
		t.Fatal(err)
	}
	pairs, err = s.StaleNotifyPairs()
	if err != nil {
		t.Fatal(err)
	}
	if len(pairs) != 1 || pairs[0].ModelID != "m-down" || pairs[0].Current != status.Operational || pairs[0].Notified != status.Down {
		t.Fatalf("recovery divergence expected, got %+v", pairs)
	}
	// a nonexistent model in subscriptions must not break the query (JOIN)
	if err := s.SetSubscription(100, "ghost", true); err != nil {
		t.Fatal(err)
	}
	if _, err = s.StaleNotifyPairs(); err != nil {
		t.Fatalf("ghost subscription must not break the query: %v", err)
	}
}

// DeferOutboxRetry parks the row without touching attempts; DeferOutboxAttempt
// keeps counting failures.
func TestDeferOutboxRetryDoesNotCountAttempts(t *testing.T) {
	s := openTest(t)
	if err := s.EnqueueOutbox(100, "single", "k1", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	row, err := s.PendingOutbox(10)
	if err != nil || len(row) != 1 {
		t.Fatalf("rows = %+v err = %v", row, err)
	}
	if err := s.DeferOutboxRetry(row[0].ID, time.Minute); err != nil {
		t.Fatal(err)
	}
	got, err := s.PendingOutbox(10)
	if err != nil || len(got) != 1 {
		t.Fatalf("rows = %+v err = %v", got, err)
	}
	if got[0].Attempts != 0 {
		t.Fatalf("retry defer must not count attempts, got %d", got[0].Attempts)
	}
	if got[0].NextAttemptAt == nil {
		t.Fatal("next attempt deadline must be set")
	}
	if d := time.Until(*got[0].NextAttemptAt); d < 30*time.Second || d > 90*time.Second {
		t.Fatalf("next attempt = %v, want ~1m", d)
	}
	if err := s.DeferOutboxAttempt(row[0].ID, time.Minute); err != nil {
		t.Fatal(err)
	}
	got, _ = s.PendingOutbox(10)
	if got[0].Attempts != 1 {
		t.Fatalf("attempt defer must count, got %d", got[0].Attempts)
	}
}
