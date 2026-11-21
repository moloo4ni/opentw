package storage

// Тесты механизма OPW-01: сверка расхождений chat_notified ↔ last_status
// и парковка строки неопределённости без счётчика попыток.

import (
	"testing"
	"time"

	"opentw/internal/status"
)

// StaleNotifyPairs: расхождение достоверного состояния с уведомлённым —
// пара; Unknown и совпадения — не пары; без chat_notified — не пара.
func TestStaleNotifyPairs(t *testing.T) {
	s := openTest(t)
	models := []Model{
		{ID: "m-down", Family: "A"},  // упала
		{ID: "m-up", Family: "A"},    // в порядке
		{ID: "m-unk", Family: "A"},   // недостоверна
		{ID: "m-fresh", Family: "A"}, // без уведомлений вовсе
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
	// chat 100 подписан на все; уведомлён: m-down=Up (расхождение),
	// m-up=Up (совпадение), m-unk=Down (сравнение не проводится),
	// m-fresh не уведомлён вовсе
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
	// восстановление после доставленного Down — тоже расхождение
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
	// несуществующая модель в подписках не должна ломать запрос (JOIN)
	if err := s.SetSubscription(100, "ghost", true); err != nil {
		t.Fatal(err)
	}
	if _, err = s.StaleNotifyPairs(); err != nil {
		t.Fatalf("ghost subscription must not break the query: %v", err)
	}
}

// DeferOutboxRetry паркует строку, не трогая attempts; DeferOutboxAttempt
// продолжает считать неудачи.
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
