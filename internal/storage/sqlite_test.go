package storage

import (
	"path/filepath"
	"testing"
	"time"

	"opentw/internal/monitor"
)

func ptrInt64(v int64) *int64     { return &v }
func ptrFloat(v float64) *float64 { return &v }

func openTest(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "opentw.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestOpenIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "opentw.db")
	s1, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	s1.Close()
	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen + migrate: %v", err)
	}
	s2.Close()
}

func TestSyncModels(t *testing.T) {
	s := openTest(t)
	err := s.SyncModels([]Model{
		{ID: "glm-5.3", Family: "GLM"},
		{ID: "kimi-k3", Family: "Kimi"},
		{ID: "glm-5.3-flash", Family: "GLM"},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = s.SyncModels([]Model{
		{ID: "glm-5.3", Family: "GLM"},
		{ID: "kimi-k3", Family: "Kimi"},
		{ID: "qwen3.8-max", Family: "Qwen"},
	})
	if err != nil {
		t.Fatal(err)
	}
	models, err := s.Models()
	if err != nil {
		t.Fatal(err)
	}
	if len(models) != 3 {
		t.Fatalf("want 3 models, got %+v", models)
	}
	if models[0].Family != "GLM" || models[0].ID != "glm-5.3" {
		t.Fatalf("ordering broken: %+v", models[0])
	}
	if models[2].ID != "qwen3.8-max" {
		t.Fatalf("new model missing: %+v", models[2])
	}
}

func TestModelStateRoundtrip(t *testing.T) {
	s := openTest(t)
	if err := s.SyncModels([]Model{{ID: "glm-5.3", Family: "GLM"}}); err != nil {
		t.Fatal(err)
	}
	at := time.Now()
	if err := s.SetModelState("glm-5.3", monitor.StatusDown, "http", "Service Unavailable", at); err != nil {
		t.Fatal(err)
	}
	m, err := s.Model("glm-5.3")
	if err != nil || m == nil {
		t.Fatalf("model gone: %v %v", m, err)
	}
	if m.LastStatus != monitor.StatusDown || m.ErrorType != "http" || m.ErrorMessage != "Service Unavailable" {
		t.Fatalf("state = %+v", m)
	}
	if m.LastCheckAt == nil || !m.LastCheckAt.Equal(at) {
		t.Fatalf("last check at = %v", m.LastCheckAt)
	}
	if m, err := s.Model("nope"); err != nil || m != nil {
		t.Fatalf("want (nil,nil) for unknown model, got (%v, %v)", m, err)
	}
}

func TestChecksRoundtrip(t *testing.T) {
	s := openTest(t)
	base := time.Now()
	older := Check{
		ModelID: "glm-5.3", At: base.Add(-2 * time.Hour), HTTPStatus: 200,
		Status: monitor.StatusOperational, LatencyUS: ptrInt64(1_200_000), TPS: ptrFloat(42.0),
		InputTokens: ptrInt64(10), OutputTokens: ptrInt64(4),
	}
	newer := Check{
		ModelID: "glm-5.3", At: base, HTTPStatus: 503,
		Status: monitor.StatusDown, ErrorType: "http", ErrorMessage: "Service Unavailable",
	}
	other := Check{ModelID: "kimi-k3", At: base, HTTPStatus: 200, Status: monitor.StatusOperational}
	for _, c := range []Check{older, newer, other} {
		if err := s.InsertCheck(c); err != nil {
			t.Fatal(err)
		}
	}
	last, err := s.LastChecks()
	if err != nil {
		t.Fatal(err)
	}
	if last["glm-5.3"].HTTPStatus != 503 || last["glm-5.3"].ErrorMessage != "Service Unavailable" {
		t.Fatalf("latest check wrong: %+v", last["glm-5.3"])
	}
	if last["kimi-k3"].Status != monitor.StatusOperational {
		t.Fatalf("other model check wrong: %+v", last["kimi-k3"])
	}
	if last["glm-5.3"].LatencyUS != nil {
		t.Fatalf("error check must have nil latency: %+v", last["glm-5.3"])
	}

	recent, err := s.RecentChecks("glm-5.3", base.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(recent) != 1 {
		t.Fatalf("recent within window = %d, want 1", len(recent))
	}
}

func TestDeleteOldChecks(t *testing.T) {
	s := openTest(t)
	now := time.Now()
	old := Check{ModelID: "a", At: now.Add(-8 * 24 * time.Hour), Status: monitor.StatusDown}
	fresh := Check{ModelID: "a", At: now.Add(-time.Hour), Status: monitor.StatusOperational}
	for _, c := range []Check{old, fresh} {
		if err := s.InsertCheck(c); err != nil {
			t.Fatal(err)
		}
	}
	n, err := s.DeleteOldChecks(7 * 24 * time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("deleted %d, want 1", n)
	}
	remaining, _ := s.RecentChecks("a", time.Time{})
	if len(remaining) != 1 {
		t.Fatalf("remaining = %d", len(remaining))
	}
}

func TestSubscriptions(t *testing.T) {
	s := openTest(t)
	if err := s.SetSubscription(100, "glm-5.3", true); err != nil {
		t.Fatal(err)
	}
	if err := s.SetSubscription(100, "kimi-k3", true); err != nil {
		t.Fatal(err)
	}
	if err := s.SetSubscription(200, "glm-5.3", true); err != nil {
		t.Fatal(err)
	}
	subs, err := s.Subscriptions(100)
	if err != nil {
		t.Fatal(err)
	}
	if len(subs) != 2 {
		t.Fatalf("chat 100 subs = %v", subs)
	}
	subsChat2, _ := s.Subscriptions(200)
	if len(subsChat2) != 1 || subsChat2[0] != "glm-5.3" {
		t.Fatalf("chat 200 subs = %v", subsChat2)
	}
	if err := s.SetSubscription(100, "glm-5.3", false); err != nil {
		t.Fatal(err)
	}
	if subs, _ := s.Subscriptions(100); len(subs) != 1 {
		t.Fatalf("after delete = %v", subs)
	}
	subscribers, err := s.Subscribers("glm-5.3")
	if err != nil {
		t.Fatal(err)
	}
	if len(subscribers) != 1 || subscribers[0] != 200 {
		t.Fatalf("subscribers of glm-5.3 = %v", subscribers)
	}
}

func TestStatusMessages(t *testing.T) {
	s := openTest(t)
	if err := s.SetStatusMessage(100, 111); err != nil {
		t.Fatal(err)
	}
	sm, err := s.StatusMessage(100)
	if err != nil || sm == nil {
		t.Fatalf("status message gone: %v %v", sm, err)
	}
	if sm.MessageID != 111 {
		t.Fatalf("message id = %d", sm.MessageID)
	}
	if err := s.SetStatusMessage(100, 222); err != nil {
		t.Fatal(err)
	}
	sm, _ = s.StatusMessage(100)
	if sm.MessageID != 222 {
		t.Fatalf("message id must be replaced, got %d", sm.MessageID)
	}
	if err := s.DeleteStatusMessage(100); err != nil {
		t.Fatal(err)
	}
	if sm, err := s.StatusMessage(100); err != nil || sm != nil {
		t.Fatalf("expected nil after delete, got %v %v", sm, err)
	}
}

func TestIncidents(t *testing.T) {
	s := openTest(t)
	start := time.Now()
	if err := s.OpenIncident("glm-5.3", monitor.StatusOperational, start); err != nil {
		t.Fatal(err)
	}
	end := start.Add(30 * time.Minute)
	inc, err := s.CloseIncident("glm-5.3", monitor.StatusDown, end)
	if err != nil {
		t.Fatal(err)
	}
	if inc == nil || inc.From != monitor.StatusOperational || inc.To != monitor.StatusDown {
		t.Fatalf("incident = %+v", inc)
	}
	if inc.Duration != 30*time.Minute {
		t.Fatalf("duration = %v", inc.Duration)
	}
	if err := s.OpenIncident("glm-5.3", monitor.StatusDown, end); err != nil {
		t.Fatal(err)
	}
	if open, _ := s.HasOpenIncident("glm-5.3"); !open {
		t.Fatal("incident must stay open after Close+Open of new one")
	}
}

func TestPersistenceAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "opentw.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SyncModels([]Model{{ID: "glm-5.3", Family: "GLM"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetModelState("glm-5.3", monitor.StatusDegraded, "http", "Rate limited", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertCheck(Check{ModelID: "glm-5.3", At: time.Now(), Status: monitor.StatusDegraded}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetSubscription(300, "glm-5.3", true); err != nil {
		t.Fatal(err)
	}
	if err := s.SetStatusMessage(300, 999); err != nil {
		t.Fatal(err)
	}
	if err := s.OpenIncident("glm-5.3", monitor.StatusOperational, time.Now()); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	m, _ := s2.Model("glm-5.3")
	if m == nil || m.LastStatus != monitor.StatusDegraded {
		t.Fatalf("model state lost: %+v", m)
	}
	if subs, _ := s2.Subscriptions(300); len(subs) != 1 || subs[0] != "glm-5.3" {
		t.Fatalf("subscriptions lost: %v", subs)
	}
	if sm, _ := s2.StatusMessage(300); sm == nil || sm.MessageID != 999 {
		t.Fatalf("status message lost: %+v", sm)
	}
	if last, _ := s2.LastChecks(); last["glm-5.3"].Status != monitor.StatusDegraded {
		t.Fatalf("checks lost: %+v", last)
	}
	if open, _ := s2.HasOpenIncident("glm-5.3"); open != true {
		t.Fatal("open incident lost")
	}
}
