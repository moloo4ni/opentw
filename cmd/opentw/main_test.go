package main

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"opentw/internal/status"
	"opentw/internal/storage"
)

// supervise must return only after ALL components finished: only then is
// it safe for run()'s deferred store.Close() to close the database.
func TestSuperviseWaitsForAllComponents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slowDone := make(chan struct{})
	release := make(chan struct{})
	slow := func(context.Context) error {
		<-release // an emulated final cycle finishing its work after the cancel
		close(slowDone)
		return nil
	}
	fast := func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	}
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	go func() { time.Sleep(60 * time.Millisecond); close(release) }()
	if err := supervise(ctx, cancel, []func(context.Context) error{fast, slow}); err != nil {
		t.Fatalf("err = %v", err)
	}
	select {
	case <-slowDone:
	default:
		t.Fatal("supervise returned before the slow component finished")
	}
}

// One component's failure cancels the other, and supervise does not return
// until the other finishes.
func TestSuperviseStopsSiblingOnError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	boom := func(context.Context) error { return errors.New("boom") }
	siblingDone := make(chan struct{})
	sibling := func(ctx context.Context) error {
		<-ctx.Done()
		close(siblingDone)
		return nil
	}
	err := supervise(ctx, cancel, []func(context.Context) error{sibling, boom})
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v", err)
	}
	select {
	case <-siblingDone:
	default:
		t.Fatal("sibling must be stopped before supervise returns")
	}
}

// --- Startup ---

// writeTestConfig creates a temporary config.yaml without network use
// (secrets come from ENV).
func writeTestConfig(t *testing.T, sqlitePath string) string {
	t.Helper()
	cfg := "interval: 5m\nhttp_timeout: 5s\nconcurrency: 2\nmax_tokens: 8\nstreaming: false\nsqlite_path: " + sqlitePath + "\n" +
		"models:\n  - family: T\n    ids:\n      - model-a\n"
	path := t.TempDir() + "/config.yaml"
	if err := os.WriteFile(path, []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// The -backup flag: a consistent snapshot of a live database, readable after
// closing.
func TestRunBackupFlag(t *testing.T) {
	db := t.TempDir() + "/live.db"
	s, err := storage.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SyncModels([]storage.Model{{ID: "model-a", Family: "T"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertCheck(storage.Check{ModelID: "model-a", At: time.Now(), Status: status.Operational, HTTPStatus: 200}); err != nil {
		t.Fatal(err)
	}
	s.Close()

	t.Setenv("BOT_TOKEN", "T")
	t.Setenv("API_KEY", "K")
	t.Setenv("API_BASE_URL", "http://localhost:1") // the network is not touched before the backup
	cfgPath := writeTestConfig(t, db)
	snap := t.TempDir() + "/snap.db"

	args := os.Args
	os.Args = []string{"opentw", "-config", cfgPath, "-backup", snap}
	defer func() { os.Args = args }()
	if err := run(); err != nil {
		t.Fatalf("run(-backup): %v", err)
	}
	if _, err := os.Stat(snap); err != nil {
		t.Fatalf("snapshot missing: %v", err)
	}
	// the snapshot is consistent: opens and holds the data
	s2, err := storage.Open(snap)
	if err != nil {
		t.Fatalf("snapshot open: %v", err)
	}
	defer s2.Close()
	checks, err := s2.RecentChecks("model-a", time.Now().Add(-time.Hour))
	if err != nil || len(checks) != 1 {
		t.Fatalf("snapshot must hold the check: %v %v", checks, err)
	}
	// a repeat into an existing file fails loudly
	if err := run(); err == nil {
		t.Fatal("backup into an existing file must fail loudly")
	}
	// run() closed the live database (deferred store.Close): the file reopens
	s3, err := storage.Open(db)
	if err != nil {
		t.Fatalf("live db must be closed by run(): %v", err)
	}
	s3.Close()
}

// A broken config — a clear error, no database created.
func TestRunBadConfig(t *testing.T) {
	t.Setenv("BOT_TOKEN", "T")
	t.Setenv("API_KEY", "K")
	t.Setenv("API_BASE_URL", "http://localhost:1")
	path := t.TempDir() + "/config.yaml"
	if err := os.WriteFile(path, []byte("interval: 3s\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	args := os.Args
	os.Args = []string{"opentw", "-config", path}
	defer func() { os.Args = args }()
	if err := run(); err == nil {
		t.Fatal("invalid config must fail run()")
	}
}

// The worst cycle accounts for the catalog request and the check waves
// (consistent with the HEALTHCHECK and maxage).
func TestWorstCycleTime(t *testing.T) {
	// 25 models, concurrency 4, timeout 120s: catalog + ceil(25/4)=7 waves
	if got := worstCycleTime(25, 4, 120*time.Second); got != (1+7)*120*time.Second {
		t.Fatalf("worstCycleTime = %v", got)
	}
	if got := worstCycleTime(1, 8, 10*time.Second); got != 2*10*time.Second {
		t.Fatalf("worstCycleTime = %v", got)
	}
}
