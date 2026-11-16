package storage

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"opentw/internal/status"

	_ "modernc.org/sqlite"
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
	if err := s.SetModelState("glm-5.3", status.Down, "http", "Service Unavailable", at); err != nil {
		t.Fatal(err)
	}
	m, err := s.Model("glm-5.3")
	if err != nil || m == nil {
		t.Fatalf("model gone: %v %v", m, err)
	}
	if m.LastStatus != status.Down || m.ErrorType != "http" || m.ErrorMessage != "Service Unavailable" {
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
		Status: status.Operational, LatencyUS: ptrInt64(1_200_000), TPS: ptrFloat(42.0),
		InputTokens: ptrInt64(10), OutputTokens: ptrInt64(4),
	}
	newer := Check{
		ModelID: "glm-5.3", At: base, HTTPStatus: 503,
		Status: status.Down, ErrorType: "http", ErrorMessage: "Service Unavailable",
	}
	other := Check{ModelID: "kimi-k3", At: base, HTTPStatus: 200, Status: status.Operational}
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
	if last["kimi-k3"].Status != status.Operational {
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
	old := Check{ModelID: "a", At: now.Add(-8 * 24 * time.Hour), Status: status.Down}
	fresh := Check{ModelID: "a", At: now.Add(-time.Hour), Status: status.Operational}
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
	if err := s.OpenIncident("glm-5.3", status.Operational, start); err != nil {
		t.Fatal(err)
	}
	end := start.Add(30 * time.Minute)
	inc, err := s.CloseIncident("glm-5.3", status.Down, end)
	if err != nil {
		t.Fatal(err)
	}
	if inc == nil || inc.From != status.Operational || inc.To != status.Down {
		t.Fatalf("incident = %+v", inc)
	}
	if inc.Duration != 30*time.Minute {
		t.Fatalf("duration = %v", inc.Duration)
	}
	if err := s.OpenIncident("glm-5.3", status.Down, end); err != nil {
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
	if err := s.SetModelState("glm-5.3", status.Degraded, "http", "Rate limited", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertCheck(Check{ModelID: "glm-5.3", At: time.Now(), Status: status.Degraded}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetSubscription(300, "glm-5.3", true); err != nil {
		t.Fatal(err)
	}
	if err := s.SetStatusMessage(300, 999); err != nil {
		t.Fatal(err)
	}
	if err := s.OpenIncident("glm-5.3", status.Operational, time.Now()); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	m, _ := s2.Model("glm-5.3")
	if m == nil || m.LastStatus != status.Degraded {
		t.Fatalf("model state lost: %+v", m)
	}
	if subs, _ := s2.Subscriptions(300); len(subs) != 1 || subs[0] != "glm-5.3" {
		t.Fatalf("subscriptions lost: %v", subs)
	}
	if sm, _ := s2.StatusMessage(300); sm == nil || sm.MessageID != 999 {
		t.Fatalf("status message lost: %+v", sm)
	}
	if last, _ := s2.LastChecks(); last["glm-5.3"].Status != status.Degraded {
		t.Fatalf("checks lost: %+v", last)
	}
	if open, _ := s2.HasOpenIncident("glm-5.3"); open != true {
		t.Fatal("open incident lost")
	}
}

// Апгрейд базы, созданной до появления миграций (таблицы v1 без журнала
// schema_migrations): Open применяет недостающие версии, данные не теряются.
func TestMigrateLegacyBaseWithoutJournal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`CREATE TABLE models (
		model_id TEXT PRIMARY KEY, family TEXT NOT NULL, last_status TEXT,
		error_type TEXT, error_message TEXT, last_check_at TEXT);
		INSERT INTO models VALUES ('glm-5.3', 'GLM', 'ok', '', '', '2026-11-01T00:00:00Z');`); err != nil {
		t.Fatal(err)
	}
	raw.Close()

	s, err := Open(path)
	if err != nil {
		t.Fatalf("upgrade legacy base: %v", err)
	}
	defer s.Close()
	m, err := s.Model("glm-5.3")
	if err != nil || m == nil || m.LastStatus != "ok" {
		t.Fatalf("legacy data lost: %v %+v", err, m)
	}
	if err := s.EnqueueOutbox(100, "single", "k", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	var version int
	if err := s.db.QueryRow(`SELECT MAX(version) FROM schema_migrations`).Scan(&version); err != nil || version != 3 {
		t.Fatalf("migrated to = %d, %v (want 3)", version, err)
	}
}

// База с «чужой» формой notification_outbox (интерим-сборка без новых колонок)
// должна блокировать запуск понятной ошибкой, а не падать в рантайме.
func TestOpenDetectsSchemaDrift(t *testing.T) {
	path := filepath.Join(t.TempDir(), "drift.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	// пересоздаём outbox в форме без next_attempt_at, журнал не трогаем
	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`DROP TABLE notification_outbox;
		CREATE TABLE notification_outbox (
			id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER NOT NULL,
			kind TEXT NOT NULL, payload TEXT NOT NULL, dedupe_key TEXT NOT NULL,
			created_at TEXT NOT NULL, sent_at TEXT, attempts INTEGER NOT NULL DEFAULT 0,
			UNIQUE (chat_id, dedupe_key));`); err != nil {
		t.Fatal(err)
	}
	raw.Close()
	if _, err := Open(path); err == nil {
		t.Fatal("schema drift must be detected")
	} else if !strings.Contains(err.Error(), "next_attempt_at") {
		t.Fatalf("drift error must name the column: %v", err)
	}
}

// BackupTo: согласованный снапшот живой WAL-базы, повтор в тот же файл — ошибка.
func TestBackupTo(t *testing.T) {
	s := openTest(t)
	if err := s.SyncModels([]Model{{ID: "glm-5.3", Family: "GLM"}}); err != nil {
		t.Fatal(err)
	}
	at := time.Now()
	if err := s.SetModelState("glm-5.3", status.Down, "http", "boom", at); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(t.TempDir(), "snapshot.db")
	if err := s.BackupTo(dst); err != nil {
		t.Fatal(err)
	}
	// снимок открыт и валиден независимо от исходной базы
	s2, err := Open(dst)
	if err != nil {
		t.Fatalf("open snapshot: %v", err)
	}
	defer s2.Close()
	m, err := s2.Model("glm-5.3")
	if err != nil || m == nil || m.LastStatus != status.Down {
		t.Fatalf("snapshot data = %v %+v", err, m)
	}
	if err := s.BackupTo(dst); err == nil {
		t.Fatal("backup must refuse to overwrite an existing file")
	}
}

// Реальный путь обновления существующих пользователей: база на версии 2
// получает 003 (ALTER ADD COLUMN) без потери данных.
func TestMigrateUpgradeFromV2(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v2.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetSubscription(300, "glm-5.3", true); err != nil {
		t.Fatal(err)
	}
	s.Close()
	// откатываем журнал к версии 2 и убираем колонку 003 — база «до обновления»
	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`DELETE FROM schema_migrations WHERE version > 2`,
		`CREATE TABLE notification_outbox_v2 AS SELECT id, chat_id, kind, payload, dedupe_key, created_at, sent_at, attempts FROM notification_outbox`,
		`DROP TABLE notification_outbox`,
		`ALTER TABLE notification_outbox_v2 RENAME TO notification_outbox`,
	} {
		if _, err := raw.Exec(q); err != nil {
			t.Fatal(q, err)
		}
	}
	raw.Close()

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("upgrade v2 -> v3: %v", err)
	}
	defer s2.Close()
	subs, err := s2.Subscriptions(300)
	if err != nil || len(subs) != 1 {
		t.Fatalf("data must survive upgrade: %v %v", subs, err)
	}
	if err := s2.DeferOutboxAttempt(1, time.Minute); err != nil {
		t.Fatalf("new column must be usable: %v", err)
	}
}
