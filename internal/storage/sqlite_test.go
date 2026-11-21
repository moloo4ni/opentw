package storage

import (
	"database/sql"
	"io"
	"os"
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
		t.Fatalf("reopen: %v", err)
	}
	s2.Close()
}

// Свежая (несуществующая) база создаётся целиком из schema.sql: все таблицы,
// индексы и колонки, которые реально читает код, плюс маркер user_version.
func TestOpenCreatesFreshSchema(t *testing.T) {
	s := openTest(t)
	tables, indexes := map[string]bool{}, map[string]bool{}
	rows, err := s.db.Query(`SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var typ, name string
		if err := rows.Scan(&typ, &name); err != nil {
			t.Fatal(err)
		}
		switch typ {
		case "table":
			tables[name] = true
		case "index":
			indexes[name] = true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	wantTables := []string{"models", "checks", "incidents", "subscriptions",
		"status_messages", "notification_outbox", "bot_state", "chat_notified"}
	for _, want := range wantTables {
		if !tables[want] {
			t.Errorf("table %s missing (have %v)", want, tables)
		}
	}
	for _, want := range []string{"idx_checks_model_ts", "idx_incidents_model", "idx_subscriptions_model", "idx_outbox_pending"} {
		if !indexes[want] {
			t.Errorf("index %s missing (have %v)", want, indexes)
		}
	}
	cols := func(table string) map[string]bool {
		t.Helper()
		out := map[string]bool{}
		rows, err := s.db.Query(`SELECT name FROM pragma_table_info(?)`, table)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatal(err)
			}
			out[name] = true
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	for _, want := range []string{"response", "status", "error_type", "http_status"} {
		if !cols("checks")[want] {
			t.Errorf("checks.%s missing", want)
		}
	}
	for _, want := range []string{"sent_at", "attempts", "next_attempt_at", "dedupe_key"} {
		if !cols("notification_outbox")[want] {
			t.Errorf("notification_outbox.%s missing", want)
		}
	}
	var v int
	if err := s.db.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil || v != schemaVersion {
		t.Fatalf("user_version = %d, %v (want %d)", v, err, schemaVersion)
	}
}

// База от старой версии OpenTW (таблицы исторических миграций без маркера
// актуальной схемы) блокирует запуск понятной ошибкой: никакой тихой
// конверсии и никакого удаления данных.
func TestOpenRejectsLegacyDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`CREATE TABLE models (
		model_id TEXT PRIMARY KEY, family TEXT NOT NULL, last_status TEXT,
		error_type TEXT, error_message TEXT, last_check_at TEXT);
		CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);
		INSERT INTO models VALUES ('glm-5.3', 'GLM', 'degraded', 'perf', '', '2026-11-01T00:00:00Z');
		INSERT INTO schema_migrations VALUES (4, '2026-11-01T00:00:00Z');`); err != nil {
		t.Fatal(err)
	}
	raw.Close()

	s, err := Open(path)
	if err == nil {
		s.Close()
		t.Fatal("legacy database must be rejected")
	}
	if !strings.Contains(err.Error(), "not compatible") || !strings.Contains(err.Error(), "clean database") {
		t.Fatalf("rejection must explain the fix: %v", err)
	}
	// база не тронута: данные и отсутствие маркера — как были
	raw2, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer raw2.Close()
	var n int
	if err := raw2.QueryRow(`SELECT COUNT(*) FROM models WHERE last_status = 'degraded'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("legacy data must stay untouched: %d, %v", n, err)
	}
	var v int
	if err := raw2.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil || v != 0 {
		t.Fatalf("user_version must stay 0, got %d %v", v, err)
	}
}

// Реальная копия продовой базы (opentw-backup-final.db): отвергается той же
// понятной ошибкой — миграция исторических данных не выполняется по дизайну.
func TestOpenRejectsRealSnapshot(t *testing.T) {
	src := filepath.Join("..", "..", "opentw-backup-final.db")
	if _, err := os.Stat(src); err != nil {
		t.Skip("opentw-backup-final.db отсутствует")
	}
	dst := filepath.Join(t.TempDir(), "backup.db")
	in, err := os.Open(src)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(out, in); err != nil {
		t.Fatal(err)
	}
	out.Close()

	s, err := Open(dst)
	if err == nil {
		s.Close()
		t.Fatal("real legacy snapshot must be rejected")
	}
	if !strings.Contains(err.Error(), "not compatible") || !strings.Contains(err.Error(), "clean database") {
		t.Fatalf("rejection must explain the fix: %v", err)
	}
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

// Модель ушла из конфигурации: вместе со строкой состояния и подписками
// уходит и уведомлённое состояние чатов — возвратившаяся модель не глухнет
// к первому повторному инциденту по stale chat_notified.
func TestSyncModelsDropsNotifiedStateOfRemovedModels(t *testing.T) {
	s := openTest(t)
	if err := s.SyncModels([]Model{{ID: "a", Family: "A"}, {ID: "gone", Family: "G"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetChatNotifiedState(100, "gone", status.Down); err != nil {
		t.Fatal(err)
	}
	if err := s.SetChatNotifiedState(100, "a", status.Operational); err != nil {
		t.Fatal(err)
	}
	if err := s.SyncModels([]Model{{ID: "a", Family: "A"}}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.ChatNotifiedState(100, "gone"); err != nil || ok {
		t.Fatalf("notified state of removed model must be gone: ok=%v err=%v", ok, err)
	}
	if st, ok, _ := s.ChatNotifiedState(100, "a"); !ok || st != status.Operational {
		t.Fatalf("notified state of kept model must survive: %q %v", st, ok)
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
	if m, err = s.Model("nope"); err != nil || m != nil {
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
		Response: "429 Too Many Requests — «квота исчерпана», повторите позже",
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
	if last["glm-5.3"].Response != newer.Response {
		t.Fatalf("response roundtrip broken: %q", last["glm-5.3"].Response)
	}
	if last["kimi-k3"].Response != "" {
		t.Fatalf("empty response must read as '' (two quotes), got %q", last["kimi-k3"].Response)
	}

	recent, err := s.RecentChecks("glm-5.3", base.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(recent) != 1 {
		t.Fatalf("recent within window = %d, want 1", len(recent))
	}
	all, err := s.RecentChecks("glm-5.3", base.Add(-3*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 || all[0].Response != "" || all[1].Response != newer.Response {
		t.Fatalf("recent response roundtrip broken: %+v", all)
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
	if sm, err = s.StatusMessage(100); err != nil || sm != nil {
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
	if err := s.SetModelState("glm-5.3", status.Down, "http", "Rate limited", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertCheck(Check{ModelID: "glm-5.3", At: time.Now(), Status: status.Down}); err != nil {
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
	if m == nil || m.LastStatus != status.Down {
		t.Fatalf("model state lost: %+v", m)
	}
	if subs, _ := s2.Subscriptions(300); len(subs) != 1 || subs[0] != "glm-5.3" {
		t.Fatalf("subscriptions lost: %v", subs)
	}
	if sm, _ := s2.StatusMessage(300); sm == nil || sm.MessageID != 999 {
		t.Fatalf("status message lost: %+v", sm)
	}
	if last, _ := s2.LastChecks(); last["glm-5.3"].Status != status.Down {
		t.Fatalf("checks lost: %+v", last)
	}
	if open, _ := s2.HasOpenIncident("glm-5.3"); open != true {
		t.Fatal("open incident lost")
	}
}

// BackupTo: согласованный снапшот живой WAL-базы (маркер схемы сохраняется),
// повтор в тот же файл — ошибка.
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
