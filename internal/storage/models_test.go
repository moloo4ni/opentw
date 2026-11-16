package storage

import (
	"testing"
	"time"

	"opentw/internal/status"
)

// Outbox: постановка → чтение → отметка → пусто; дедуп по (chat_id, dedupe_key).
func TestOutboxRoundtripAndDedupe(t *testing.T) {
	s := openTest(t)
	if err := s.EnqueueOutbox(100, "single", "k1", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.EnqueueOutbox(100, "single", "k1", []byte(`{}`)); err != nil { // дубликат
		t.Fatal(err)
	}
	if err := s.EnqueueOutbox(100, "single", "k2", []byte(`{}`)); err != nil { // другой ключ
		t.Fatal(err)
	}
	rows, err := s.PendingOutbox(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("want 2 unique rows, got %d", len(rows))
	}
	if rows[0].ChatID != 100 || rows[0].Kind != "single" || rows[0].DedupeKey != "k1" {
		t.Fatalf("row0 = %+v", rows[0])
	}
	if string(rows[0].Payload) != "{}" {
		t.Fatalf("payload = %q", rows[0].Payload)
	}
	if err := s.MarkOutboxSent(rows[0].ID); err != nil {
		t.Fatal(err)
	}
	rest, err := s.PendingOutbox(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 1 || rest[0].DedupeKey != "k2" {
		t.Fatalf("after mark: %+v", rest)
	}
}

// Очистка: отправленное старше 7д удаляется; не отправленное старше TTL дропается.
func TestOutboxTTLAndCleanup(t *testing.T) {
	s := openTest(t)
	if err := s.EnqueueOutbox(1, "single", "old-sent", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.EnqueueOutbox(1, "single", "old-stale", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.EnqueueOutbox(1, "single", "fresh", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE notification_outbox SET sent_at = ?, created_at = ? WHERE dedupe_key = 'old-sent'`,
		fmtTime(time.Now().Add(-8*24*time.Hour)), fmtTime(time.Now().Add(-8*24*time.Hour))); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE notification_outbox SET created_at = ? WHERE dedupe_key = 'old-stale'`,
		fmtTime(time.Now().Add(-25*time.Hour))); err != nil {
		t.Fatal(err)
	}
	if n, err := s.DeleteOldOutbox(7 * 24 * time.Hour); err != nil || n != 1 {
		t.Fatalf("DeleteOldOutbox = %d, %v", n, err)
	}
	if n, err := s.DropStaleOutbox(24 * time.Hour); err != nil || n != 1 {
		t.Fatalf("DropStaleOutbox = %d, %v", n, err)
	}
	rows, err := s.PendingOutbox(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].DedupeKey != "fresh" {
		t.Fatalf("cleanup must leave only fresh: %+v", rows)
	}
	if n, err := s.DeleteOutboxChat(1); err != nil || n != 1 {
		t.Fatalf("DeleteOutboxChat = %d, %v", n, err)
	}
	if rows, _ = s.PendingOutbox(10); len(rows) != 0 {
		t.Fatalf("DeleteOutboxChat must clear chat rows: %+v", rows)
	}
}

// ApplyCycle: состояния и события фиксируются атомарно — частичный коммит
// невозможен (P0-1: событие не существует без нового состояния и наоборот).
func TestApplyCycleAtomic(t *testing.T) {
	s := openTest(t)
	if err := s.SyncModels([]Model{{ID: "m", Family: "A"}}); err != nil {
		t.Fatal(err)
	}
	at := time.Now()
	checks := map[string]Check{
		"m": {ModelID: "m", At: at, HTTPStatus: 200, Status: status.Down},
	}
	events := []OutboxEvent{{ChatID: 100, Kind: "single", DedupeKey: "c1", Payload: []byte(`{}`)}}
	if err := s.ApplyCycle(checks, events); err != nil {
		t.Fatal(err)
	}
	m, err := s.Model("m")
	if err != nil || m == nil {
		t.Fatal(err)
	}
	if m.LastStatus != "down" {
		t.Fatalf("state = %+v", m)
	}
	if rows, err := s.PendingOutbox(10); err != nil || len(rows) != 1 {
		t.Fatalf("event missing: %v %+v", err, rows)
	}
	// дедуп: повторный коммит того же цикла ничего не добавляет
	if err := s.ApplyCycle(checks, events); err != nil {
		t.Fatal(err)
	}
	if rows, _ := s.PendingOutbox(10); len(rows) != 1 {
		t.Fatalf("dedupe broken: %+v", rows)
	}
	// провал на записи события откатывает и состояния: следующие циклы
	// переоткроют переходы от прежнего состояния, ничего не теряется
	if _, err := s.db.Exec(`DROP TABLE notification_outbox`); err != nil {
		t.Fatal(err)
	}
	checks["m"] = Check{ModelID: "m", At: at.Add(time.Minute), HTTPStatus: 200, Status: status.Operational}
	failEvents := []OutboxEvent{{ChatID: 100, Kind: "single", DedupeKey: "c2", Payload: []byte(`{}`)}}
	if err := s.ApplyCycle(checks, failEvents); err == nil {
		t.Fatal("ApplyCycle must fail without outbox table")
	}
	m, err = s.Model("m")
	if err != nil {
		t.Fatal(err)
	}
	if m.LastStatus != "down" {
		t.Fatalf("failed commit must roll states back: %+v", m)
	}
}

// DeferOutboxAttempt уважает retry_after и считается в attempts.
func TestOutboxDeferAttempt(t *testing.T) {
	s := openTest(t)
	if err := s.EnqueueOutbox(100, "single", "k", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	rows, err := s.PendingOutbox(10)
	if err != nil || len(rows) != 1 {
		t.Fatal(err)
	}
	if rows[0].NextAttemptAt != nil {
		t.Fatalf("fresh row must not be deferred: %+v", rows[0])
	}
	if err := s.DeferOutboxAttempt(rows[0].ID, 60*time.Second); err != nil {
		t.Fatal(err)
	}
	rows, err = s.PendingOutbox(10)
	if err != nil || len(rows) != 1 {
		t.Fatal(err)
	}
	if rows[0].NextAttemptAt == nil || !rows[0].NextAttemptAt.After(time.Now().Add(30*time.Second)) {
		t.Fatalf("deferred row must respect retry_after: %+v", rows[0])
	}
	cnt, err := s.PendingOutboxCount()
	if err != nil || cnt != 1 {
		t.Fatalf("count = %d, %v", cnt, err)
	}
}

// Дайджест-запросы: устаревший чат, все его строки, отметка всего чата сразу.
func TestStaleOutboxChatHelpers(t *testing.T) {
	s := openTest(t)
	for _, k := range []string{"k1", "k2", "k3"} {
		if err := s.EnqueueOutbox(100, "single", k, []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.EnqueueOutbox(200, "single", "k4", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE notification_outbox SET created_at = ? WHERE chat_id = 100`,
		fmtTime(time.Now().Add(-time.Hour))); err != nil {
		t.Fatal(err)
	}
	chats, err := s.StaleOutboxChats(30 * time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(chats) != 1 || chats[0] != 100 {
		t.Fatalf("stale chats = %v", chats)
	}
	rows, err := s.PendingOutboxChat(100)
	if err != nil || len(rows) != 3 {
		t.Fatalf("chat rows = %+v", rows)
	}
	if rows[0].CreatedAt.IsZero() {
		t.Fatalf("CreatedAt must be scanned: %+v", rows[0])
	}
	ids := make([]int64, len(rows))
	for i := range rows {
		ids[i] = rows[i].ID
	}
	n, err := s.MarkOutboxSentByIDs(ids)
	if err != nil || n != 3 {
		t.Fatalf("mark digest rows by ids = %d, %v", n, err)
	}
	cnt, _ := s.PendingOutboxCount()
	if cnt != 1 {
		t.Fatalf("digest must leave fresh chats pending, got %d", cnt)
	}
	// D1: помечаются только заданные ID — чужие строки чата остаются pending.
	if err := s.EnqueueOutbox(100, "single", "k9", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	rows, _ = s.PendingOutboxChat(100)
	if len(rows) != 1 {
		t.Fatalf("late row must stay pending: %+v", rows)
	}
	if n, err := s.MarkOutboxSentByIDs(ids); err != nil || n != 0 {
		t.Fatalf("re-marking already-sent ids must affect nothing: %d, %v", n, err)
	}
	rows, _ = s.PendingOutboxChat(100)
	if len(rows) != 1 {
		t.Fatalf("late row must survive marking of old ids: %+v", rows)
	}
	if n, err := s.MarkOutboxSentByIDs(nil); err != nil || n != 0 {
		t.Fatalf("empty ids must be a no-op: %d, %v", n, err)
	}
}

// Атомарная замена подписок чата.
func TestReplaceSubscriptions(t *testing.T) {
	s := openTest(t)
	if err := s.ReplaceSubscriptions(100, []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	if err := s.ReplaceSubscriptions(100, []string{"b", "c"}); err != nil {
		t.Fatal(err)
	}
	subs, err := s.Subscriptions(100)
	if err != nil {
		t.Fatal(err)
	}
	if len(subs) != 2 || subs[0] != "b" || subs[1] != "c" {
		t.Fatalf("replace must be exact, got %v", subs)
	}
	if err := s.ReplaceSubscriptions(100, nil); err != nil {
		t.Fatal(err)
	}
	if subs, _ = s.Subscriptions(100); len(subs) != 0 {
		t.Fatalf("empty replace must clear, got %v", subs)
	}
}

// Инциденты старше 30 дней удаляются, включая открытые.
func TestDeleteOldIncidents(t *testing.T) {
	s := openTest(t)
	old := time.Now().Add(-31 * 24 * time.Hour)
	if err := s.OpenIncident("m1", "operational", old); err != nil {
		t.Fatal(err)
	}
	if err := s.OpenIncident("m2", "operational", time.Now()); err != nil {
		t.Fatal(err)
	}
	n, err := s.DeleteOldIncidents(30 * 24 * time.Hour)
	if err != nil || n != 1 {
		t.Fatalf("DeleteOldIncidents = %d, %v", n, err)
	}
	if open, _ := s.HasOpenIncident("m1"); open {
		t.Fatal("stale incident must be deleted")
	}
	if open, _ := s.HasOpenIncident("m2"); !open {
		t.Fatal("fresh incident must stay")
	}
}

// Оффсет getUpdates переживает рестарт.
func TestBotOffset(t *testing.T) {
	s := openTest(t)
	if off, err := s.BotOffset(); err != nil || off != 0 {
		t.Fatalf("default offset = %d, %v", off, err)
	}
	if err := s.SetBotOffset(42); err != nil {
		t.Fatal(err)
	}
	if err := s.SetBotOffset(43); err != nil {
		t.Fatal(err)
	}
	if off, err := s.BotOffset(); err != nil || off != 43 {
		t.Fatalf("offset = %d, %v", off, err)
	}
}

// D3: операции инцидентов выполняются в транзакции цикла. Открытие
// идемпотентно (повтор цикла после сбоя не дублирует), закрытие по ID
// проваливается целиком с откатом состояний, если инцидент уже закрыт.
func TestApplyCycleIncidentOps(t *testing.T) {
	s := openTest(t)
	if err := s.SyncModels([]Model{{ID: "m", Family: "A"}}); err != nil {
		t.Fatal(err)
	}
	at := time.Now()
	op := IncidentOp{ModelID: "m", Open: true, OpenFrom: status.Down, OpenAt: at}
	down := map[string]Check{"m": {ModelID: "m", At: at, HTTPStatus: 503, Status: status.Down}}
	for i := 0; i < 2; i++ { // коммит + redo той же операции
		if err := s.ApplyCycle(down, nil, op); err != nil {
			t.Fatal(err)
		}
	}
	var open int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM incidents WHERE model_id = 'm' AND ended_at IS NULL`).Scan(&open); err != nil || open != 1 {
		t.Fatalf("open incidents = %d, %v — idempotent open", open, err)
	}
	cur, err := s.CurrentIncident("m")
	if err != nil || cur == nil || cur.From != status.Down {
		t.Fatalf("current incident = %+v, %v", cur, err)
	}
	if cur.Started.IsZero() {
		t.Fatalf("started_at must scan: %+v", cur)
	}
	// закрытие в одной транзакции с состоянием
	recovered := map[string]Check{"m": {ModelID: "m", At: at.Add(time.Minute), HTTPStatus: 200, Status: status.Operational}}
	if err := s.ApplyCycle(recovered, nil, IncidentOp{ModelID: "m", CloseID: cur.ID, CloseTo: status.Operational, CloseAt: at.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	m, _ := s.Model("m")
	if m.LastStatus != "operational" {
		t.Fatalf("recovery must commit, got %+v", m)
	}
	if open, _ := s.HasOpenIncident("m"); open {
		t.Fatal("close op must close the incident")
	}
	// конфликт: закрывать уже нечего — цикл целиком откатывается
	fell := map[string]Check{"m": {ModelID: "m", At: at.Add(2 * time.Minute), HTTPStatus: 503, Status: status.Down}}
	if err := s.ApplyCycle(fell, nil, IncidentOp{ModelID: "m", CloseID: cur.ID, CloseTo: status.Down, CloseAt: at.Add(2 * time.Minute)}); err == nil {
		t.Fatal("closing an already-closed incident must fail the cycle")
	}
	m, _ = s.Model("m")
	if m.LastStatus != "operational" {
		t.Fatalf("failed incident op must roll the cycle back: %+v", m)
	}
}

// Этап 2.5: SyncModels удаляет подписки на модели, покинувшие конфигурацию,
// и закрывает открытые инциденты удалённых моделей (2.3) одной транзакцией.
func TestSyncModelsCleansOrphanSubscriptionsAndIncidents(t *testing.T) {
	s := openTest(t)
	if err := s.SyncModels([]Model{{ID: "model-a", Family: "A"}, {ID: "model-b", Family: "B"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetSubscription(100, "model-a", true); err != nil {
		t.Fatal(err)
	}
	if err := s.SetSubscription(100, "model-b", true); err != nil {
		t.Fatal(err)
	}
	if err := s.SetSubscription(101, "model-a", true); err != nil {
		t.Fatal(err)
	}
	if err := s.OpenIncident("model-a", status.Down, time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}

	// model-a покидает конфигурацию
	if err := s.SyncModels([]Model{{ID: "model-b", Family: "B"}}); err != nil {
		t.Fatal(err)
	}
	var subsA []string
	if subs, err := s.Subscriptions(100); err != nil {
		t.Fatal(err)
	} else {
		subsA = subs
	}
	if len(subsA) != 1 || subsA[0] != "model-b" {
		t.Fatalf("subscription to removed model must be dropped: %v", subsA)
	}
	subs101, err := s.Subscriptions(101)
	if err != nil {
		t.Fatal(err)
	}
	if len(subs101) != 0 {
		t.Fatalf("all subscriptions of removed model must be dropped: %v", subs101)
	}
	// инцидент удалённой модели закрыт (не остаётся открытым навсегда)
	if open, err := s.HasOpenIncident("model-a"); err != nil || open {
		t.Fatalf("open incident must be closed when the model leaves the config: %v %v", open, err)
	}
}
