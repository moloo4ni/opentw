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

// chat_notified: set/get, перезапись, отсутствие строки, чистка по чату.
func TestChatNotifiedStateLifecycle(t *testing.T) {
	s := openTest(t)
	if _, ok, err := s.ChatNotifiedState(100, "m"); err != nil || ok {
		t.Fatalf("no row must give ok=false, got ok=%v err=%v", ok, err)
	}
	if _, ok, err := s.ChatNotifiedState(100, "other"); ok || err != nil {
		t.Fatalf("unknown model still ok=false, got ok=%v err=%v", ok, err)
	}
	if err := s.SetChatNotifiedState(100, "m", status.Down); err != nil {
		t.Fatal(err)
	}
	if err := s.SetChatNotifiedState(100, "m2", status.Operational); err != nil {
		t.Fatal(err)
	}
	if err := s.SetChatNotifiedState(200, "m", status.Operational); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := s.ChatNotifiedState(100, "m"); err != nil || !ok || got != status.Down {
		t.Fatalf("state(100,m) = %v %v %v, want down", got, ok, err)
	}
	// перезапись: последнее состояние важнее предыдущего
	if err := s.SetChatNotifiedState(100, "m", status.Operational); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := s.ChatNotifiedState(100, "m"); err != nil || !ok || got != status.Operational {
		t.Fatalf("overwrite = %v %v %v, want operational", got, ok, err)
	}
	// удаление — только строки одного чата
	if err := s.DeleteChatNotifiedState(100); err != nil {
		t.Fatal(err)
	}
	for _, pair := range []struct {
		chat  int64
		model string
	}{{100, "m"}, {100, "m2"}} {
		if _, ok, err := s.ChatNotifiedState(pair.chat, pair.model); err != nil || ok {
			t.Fatalf("after delete (%d,%s) ok must be false, got %v %v", pair.chat, pair.model, ok, err)
		}
	}
	if got, ok, err := s.ChatNotifiedState(200, "m"); err != nil || !ok || got != status.Operational {
		t.Fatalf("delete-by-chat must not touch other chats: %v %v %v", got, ok, err)
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

// CommitDelivery: пометки строк и уведомлённые состояния фиксируются одним
// вызовом (одной транзакцией); D1 — помечаются только заданные ID, поздние
// строки чата остаются pending и доезжают отдельно.
func TestCommitDelivery(t *testing.T) {
	s := openTest(t)
	for _, k := range []string{"k1", "k2", "k3"} {
		if err := s.EnqueueOutbox(100, "single", k, []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.EnqueueOutbox(200, "single", "k4", []byte(`{}`)); err != nil {
		t.Fatal(err)
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
	// коммит без уведомлённых состояний — только пометки строк (частичная
	// доставка: старые ID отдают свои строки, поздние остаются)
	if err := s.CommitDelivery(100, nil, ids[:2]); err != nil {
		t.Fatal(err)
	}
	cnt, _ := s.PendingOutboxCount()
	if cnt != 2 { // k3 чата 100 и k4 чата 200
		t.Fatalf("one of three rows must stay pending, got %d", cnt)
	}
	// D1: поздняя строка чата не помечается старыми ID и доедет отдельно
	if err := s.EnqueueOutbox(100, "single", "k9", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitDelivery(100, map[string]status.Status{"m1": status.Down}, ids); err != nil {
		t.Fatal(err)
	}
	rows, _ = s.PendingOutboxChat(100)
	if len(rows) != 1 || rows[0].DedupeKey != "k9" {
		t.Fatalf("late row must stay pending: %+v", rows)
	}
	st, ok, err := s.ChatNotifiedState(100, "m1")
	if err != nil || !ok || st != status.Down {
		t.Fatalf("notified state must be committed: %q %v %v", st, ok, err)
	}
	// повторный коммит тех же ID — пометки идемпотентны, состояние обновляется
	if err := s.CommitDelivery(100, map[string]status.Status{"m1": status.Operational}, ids); err != nil {
		t.Fatal(err)
	}
	if st, ok, _ := s.ChatNotifiedState(100, "m1"); !ok || st != status.Operational {
		t.Fatalf("notified state must update: %q %v", st, ok)
	}
	if rows, _ = s.PendingOutboxChat(100); len(rows) != 1 {
		t.Fatalf("re-marking already-sent ids must affect nothing: %+v", rows)
	}
	// пустые ids — коммит только уведомлённых состояний
	if err := s.CommitDelivery(100, nil, nil); err != nil {
		t.Fatal(err)
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

// Этап 6.3: PendingOutboxDue — строгий FIFO внутри чата и честная выборка:
// только головная (старейшая) строка каждого чата и только созревшая
// (next_attempt_at); отложенная голова не выпускает более новые строки
// своего чата, но не блокирует чужие чаты.
func TestPendingOutboxDue(t *testing.T) {
	s := openTest(t)
	// чат 100: три строки; чат 200: одна
	for _, k := range []string{"k1", "k2", "k3"} {
		if err := s.EnqueueOutbox(100, "single", k, []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.EnqueueOutbox(200, "single", "k4", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}

	rows, err := s.PendingOutboxDue(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].ChatID != 100 || rows[1].ChatID != 200 {
		t.Fatalf("heads of both chats expected, got %+v", rows)
	}
	if rows[0].DedupeKey != "k1" {
		t.Fatalf("chat head must be the oldest row, got %+v", rows[0])
	}

	// голова чата 100 отложена: чат не участвует в выборке, чат 200 — участвует
	if err := s.DeferOutboxAttempt(rows[0].ID, time.Hour); err != nil {
		t.Fatal(err)
	}
	rows, err = s.PendingOutboxDue(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ChatID != 200 {
		t.Fatalf("deferred head must hold back its chat only, got %+v", rows)
	}

	// созревание (next_attempt_at в прошлом) возвращает голову
	if err := s.DeferOutboxAttempt(rows[0].ID, -2*time.Hour); err != nil { // и чат 200 заодно отложен в прошлое
		t.Fatal(err)
	}
	if err := s.DeferOutboxAttempt(1, -2*time.Hour); err != nil {
		t.Fatal(err)
	}
	rows, err = s.PendingOutboxDue(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("both matured heads expected, got %+v", rows)
	}

	// доставка головы открывает следующую строку того же чата
	if err := s.MarkOutboxSent(rows[0].ID); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkOutboxSent(rows[1].ID); err != nil {
		t.Fatal(err)
	}
	rows, err = s.PendingOutboxDue(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ChatID != 100 || rows[0].DedupeKey != "k2" {
		t.Fatalf("next head of chat 100 expected, got %+v", rows)
	}

	// количество попыток доступно для расчёта backoff (6.1)
	if err := s.BumpOutboxAttempt(rows[0].ID); err != nil {
		t.Fatal(err)
	}
	rows, err = s.PendingOutboxDue(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Attempts != 1 {
		t.Fatalf("attempts must be readable, got %+v", rows[0])
	}
}
