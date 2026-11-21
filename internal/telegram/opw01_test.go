package telegram

// Регрессионные тесты дефекта OPW-01: уведомление о падении терялось навсегда,
// когда между созданием события и доставкой статус модели становился Unknown.
// Delivery помечало строку отправленной (без сообщения и без chat_notified),
// а последующий Unknown→Down при живом инциденте не создавал нового события.
//
// Все сценарии гоняются через реальные методы компонентов: monitor.RunCycle с
// scriptedUpstream (реальная классификация, инциденты, ApplyCycle), реальная
// доставка deliverChat и настоящая SQLite-база. Проверяется фактическое
// состояние БД (outbox, chat_notified) и реально ушедшие сообщения.

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"opentw/internal/api"
	"opentw/internal/monitor"
	"opentw/internal/status"
	"opentw/internal/storage"
)

// scriptedUpstream — детерминированный upstream-провайдер для реального
// раннера: ListModels всегда жив (нет гашения по catalogIssue), Generate
// отдаёт назначенный результат модели.
type scriptedUpstream struct {
	mu      sync.Mutex
	results map[string]*api.RawResult
}

func newScriptedUpstream(ids ...string) *scriptedUpstream {
	u := &scriptedUpstream{results: map[string]*api.RawResult{}}
	for _, id := range ids {
		u.setUp(id)
	}
	return u
}

func (u *scriptedUpstream) ListModels(ctx context.Context) ([]string, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make([]string, 0, len(u.results))
	for id := range u.results {
		out = append(out, id)
	}
	return out, nil
}

func (u *scriptedUpstream) Generate(ctx context.Context, modelID string, maxTokens int) *api.RawResult {
	u.mu.Lock()
	defer u.mu.Unlock()
	if r := u.results[modelID]; r != nil {
		return r
	}
	return &api.RawResult{ModelID: modelID, Timestamp: time.Now(), ErrorKind: "invalid", ErrorMessage: "not scripted", Response: "not scripted", LatencyUS: 1_000}
}

func (u *scriptedUpstream) set(id string, r *api.RawResult) {
	u.mu.Lock()
	u.results[id] = r
	u.mu.Unlock()
}

func (u *scriptedUpstream) setUp(id string)   { u.set(id, upstreamUp(id)) }
func (u *scriptedUpstream) setDown(id string) { u.set(id, upstreamDown(id)) }
func (u *scriptedUpstream) setUnknown(id string) {
	// 401 — проблема ключа самого монитора: Classify даёт Unknown,
	// каталог при этом жив, гашения по catalogIssue нет.
	u.set(id, &api.RawResult{ModelID: id, Timestamp: time.Now(), HTTPStatus: 401,
		ErrorKind: "http", ErrorMessage: "Unauthorized", Response: "Unauthorized", LatencyUS: 20_000})
}

func upstreamUp(id string) *api.RawResult {
	return &api.RawResult{
		ModelID: id, Timestamp: time.Now(), HTTPStatus: 200,
		LatencyUS: 1_000_000, TTFTUS: ptr1(300_000), GenUS: ptr1(700_000),
		TPS: ptrF(40), InputTokens: ptr1(10), OutputTokens: ptr1(8),
		Content: "OK", Response: "OK", Streamed: true,
	}
}

func upstreamDown(id string) *api.RawResult {
	return &api.RawResult{
		ModelID: id, Timestamp: time.Now(), HTTPStatus: 503,
		ErrorKind: "http", ErrorMessage: "Service Unavailable",
		Response: "Service Unavailable", LatencyUS: 40_000,
	}
}

// newOPWBot собирает боевую связку: реальный раннер мониторинга + реальный
// бот доставки на одной SQLite-базе (как в cmd/opentw/main.go).
func newOPWBot(t *testing.T, m *mockTG, u *scriptedUpstream, models []storage.Model) (*Bot, *storage.Store, *monitor.Runner) {
	t.Helper()
	store, err := storage.Open(t.TempDir() + "/bot.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	if err := store.SyncModels(models); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := NewBot(BotCfg{
		API:    &Client{Token: "T", BaseURL: m.url, HTTP: &http.Client{Timeout: 5 * time.Second}},
		Store:  store,
		Models: models,
		Logger: log,
	})
	r := monitor.NewRunner(monitor.RunnerCfg{
		API:         u,
		Store:       store,
		Models:      models,
		Concurrency: 2,
		MaxTokens:   8,
		Interval:    15 * time.Minute,
		OnCycle: func(res monitor.CycleResult) ([]storage.OutboxEvent, error) {
			return b.OnCycle(context.Background(), res)
		},
		Logger: log,
	})
	return b, store, r
}

func mustCycle(t *testing.T, r *monitor.Runner) *monitor.CycleResult {
	t.Helper()
	res, err := r.RunCycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func pendingRows(t *testing.T, store *storage.Store) []storage.OutboxRow {
	t.Helper()
	rows, err := store.PendingOutbox(100)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func mustNotified(t *testing.T, store *storage.Store, chatID int64, modelID string, want status.Status) {
	t.Helper()
	st, ok, err := store.ChatNotifiedState(chatID, modelID)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || st != want {
		t.Fatalf("chat_notified(%d, %s) = %q ok=%v, want %q", chatID, modelID, st, ok, want)
	}
}

// seedNotifiedUp доводит пару «чат—модель» до штатного состояния: last_status=Up,
// chat_notified=Up (реально доставленным уведомлением о восстановлении),
// очередь пуста, открытых инцидентов нет.
func seedNotifiedUp(t *testing.T, m *mockTG, u *scriptedUpstream, b *Bot, r *monitor.Runner, chatID int64, modelIDs ...string) {
	t.Helper()
	for _, id := range modelIDs {
		u.setDown(id)
	}
	mustCycle(t, r) // первое наблюдение Down — базлайн, события нет
	for _, id := range modelIDs {
		u.setUp(id)
	}
	mustCycle(t, r) // восстановление: событие для всех подписанных чатов
	b.deliveryTick(context.Background())
	if n := m.callCount("sendRichMessage"); n == 0 {
		t.Fatal("seed: recovery notification must be delivered")
	}
	if rows := pendingRows(t, b.store); len(rows) != 0 {
		t.Fatalf("seed: queue must be empty: %+v", rows)
	}
	for _, id := range modelIDs {
		mustNotified(t, b.store, chatID, id, status.Operational)
	}
	m.resetCalls()
}

// Основной сценарий OPW-01 (§6): подписка → chat_notified=Up → монитор
// фиксирует Down (событие в outbox) → доставка задерживается → следующий
// цикл даёт Unknown → delivery выполняется (тишина) → цикл снова даёт Down
// (живой инцидент подавляет новое событие) → delivery выполняется →
// подписчик получает уведомление о падении.
// На исходной реализации тест падает: первый delivery помечал событие sent
// при Unknown, второго события не существовало — подписчик не узнавал о Down.
func TestOPW01UnknownBetweenCyclesKeepsDownNotification(t *testing.T) {
	m := newMockTG(t)
	u := newScriptedUpstream("gpt-6-astra")
	b, store, r := newOPWBot(t, m, u, []storage.Model{{ID: "gpt-6-astra", Family: "GPT"}})
	ctx := context.Background()
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	seedNotifiedUp(t, m, u, b, r, 100, "gpt-6-astra")

	// цикл 1: Up → Down, событие ложится в outbox
	u.setDown("gpt-6-astra")
	mustCycle(t, r)
	rows := pendingRows(t, store)
	if len(rows) != 1 {
		t.Fatalf("down event must be queued exactly once: %+v", rows)
	}
	downRowID := rows[0].ID

	// цикл 2: Unknown (доставка так и не состоялась — бэклог/недоступность)
	u.setUnknown("gpt-6-astra")
	mustCycle(t, r)
	if rows := pendingRows(t, store); len(rows) != 1 {
		t.Fatalf("unknown cycle must not touch the queue: %+v", rows)
	}

	// delivery при cur=Unknown: тишина, но свидетельство обязано выжить
	b.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n != 0 {
		t.Fatalf("cur Unknown must stay silent, got %d sends", n)
	}
	rows = pendingRows(t, store)
	if len(rows) != 1 || rows[0].ID != downRowID {
		t.Fatalf("undelivered down evidence must survive the unknown tick: %+v", rows)
	}
	if rows[0].NextAttemptAt == nil {
		t.Fatal("stuck head must be parked with a short retry, not hot-looped")
	}
	if rows[0].Attempts != 0 {
		t.Fatalf("state uncertainty is not a delivery failure: attempts = %d", rows[0].Attempts)
	}
	mustNotified(t, store, 100, "gpt-6-astra", status.Operational)

	// цикл 3: снова Down — живой инцидент подавляет повторное событие
	u.setDown("gpt-6-astra")
	res := mustCycle(t, r)
	if len(res.Transitions) != 0 {
		t.Fatalf("live incident must suppress a duplicate transition: %+v", res.Transitions)
	}

	// delivery: уведомление о падении доезжает
	if err := store.DeferOutboxRetry(downRowID, -time.Hour); err != nil {
		t.Fatal(err)
	}
	b.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("subscriber must receive the down notification, got %d sends", n)
	}
	html := lastRichHTML(t, m, "sendRichMessage")
	if !strings.Contains(html, "<p>🔴 <code>gpt-6-astra</code>") {
		t.Fatalf("down line expected:\n%s", html)
	}
	if rows := pendingRows(t, store); len(rows) != 0 {
		t.Fatalf("queue must drain after delivery: %+v", rows)
	}
	mustNotified(t, store, 100, "gpt-6-astra", status.Down)

	// сходимость: дальнейшие Down-циклы не плодят строки
	mustCycle(t, r)
	mustCycle(t, r)
	if rows := pendingRows(t, store); len(rows) != 0 {
		t.Fatalf("converged pair must not queue rows: %+v", rows)
	}
}

// TTL (§6): Unknown длится дольше TTL outbox — исходная строка истекла
// и удалена, но актуальное уведомление о Down всё равно доходит: сверка
// chat_notified ↔ last_status вооружается самостоятельно. Мусор в очереди
// при этом не накапливается. Вторая фаза — то же для потерянного
// восстановления.
func TestOPW01TTLExpiryStillNotifies(t *testing.T) {
	m := newMockTG(t)
	u := newScriptedUpstream("gpt-6-astra")
	b, store, r := newOPWBot(t, m, u, []storage.Model{{ID: "gpt-6-astra", Family: "GPT"}})
	ctx := context.Background()
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	seedNotifiedUp(t, m, u, b, r, 100, "gpt-6-astra")

	// падение → Unknown → тихий delivery (строка припаркована) → TTL истёк
	u.setDown("gpt-6-astra")
	mustCycle(t, r)
	u.setUnknown("gpt-6-astra")
	mustCycle(t, r)
	b.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n != 0 {
		t.Fatalf("unknown must stay silent, got %d sends", n)
	}
	if _, err := store.DropStaleOutbox(time.Nanosecond); err != nil { // исходная запись истекла
		t.Fatal(err)
	}
	if rows := pendingRows(t, store); len(rows) != 0 {
		t.Fatalf("expired row must be gone: %+v", rows)
	}

	// длительный Unknown: сверка не вооружается (нет достоверного
	// состояния), очередь не растёт
	mustCycle(t, r)
	mustCycle(t, r)
	if rows := pendingRows(t, store); len(rows) != 0 {
		t.Fatalf("unknown must not grow the queue: %+v", rows)
	}

	// модель снова Down: состояние фиксируется, событие подавлено живым
	// инцидентом — но сверочная строка вооружается со следующего цикла
	u.setDown("gpt-6-astra")
	mustCycle(t, r)
	if rows := pendingRows(t, store); len(rows) != 0 {
		t.Fatalf("fresh divergence arms one cycle later: %+v", rows)
	}
	mustCycle(t, r)
	rows := pendingRows(t, store)
	if len(rows) != 1 || rows[0].Kind != "reconcile" {
		t.Fatalf("exactly one reconcile row expected: %+v", rows)
	}

	// доставка: пользователь всё равно получает актуальное уведомление
	b.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("down must reach the subscriber after TTL expiry, got %d sends", n)
	}
	if html := lastRichHTML(t, m, "sendRichMessage"); !strings.Contains(html, "<p>🔴 <code>gpt-6-astra</code>") {
		t.Fatalf("down line expected:\n%s", html)
	}
	mustNotified(t, store, 100, "gpt-6-astra", status.Down)

	// сходимость: сверка не плодит строки, пока состояние совпадает
	mustCycle(t, r)
	mustCycle(t, r)
	if rows := pendingRows(t, store); len(rows) != 0 {
		t.Fatalf("no garbage after convergence: %+v", rows)
	}

	// фаза 2: потерянное ВОССТАНОВЛЕНИЕ тоже самовосстанавливается
	u.setUnknown("gpt-6-astra")
	mustCycle(t, r)
	u.setUp("gpt-6-astra")
	mustCycle(t, r)                                                   // Unknown→Up: событие восстановления создано…
	if _, err := store.DropStaleOutbox(time.Nanosecond); err != nil { // …и истекло недоставленным
		t.Fatal(err)
	}
	mustCycle(t, r) // сверка: last=Up ≠ notified=Down
	rows = pendingRows(t, store)
	if len(rows) != 1 || rows[0].Kind != "reconcile" {
		t.Fatalf("recovery reconcile row expected: %+v", rows)
	}
	b.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n != 2 {
		t.Fatalf("recovery must reach the subscriber after TTL expiry, got %d sends", n)
	}
	if html := lastRichHTML(t, m, "sendRichMessage"); !strings.Contains(html, "<p>🟢 <code>gpt-6-astra</code> - Восстановилась</p>") {
		t.Fatalf("recovery line expected:\n%s", html)
	}
	mustNotified(t, store, 100, "gpt-6-astra", status.Operational)
}

// FIFO и изоляция моделей (§6): A=Unknown с ожидающим событием, B=Down —
// оба в одном подписанном чате. B доставляется своевременно, свидетельство
// A сохраняется и доезжает, когда A получает достоверный статус.
func TestOPW01UnknownModelDoesNotBlockChatPeer(t *testing.T) {
	m := newMockTG(t)
	u := newScriptedUpstream("a", "b")
	b, store, r := newOPWBot(t, m, u, []storage.Model{{ID: "a", Family: "A"}, {ID: "b", Family: "B"}})
	ctx := context.Background()
	for _, id := range []string{"a", "b"} {
		if err := store.SetSubscription(100, id, true); err != nil {
			t.Fatal(err)
		}
	}
	seedNotifiedUp(t, m, u, b, r, 100, "a", "b")

	// A падает, затем B падает (разные циклы — разные строки)
	u.setDown("a")
	mustCycle(t, r)
	u.setDown("b")
	mustCycle(t, r)
	// A теряет достоверность
	u.setUnknown("a")
	mustCycle(t, r)
	if rows := pendingRows(t, store); len(rows) != 2 { // событие A, событие B; сверка не дублирует живое свидетельство
		t.Fatalf("queue = %+v", rows)
	}

	// delivery: B уезжает, A остаётся
	b.deliveryTick(ctx)
	bodies := m.callBodies("sendRichMessage")
	if len(bodies) != 1 {
		t.Fatalf("B must be delivered on time, got %d sends", len(bodies))
	}
	if html := richHTML(t, parseBody(t, bodies[0])); !strings.Contains(html, "<code>b</code>") || strings.Contains(html, "<code>a</code>") {
		t.Fatalf("first message must carry only B's line:\n%s", html)
	}
	rows := pendingRows(t, store)
	if len(rows) != 1 || rows[0].Attempts != 0 || rows[0].NextAttemptAt == nil {
		t.Fatalf("A's evidence must stay pending, parked, not failed: %+v", rows)
	}
	mustNotified(t, store, 100, "a", status.Operational) // A не тронуто
	mustNotified(t, store, 100, "b", status.Down)        // B доставлено

	// A получает достоверный Down — его уведомление доезжает
	u.setDown("a")
	mustCycle(t, r)
	if rows := pendingRows(t, store); len(rows) != 1 {
		t.Fatalf("converged B and suppressed A must add nothing: %+v", rows)
	}
	if err := store.DeferOutboxRetry(rows[0].ID, -time.Hour); err != nil {
		t.Fatal(err)
	}
	b.deliveryTick(ctx)
	bodies = m.callBodies("sendRichMessage")
	if len(bodies) != 2 {
		t.Fatalf("A must be delivered after certainty returns, got %d sends", len(bodies))
	}
	if html := richHTML(t, parseBody(t, bodies[1])); !strings.Contains(html, "<code>a</code>") || strings.Contains(html, "<code>b</code>") {
		t.Fatalf("second message must carry only A's line:\n%s", html)
	}
	if rows := pendingRows(t, store); len(rows) != 0 {
		t.Fatalf("queue must drain: %+v", rows)
	}
	mustNotified(t, store, 100, "a", status.Down)
	mustNotified(t, store, 100, "b", status.Down)
}

// Сворачивание актуального состояния (§6): Up→Down→Unknown→Up при
// notified=Up — устаревшего уведомления о Down не приходит.
func TestOPW01DownUnknownUpSendsNoStaleHistory(t *testing.T) {
	m := newMockTG(t)
	u := newScriptedUpstream("gpt-6-astra")
	b, store, r := newOPWBot(t, m, u, []storage.Model{{ID: "gpt-6-astra", Family: "GPT"}})
	ctx := context.Background()
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	seedNotifiedUp(t, m, u, b, r, 100, "gpt-6-astra")

	u.setDown("gpt-6-astra")
	mustCycle(t, r)
	u.setUnknown("gpt-6-astra")
	mustCycle(t, r)
	u.setUp("gpt-6-astra")
	mustCycle(t, r) // Down→Unknown→Up: падение и восстановление по дороге

	b.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n != 0 {
		t.Fatalf("cur == notified — устаревшая история не отправляется, got %d sends", n)
	}
	if rows := pendingRows(t, store); len(rows) != 0 {
		t.Fatalf("rows must resolve without a message: %+v", rows)
	}
	mustNotified(t, store, 100, "gpt-6-astra", status.Operational)
}

// Последовательности нескольких циклов (§6).
func TestOPW01SequenceDownUnknownUnknownDown(t *testing.T) {
	m := newMockTG(t)
	u := newScriptedUpstream("gpt-6-astra")
	b, store, r := newOPWBot(t, m, u, []storage.Model{{ID: "gpt-6-astra", Family: "GPT"}})
	ctx := context.Background()
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	seedNotifiedUp(t, m, u, b, r, 100, "gpt-6-astra")

	u.setDown("gpt-6-astra")
	mustCycle(t, r)
	u.setUnknown("gpt-6-astra")
	mustCycle(t, r)
	mustCycle(t, r)
	u.setDown("gpt-6-astra")
	mustCycle(t, r)

	b.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("one actual down line expected, got %d sends", n)
	}
	if html := lastRichHTML(t, m, "sendRichMessage"); strings.Count(html, "<p>") != 1 || !strings.Contains(html, "<p>🔴 <code>gpt-6-astra</code>") {
		t.Fatalf("exactly one down line expected:\n%s", html)
	}
	if rows := pendingRows(t, store); len(rows) != 0 {
		t.Fatalf("queue must drain: %+v", rows)
	}
	mustNotified(t, store, 100, "gpt-6-astra", status.Down)
}

func TestOPW01SequenceDownUnknownUpDown(t *testing.T) {
	m := newMockTG(t)
	u := newScriptedUpstream("gpt-6-astra")
	b, store, r := newOPWBot(t, m, u, []storage.Model{{ID: "gpt-6-astra", Family: "GPT"}})
	ctx := context.Background()
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	seedNotifiedUp(t, m, u, b, r, 100, "gpt-6-astra")

	for _, st := range []func(string){u.setDown, u.setUnknown, u.setUp, u.setDown} {
		st("gpt-6-astra")
		mustCycle(t, r)
	}

	b.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("final state must collapse into one line, got %d sends", n)
	}
	if html := lastRichHTML(t, m, "sendRichMessage"); strings.Count(html, "<p>") != 1 || !strings.Contains(html, "<p>🔴 <code>gpt-6-astra</code>") {
		t.Fatalf("exactly one down line expected:\n%s", html)
	}
	if rows := pendingRows(t, store); len(rows) != 0 {
		t.Fatalf("queue must drain: %+v", rows)
	}
	mustNotified(t, store, 100, "gpt-6-astra", status.Down)
}

func TestOPW01SequenceDownUnknownUp(t *testing.T) {
	m := newMockTG(t)
	u := newScriptedUpstream("gpt-6-astra")
	b, store, r := newOPWBot(t, m, u, []storage.Model{{ID: "gpt-6-astra", Family: "GPT"}})
	ctx := context.Background()
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	seedNotifiedUp(t, m, u, b, r, 100, "gpt-6-astra")

	u.setDown("gpt-6-astra")
	mustCycle(t, r)
	u.setUnknown("gpt-6-astra")
	mustCycle(t, r)
	u.setUp("gpt-6-astra")
	mustCycle(t, r)

	b.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n != 0 {
		t.Fatalf("model fell and recovered back to the notified state — silence, got %d sends", n)
	}
	if rows := pendingRows(t, store); len(rows) != 0 {
		t.Fatalf("queue must drain: %+v", rows)
	}
	mustNotified(t, store, 100, "gpt-6-astra", status.Operational)
}

// Если Down был успешно доставлен, последующее Unknown→Up рождает
// уведомление о восстановлении (инвариант «в итоге дойти»).
func TestOPW01RecoveryAfterDeliveredDownStillArrives(t *testing.T) {
	m := newMockTG(t)
	u := newScriptedUpstream("gpt-6-astra")
	b, store, r := newOPWBot(t, m, u, []storage.Model{{ID: "gpt-6-astra", Family: "GPT"}})
	ctx := context.Background()
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	seedNotifiedUp(t, m, u, b, r, 100, "gpt-6-astra")

	u.setDown("gpt-6-astra")
	mustCycle(t, r)
	b.deliveryTick(ctx) // 🔴 доставлено
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("down must be delivered, got %d sends", n)
	}
	mustNotified(t, store, 100, "gpt-6-astra", status.Down)
	m.resetCalls()

	u.setUnknown("gpt-6-astra")
	mustCycle(t, r)
	u.setUp("gpt-6-astra")
	mustCycle(t, r)
	b.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("recovery must arrive, got %d sends", n)
	}
	if html := lastRichHTML(t, m, "sendRichMessage"); !strings.Contains(html, "<p>🟢 <code>gpt-6-astra</code> - Восстановилась</p>") {
		t.Fatalf("recovery line expected:\n%s", html)
	}
	mustNotified(t, store, 100, "gpt-6-astra", status.Operational)
}

// Повторная доставка (§6): Telegram подтвердил отправку, но фиксация в
// SQLite упала — свидетельство и chat_notified не «фиктивно доставлены»;
// повторный delivery доезжает дублем (допустимый at-least-once) и
// фиксирует корректное состояние.
func TestOPW01CommitFailureKeepsEvidenceAndRedelivers(t *testing.T) {
	m := newMockTG(t)
	path := t.TempDir() + "/bot.db"
	models := []storage.Model{{ID: "gpt-6-astra", Family: "GPT"}}
	b, store := newBotAt(t, m, path, models)
	ctx := context.Background()
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	if err := store.SetChatNotifiedState(100, "gpt-6-astra", status.Operational); err != nil {
		t.Fatal(err)
	}
	// падение → событие; Unknown: строка переживает тихий delivery
	checks := downChecks("gpt-6-astra", 503, "http")
	seedCheck(t, b, checks)
	commitCycle(t, b, cycle(checks, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	if err := store.SetModelState("gpt-6-astra", status.Unknown, "invalid", "unparseable", time.Now()); err != nil {
		t.Fatal(err)
	}
	b.deliveryTick(ctx)
	if rows := pendingRows(t, store); len(rows) != 1 {
		t.Fatalf("evidence must survive the unknown tick: %+v", rows)
	}
	// свидетельство истекло (TTL) → сверочная строка вооружается
	if err := store.SetModelState("gpt-6-astra", status.Down, "http", "HTTP 503", time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DropStaleOutbox(time.Nanosecond); err != nil {
		t.Fatal(err)
	}
	commitCycle(t, b, cycle(downChecks("gpt-6-astra", 503, "http")))
	rows := pendingRows(t, store)
	if len(rows) != 1 || rows[0].Kind != "reconcile" {
		t.Fatalf("reconcile row must replace the expired evidence: %+v", rows)
	}

	// отправка успешна, коммит — нет: SQLite закрыт прямо во время send
	m.handler = func(method string, body []byte) (any, int) {
		if method == "sendRichMessage" {
			_ = store.Close()
			return map[string]any{"ok": true, "result": map[string]any{"message_id": m.nextMessageID()}}, http.StatusOK
		}
		return map[string]any{"ok": true, "result": true}, http.StatusOK
	}
	b.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("one send must happen, got %d", n)
	}

	// рестарт на том же файле: ничего не зафиксировано — ни пометок,
	// ни уведомлённого состояния
	b2, store2 := newBotAt(t, m, path, models)
	if rows := pendingRows(t, store2); len(rows) != 1 {
		t.Fatalf("failed commit must leave the row pending: %+v", rows)
	}
	mustNotified(t, store2, 100, "gpt-6-astra", status.Operational)

	// повторный delivery: дубль (at-least-once) и корректная фиксация
	m.handler = nil
	b2.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n < 2 {
		t.Fatalf("at-least-once: a duplicate send is expected, got %d", n)
	}
	if rows := pendingRows(t, store2); len(rows) != 0 {
		t.Fatalf("rows must drain on retry: %+v", rows)
	}
	mustNotified(t, store2, 100, "gpt-6-astra", status.Down)
}

// Несколько чатов (§6): подписанный и неподписанный чаты, успешная доставка
// и ошибка в другом — состояния чатов независимы.
func TestOPW01MultiChatIsolation(t *testing.T) {
	m := newMockTG(t)
	u := newScriptedUpstream("gpt-6-astra")
	b, store, r := newOPWBot(t, m, u, []storage.Model{{ID: "gpt-6-astra", Family: "GPT"}})
	ctx := context.Background()
	for _, chat := range []int64{100, 200} {
		if err := store.SetSubscription(chat, "gpt-6-astra", true); err != nil {
			t.Fatal(err)
		}
	}
	seedNotifiedUp(t, m, u, b, r, 100, "gpt-6-astra")
	mustNotified(t, store, 200, "gpt-6-astra", status.Operational)
	m.resetCalls()

	// падение: строки только подписанным чатам
	u.setDown("gpt-6-astra")
	mustCycle(t, r)
	rows := pendingRows(t, store)
	if len(rows) != 2 {
		t.Fatalf("two subscribed chats must hold rows: %+v", rows)
	}
	for _, row := range rows {
		if row.ChatID != 100 && row.ChatID != 200 {
			t.Fatalf("unsubscribed chat must not receive rows: %+v", row)
		}
	}

	// чат 200 errorит: чат 100 доставляется независимо от порядка обработки
	fail200 := true
	m.handler = func(method string, body []byte) (any, int) {
		if method == "sendRichMessage" && fail200 {
			var p map[string]any
			if err := json.Unmarshal(body, &p); err == nil {
				if chatID, _ := p["chat_id"].(float64); int64(chatID) == 200 {
					return map[string]any{"ok": false, "error_code": 500, "description": "Internal Server Error"}, http.StatusInternalServerError
				}
			}
		}
		return map[string]any{"ok": true, "result": map[string]any{"message_id": m.nextMessageID()}}, http.StatusOK
	}
	b.deliveryTick(ctx)
	mustNotified(t, store, 100, "gpt-6-astra", status.Down) // успех одного чата
	rows = pendingRows(t, store)
	if len(rows) != 1 || rows[0].ChatID != 200 {
		t.Fatalf("only the failed chat's row must stay: %+v", rows)
	}
	mustNotified(t, store, 200, "gpt-6-astra", status.Operational) // ошибка не двигает chat_notified
	c100, c200 := 0, 0
	for _, c := range m.sentChatIDs("sendRichMessage") {
		switch c {
		case 100:
			c100++
		case 200:
			c200++
		}
	}
	if c100 != 1 {
		t.Fatalf("chat 100 must be delivered exactly once, got %d", c100)
	}
	if c200 < 1 {
		t.Fatalf("chat 200 must be attempted, got %d", c200)
	}

	// чат 200 оживает: его уведомление доезжает, чат 100 не дёргается
	fail200 = false
	m.handler = nil
	m.resetCalls()
	if err := store.DeferOutboxAttempt(rows[0].ID, -time.Hour); err != nil {
		t.Fatal(err)
	}
	b.deliveryTick(ctx)
	chats := m.sentChatIDs("sendRichMessage")
	if len(chats) != 1 || chats[0] != 200 {
		t.Fatalf("only chat 200 must receive the retry, got %v", chats)
	}
	mustNotified(t, store, 200, "gpt-6-astra", status.Down)
	if rows := pendingRows(t, store); len(rows) != 0 {
		t.Fatalf("queue must drain: %+v", rows)
	}
}

// parseBody — тело вызова как объект (чтобы читалось одной строкой).
func parseBody(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var p map[string]any
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatal(err)
	}
	return p
}

// Ревью P2-3а: grouped-строка с ДВУМЯ моделями, одна из которых Unknown.
// Строка гасится только целиком (частичная пометка невозможна), но свёртка
// не удерживает достоверную модель: строки достоверных моделей уходят
// сразу (chat_notified фиксирует только реально отправленное), строка
// остаётся pending, пока все её модели не станут достоверными.
func TestOPW01MixedRowWaitsForAllItsModels(t *testing.T) {
	m := newMockTG(t)
	u := newScriptedUpstream("a", "b")
	b, store, r := newOPWBot(t, m, u, []storage.Model{{ID: "a", Family: "A"}, {ID: "b", Family: "B"}})
	ctx := context.Background()
	for _, id := range []string{"a", "b"} {
		if err := store.SetSubscription(100, id, true); err != nil {
			t.Fatal(err)
		}
	}
	seedNotifiedUp(t, m, u, b, r, 100, "a", "b")

	// падение ОБЕИХ моделей одним циклом: одна строка с двумя переходами
	u.setDown("a")
	u.setDown("b")
	mustCycle(t, r)
	rows := pendingRows(t, store)
	if len(rows) != 1 {
		t.Fatalf("one grouped row expected: %+v", rows)
	}
	// a теряет достоверность
	u.setUnknown("a")
	mustCycle(t, r)

	// delivery: строка b уходит (достоверна), сама строка НЕ гасится —
	// свидетельство об a живо
	b.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("b's line must be delivered on time, got %d sends", n)
	}
	if html := lastRichHTML(t, m, "sendRichMessage"); strings.Count(html, "<p>") != 1 || !strings.Contains(html, "<code>b</code>") || strings.Contains(html, "<code>a</code>") {
		t.Fatalf("only b's line expected:\n%s", html)
	}
	rows = pendingRows(t, store)
	if len(rows) != 1 || rows[0].Attempts != 0 || rows[0].NextAttemptAt == nil {
		t.Fatalf("mixed row must stay pending, parked: %+v", rows)
	}
	mustNotified(t, store, 100, "a", status.Operational) // a не тронуто
	mustNotified(t, store, 100, "b", status.Down)        // b реально доставлено

	// повторный тик при том же состоянии: без дублей, без спама
	b.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("no duplicate for the parked row, got %d sends", n)
	}

	// обе модели достоверны: строка a уходит, строка гасится целиком
	u.setDown("a")
	mustCycle(t, r)
	if rows := pendingRows(t, store); len(rows) != 1 {
		t.Fatalf("suppressed transition must not add rows: %+v", rows)
	}
	if err := store.DeferOutboxRetry(rows[0].ID, -time.Hour); err != nil {
		t.Fatal(err)
	}
	b.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n != 2 {
		t.Fatalf("one collapsed message expected, got %d sends", n)
	}
	html := lastRichHTML(t, m, "sendRichMessage")
	if strings.Count(html, "<p>") != 1 || !strings.Contains(html, "<code>a</code>") || strings.Contains(html, "<code>b</code>") {
		t.Fatalf("only a's line expected (b already notified):\n%s", html)
	}
	if rows := pendingRows(t, store); len(rows) != 0 {
		t.Fatalf("queue must drain: %+v", rows)
	}
	mustNotified(t, store, 100, "a", status.Down)
	mustNotified(t, store, 100, "b", status.Down)
}

// Ревью P2-3б: сверка перевооружается в НОВОМ процессе: последний коммит
// старого процесса зафиксировал last=Down при notified=Up, исходная строка
// истекла по TTL, рестарт — первый же цикл нового процесса вооружает сверку.
func TestOPW01ReconcileArmsAfterRestart(t *testing.T) {
	m := newMockTG(t)
	path := t.TempDir() + "/bot.db"
	models := []storage.Model{{ID: "gpt-6-astra", Family: "GPT"}}
	b, store := newBotAt(t, m, path, models)
	ctx := context.Background()
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	if err := store.SetChatNotifiedState(100, "gpt-6-astra", status.Operational); err != nil {
		t.Fatal(err)
	}
	// старый процесс: падение зафиксировано, строка событий истекла по TTL
	checks := downChecks("gpt-6-astra", 503, "http")
	seedCheck(t, b, checks)
	commitCycle(t, b, cycle(checks, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	if _, err := store.DropStaleOutbox(time.Nanosecond); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// новый процесс: первый цикл вооружает сверку (событие подавлено инцидентом)
	b2, store2 := newBotAt(t, m, path, models)
	commitCycle(t, b2, cycle(downChecks("gpt-6-astra", 503, "http")))
	rows := pendingRows(t, store2)
	if len(rows) != 1 || rows[0].Kind != "reconcile" {
		t.Fatalf("reconcile must arm in the new process: %+v", rows)
	}
	b2.deliveryTick(ctx)
	if n := m.callCount("sendRichMessage"); n != 1 {
		t.Fatalf("down must reach the subscriber after restart, got %d sends", n)
	}
	if html := lastRichHTML(t, m, "sendRichMessage"); !strings.Contains(html, "<p>🔴 <code>gpt-6-astra</code>") {
		t.Fatalf("down line expected:\n%s", html)
	}
	if rows := pendingRows(t, store2); len(rows) != 0 {
		t.Fatalf("queue must drain: %+v", rows)
	}
	mustNotified(t, store2, 100, "gpt-6-astra", status.Down)
}
