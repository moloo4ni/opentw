package telegram

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"net/http"

	"opentw/internal/api"
	"opentw/internal/monitor"
	"opentw/internal/status"
	"opentw/internal/storage"
)

// OnCycle больше не ходит в сеть: событие ложится в outbox (D3).
func TestOnCycleDoesNotSendDirectly(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	commitCycle(t, b, cycle(nil, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	if n := m.callCount("sendMessage"); n != 0 {
		t.Fatalf("OnCycle must not send, got %d calls", n)
	}
	rows, err := store.PendingOutbox(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ChatID != 100 {
		t.Fatalf("outbox must hold one row for chat 100: %+v", rows)
	}
}

// Массовый сбой при живом каталоге: один чат — одно сообщение, в котором
// только его подписки (AC-14).
func TestGroupedMassOutageOneMessage(t *testing.T) {
	m := newMockTG(t)
	models := []storage.Model{
		{ID: "a", Family: "A"},
		{ID: "b", Family: "A"},
		{ID: "c", Family: "B"},
		{ID: "d", Family: "B"},
	}
	b, store := newBotAt(t, m, t.TempDir()+"/bot.db", models)
	if err := store.SetSubscription(100, "a", true); err != nil {
		t.Fatal(err)
	}
	if err := store.SetSubscription(100, "b", true); err != nil {
		t.Fatal(err)
	}
	trs := []monitor.Transition{
		transition("a", status.Operational, status.Down, time.Now(), nil),
		transition("b", status.Operational, status.Down, time.Now(), nil),
		transition("c", status.Operational, status.Down, time.Now(), nil), // не подписан
	}
	commitCycle(t, b, cycle(nil, trs...))
	b.deliveryTick(context.Background())

	if n := m.callCount("sendMessage"); n != 1 {
		t.Fatalf("one message expected, got %d", n)
	}
	text := fmt.Sprint(m.lastBody("sendMessage")["text"])
	for _, want := range []string{
		"🔴 Массовый сбой: 3 из 4 моделей недоступны",
		"<code>a</code> - HTTP 503",
		"<code>b</code> - HTTP 503",
		"Проверено: ",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("mass message missing %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "<code>c</code>") {
		t.Errorf("unsubscribed model leaked into chat message:\n%s", text)
	}
}

// Массовое восстановление: список с длительностями, только подписки (AC-13).
func TestGroupedRecovery(t *testing.T) {
	m := newMockTG(t)
	models := []storage.Model{
		{ID: "a", Family: "A"}, {ID: "b", Family: "A"}, {ID: "c", Family: "B"},
		{ID: "d", Family: "B"}, {ID: "e", Family: "B"},
	}
	b, store := newBotAt(t, m, t.TempDir()+"/bot.db", models)
	for _, id := range []string{"a", "b", "d"} {
		if err := store.SetSubscription(100, id, true); err != nil {
			t.Fatal(err)
		}
	}
	trs := []monitor.Transition{
		transition("a", status.Down, status.Operational, time.Now(), &storage.Incident{From: status.Down, To: status.Operational, Duration: 90 * time.Minute}),
		transition("b", status.Down, status.Operational, time.Now(), &storage.Incident{From: status.Down, To: status.Operational, Duration: 30 * time.Minute}),
		transition("d", status.Down, status.Operational, time.Now(), &storage.Incident{From: status.Down, To: status.Operational, Duration: 15 * time.Minute}),
		transition("e", status.Down, status.Operational, time.Now(), nil), // не подписан
	}
	commitCycle(t, b, cycle(nil, trs...))
	b.deliveryTick(context.Background())

	if n := m.callCount("sendMessage"); n != 1 {
		t.Fatalf("one message expected, got %d", n)
	}
	text := fmt.Sprint(m.lastBody("sendMessage")["text"])
	if !strings.Contains(text, "🟢 Восстановились: 3 модели") {
		t.Fatalf("header wrong:\n%s", text)
	}
	if !strings.Contains(text, "<code>a</code> - сбой длился 1ч 30м") {
		t.Fatalf("duration line missing:\n%s", text)
	}
	if !strings.Contains(text, "<code>b</code> - сбой длился 30м") {
		t.Fatalf("duration line missing:\n%s", text)
	}
	if strings.Contains(text, "<code>e</code>") {
		t.Fatalf("unsubscribed leaked:\n%s", text)
	}
}

// Локальный случай: меньше половины упало — «Недоступны», не «Массовый».
func TestLocalTwoDownOneMessage(t *testing.T) {
	m := newMockTG(t)
	models := []storage.Model{
		{ID: "a", Family: "A"}, {ID: "b", Family: "A"}, {ID: "c", Family: "B"},
		{ID: "d", Family: "B"}, {ID: "e", Family: "B"},
	}
	b, store := newBotAt(t, m, t.TempDir()+"/bot.db", models)
	if err := store.SetSubscription(100, "a", true); err != nil {
		t.Fatal(err)
	}
	if err := store.SetSubscription(100, "b", true); err != nil {
		t.Fatal(err)
	}
	trs := []monitor.Transition{
		transition("a", status.Operational, status.Down, time.Now(), nil),
		transition("b", status.Operational, status.Down, time.Now(), nil),
	}
	commitCycle(t, b, cycle(nil, trs...))
	b.deliveryTick(context.Background())
	if n := m.callCount("sendMessage"); n != 1 {
		t.Fatalf("one message expected, got %d", n)
	}
	text := fmt.Sprint(m.lastBody("sendMessage")["text"])
	if !strings.Contains(text, "🔴 Недоступны: 2 модели") {
		t.Fatalf("header wrong:\n%s", text)
	}
	if strings.Contains(text, "Массовый") {
		t.Fatalf("2 of 5 is not a mass outage:\n%s", text)
	}
}

// Единственное событие за цикл — одиночный формат (AC-15).
func TestSingleTransitionStaysSingle(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	commitCycle(t, b, cycle(nil, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	b.deliveryTick(context.Background())
	if n := m.callCount("sendMessage"); n != 1 {
		t.Fatalf("one message expected, got %d", n)
	}
	text := fmt.Sprint(m.lastBody("sendMessage")["text"])
	if !strings.Contains(text, "🔴 <code>gpt-6-astra</code> - недоступна") {
		t.Fatalf("single format expected:\n%s", text)
	}
	if strings.Contains(text, "Массовый") || strings.Contains(text, "Недоступны:") {
		t.Fatalf("single event must not use group headers:\n%s", text)
	}
}

// Список свёрнут до 15 строк + «... ещё N» (лимит 4096).
func TestGroupedLineCap(t *testing.T) {
	m := newMockTG(t)
	models := make([]storage.Model, 0, 20)
	for i := 0; i < 20; i++ {
		models = append(models, storage.Model{ID: fmt.Sprintf("m%02d", i), Family: "F"})
	}
	b, store := newBotAt(t, m, t.TempDir()+"/bot.db", models)
	trs := make([]monitor.Transition, 0, 20)
	for i := 0; i < 20; i++ {
		if err := store.SetSubscription(100, models[i].ID, true); err != nil {
			t.Fatal(err)
		}
		trs = append(trs, transition(models[i].ID, status.Operational, status.Down, time.Now(), nil))
	}
	commitCycle(t, b, cycle(nil, trs...))
	b.deliveryTick(context.Background())
	if n := m.callCount("sendMessage"); n != 1 {
		t.Fatalf("one message expected, got %d", n)
	}
	text := fmt.Sprint(m.lastBody("sendMessage")["text"])
	if strings.Contains(text, "<code>m15</code>") {
		t.Fatalf("list must be capped at 15 lines:\n%s", text)
	}
	if !strings.Contains(text, "ещё 5") {
		t.Fatalf("overflow counter missing:\n%s", text)
	}
	if l := len([]rune(text)); l > 4096 {
		t.Fatalf("message too long: %d", l)
	}
}

// Повторный OnCycle с тем же временем цикла не дублирует строку (идемпотентность).
func TestOutboxDedupeSameCycle(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	res := cycle(nil, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil))
	commitCycle(t, b, res)
	commitCycle(t, b, res)
	rows, _ := store.PendingOutbox(10)
	if len(rows) != 1 {
		t.Fatalf("dedupe failed: %+v", rows)
	}
	b.deliveryTick(context.Background())
	b.deliveryTick(context.Background())
	if n := m.callCount("sendMessage"); n != 1 {
		t.Fatalf("message must be sent once, got %d", n)
	}
}

// Событие переживает рестарт процесса: тот же файл БД, новый Bot.
func TestOutboxSurvivesRestart(t *testing.T) {
	m := newMockTG(t)
	path := t.TempDir() + "/bot.db"
	b1, store1 := newBotAt(t, m, path, []storage.Model{{ID: "gpt-6-astra", Family: "GPT"}, {ID: "glm-5.3", Family: "GLM"}})
	if err := store1.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	commitCycle(t, b1, cycle(nil, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	if err := store1.Close(); err != nil {
		t.Fatal(err)
	}

	b2, _ := newBotAt(t, m, path, []storage.Model{{ID: "gpt-6-astra", Family: "GPT"}, {ID: "glm-5.3", Family: "GLM"}})
	b2.deliveryTick(context.Background())
	if n := m.callCount("sendMessage"); n != 1 {
		t.Fatalf("notification must survive restart, got %d sends", n)
	}
	text := fmt.Sprint(m.lastBody("sendMessage")["text"])
	if !strings.Contains(text, "недоступна") {
		t.Fatalf("text = %q", text)
	}
	b2.deliveryTick(context.Background())
	if n := m.callCount("sendMessage"); n != 1 {
		t.Fatalf("delivered row must not resend, got %d", n)
	}
}

// Временный сбой Telegram: строка остаётся, ретраится не раньше backoff
// (мгновенного перевзвешивания больше нет, 6.1) и доезжает после созревания.
func TestTelegramTransientFailurePostpones(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	var fail atomic.Bool
	fail.Store(true)
	m.handler = func(method string, body []byte) (any, int) {
		if method == "sendMessage" && fail.Load() {
			return map[string]any{"ok": false, "error_code": 500, "description": "Internal Server Error"}, http.StatusInternalServerError
		}
		return nil, http.StatusOK // дефолт ниже не нужен: вернём ok
	}
	commitCycle(t, b, cycle(nil, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	b.deliveryTick(context.Background())
	if n := m.callCount("sendMessage"); n == 0 {
		t.Fatal("send attempt expected")
	}
	rows, _ := store.PendingOutbox(10)
	if len(rows) != 1 {
		t.Fatalf("failed row must stay pending: %+v", rows)
	}
	// до истечения backoff — никакого повторного запроса
	fail.Store(false)
	m.handler = nil
	b.deliveryTick(context.Background())
	if n := m.callCount("sendMessage"); n != 1 {
		t.Fatalf("row must not be retried before the backoff elapses, sends = %d", n)
	}
	// срок настал — строка доезжает
	if err := store.DeferOutboxAttempt(rows[0].ID, -time.Hour); err != nil {
		t.Fatal(err)
	}
	b.deliveryTick(context.Background())
	if n := m.callCount("sendMessage"); n < 2 {
		t.Fatalf("row must be retried after maturity, sends = %d", n)
	}
	rows, _ = store.PendingOutbox(10)
	if len(rows) != 0 {
		t.Fatalf("delivered row must be marked: %+v", rows)
	}
}

// 429 исчерпал ретраи — событие не теряется, доезжает после нормализации.
func Test429ExhaustionKeepsRow(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	var limited atomic.Bool
	limited.Store(true)
	m.handler = func(method string, body []byte) (any, int) {
		if method == "sendMessage" && limited.Load() {
			return map[string]any{"ok": false, "error_code": 429, "description": "Too Many Requests",
				"parameters": map[string]any{"retry_after": 0}}, http.StatusTooManyRequests
		}
		return nil, http.StatusOK
	}
	commitCycle(t, b, cycle(nil, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	b.deliveryTick(context.Background())
	rows, _ := store.PendingOutbox(10)
	if len(rows) != 1 {
		t.Fatalf("row must survive 429 exhaustion: %+v", rows)
	}
	if rows[0].ID == 0 {
		t.Fatal("row must have id")
	}
	if rows[0].NextAttemptAt == nil {
		t.Fatal("429 must defer the row until retry_after elapses (P1-4)")
	}
	limited.Store(false)
	m.handler = nil
	// повтор не раньше retry_after (floor 1с) и после снятия паузы батча
	time.Sleep(time.Until(*rows[0].NextAttemptAt) + 20*time.Millisecond)
	b.floodUntil.Store(0)
	b.deliveryTick(context.Background())
	rows, _ = store.PendingOutbox(10)
	if len(rows) != 0 {
		t.Fatalf("row must be delivered after rate limit lifted: %+v", rows)
	}
}

// Завершение во время рассылки: недоставленное остаётся в БД.
func TestDeliveryShutdownKeepsPending(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	blocking := make(chan struct{})
	m.handler = func(method string, body []byte) (any, int) {
		if method == "sendMessage" {
			<-blocking
		}
		return nil, http.StatusOK
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.RunDelivery(ctx) }()
	commitCycle(t, b, cycle(nil, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	deadline := time.Now().Add(2 * time.Second)
	for m.callCount("sendMessage") == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunDelivery must exit on ctx cancel")
	}
	close(blocking)
	rows, _ := store.PendingOutbox(10)
	if len(rows) != 1 {
		t.Fatalf("undelivered notification must persist: %+v", rows)
	}
}

// Панели обновляются воркером из LastChecks: неполный цикл не затирает
// известное состояние «нет данных» (D1).
func TestPanelsSyncedInWorkerFromLastChecks(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	ctx := context.Background()
	if err := b.HandleUpdate(ctx, msgUpdate(100, 1, "private", "/status")); err != nil {
		t.Fatal(err)
	}
	at := time.Now()
	if err := store.InsertCheck(storage.Check{ModelID: "gpt-6-astra", At: at, Status: status.Operational, HTTPStatus: 200, LatencyUS: ptr1(2_000_000)}); err != nil {
		t.Fatal(err)
	}
	if err := store.InsertCheck(storage.Check{ModelID: "glm-5.3", At: at, Status: status.Operational, HTTPStatus: 200, LatencyUS: ptr1(2_000_000)}); err != nil {
		t.Fatal(err)
	}
	// «цикл», в котором проверка glm-5.3 отменена: в res.Checks её нет
	commitCycle(t, b, cycle(map[string]storage.Check{
		"gpt-6-astra": {ModelID: "gpt-6-astra", At: at, Status: status.Operational, HTTPStatus: 200, LatencyUS: ptr1(2_000_000)},
	}))
	b.deliveryTick(ctx)
	if n := m.callCount("editMessageText"); n != 1 {
		t.Fatalf("panel edit expected, got %d", n)
	}
	text := fmt.Sprint(m.lastBody("editMessageText")["text"])
	if !strings.Contains(text, "🟢 <code>glm-5.3</code> - 2.0s") {
		t.Fatalf("known state must survive a partial cycle (D1):\n%s", text)
	}
	if strings.Contains(text, " - нет данных") {
		t.Fatalf("no-data must not replace a known state:\n%s", text)
	}
}

// Текст ошибки в уведомлении ограничен 500 рунами (страховка лимита 4096).
func TestNotificationErrorTextCapped(t *testing.T) {
	tr := transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)
	tr.Check.ErrorMessage = strings.Repeat("z", 600)
	text := RenderNotification(tr)
	if strings.Count(text, "z") > 500 {
		t.Fatalf("error text must be capped, got %d z's", strings.Count(text, "z"))
	}
}

// P1-2: устаревший backlog сворачивается в один дайджест на чат — итоговые
// состояния и честные счётчики вместо лавины противоречивых сообщений.
func TestStaleBacklogCompactedToDigest(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	for _, id := range []string{"gpt-6-astra", "glm-5.3"} {
		if err := store.SetSubscription(100, id, true); err != nil {
			t.Fatal(err)
		}
	}
	b.backlogStale = time.Nanosecond // в тесте всё устарело немедленно
	// три «цикла» недоступности: glm падала и восстановилась, gpt упал и держится
	commitCycle(t, b, cycle(nil, transition("glm-5.3", status.Operational, status.Down, time.Now().Add(-40*time.Minute), nil)))
	commitCycle(t, b, cycle(nil, transition("glm-5.3", status.Down, status.Operational, time.Now().Add(-25*time.Minute),
		&storage.Incident{From: status.Down, To: status.Operational, Duration: 15 * time.Minute})))
	commitCycle(t, b, cycle(nil, transition("gpt-6-astra", status.Operational, status.Down, time.Now().Add(-10*time.Minute), nil)))
	b.deliveryTick(context.Background())

	if n := m.callCount("sendMessage"); n != 1 {
		t.Fatalf("stale backlog must be one digest, got %d messages", n)
	}
	text := fmt.Sprint(m.lastBody("sendMessage")["text"])
	for _, want := range []string{
		"📬 Пока Telegram был недоступен: 3 события",
		"🔴 Сейчас недоступны (1):",
		"<code>gpt-6-astra</code> - недоступна",
		"🟢 Восстановились (1):",
		"<code>glm-5.3</code> - сбой длился 15м",
		"Проверено: ",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("digest missing %q:\n%s", want, text)
		}
	}
	if l := len([]rune(text)); l > 4096 {
		t.Fatalf("digest too long: %d", l)
	}
	if rows, _ := store.PendingOutbox(10); len(rows) != 0 {
		t.Fatalf("digest must mark all chat rows sent: %+v", rows)
	}
	// после дайджеста свежие события идут штатно, по одному сообщению
	b.backlogStale = 0
	commitCycle(t, b, cycle(nil, transition("glm-5.3", status.Operational, status.Down, time.Now(), nil)))
	b.deliveryTick(context.Background())
	text = fmt.Sprint(m.lastBody("sendMessage")["text"])
	if !strings.Contains(text, "недоступна") || strings.Contains(text, "📬") {
		t.Fatalf("fresh row must be a regular notification:\n%s", text)
	}
	if n := m.callCount("sendMessage"); n != 2 {
		t.Fatalf("fresh rows must be delivered one by one, got %d", n)
	}
}

// P1-2: неудавшийся дайджест повторяется следующим тиком, строки не теряются.
func TestDigestRetriedWhenSendFails(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	b.backlogStale = time.Nanosecond
	commitCycle(t, b, cycle(nil, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	m.handler = func(method string, body []byte) (any, int) {
		return map[string]any{"ok": false, "error_code": 500, "description": "Internal Server Error"}, http.StatusInternalServerError
	}
	b.deliveryTick(context.Background())
	if rows, _ := store.PendingOutbox(10); len(rows) != 1 {
		t.Fatalf("failed digest must keep rows pending: %+v", rows)
	}
	m.handler = nil
	b.deliveryTick(context.Background())
	if rows, _ := store.PendingOutbox(10); len(rows) != 0 {
		t.Fatalf("digest must be retried and delivered: %+v", rows)
	}
	if n := m.callCount("sendMessage"); n != 2 {
		t.Fatalf("digest must be attempted twice, got %d", n)
	}
}

// P1-3/P1-4: глобальный 429 останавливает весь батч, а не только строку —
// флуд-лимит бота общий, и повтор строки не раньше retry_after.
func Test429PausesWholeBatch(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	if err := store.SetSubscription(200, "glm-5.3", true); err != nil {
		t.Fatal(err)
	}
	commitCycle(t, b, cycle(nil, transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	commitCycle(t, b, cycle(nil, transition("glm-5.3", status.Operational, status.Down, time.Now(), nil)))
	var limited atomic.Bool
	limited.Store(true)
	m.handler = func(method string, body []byte) (any, int) {
		if method == "sendMessage" && limited.Load() {
			return map[string]any{"ok": false, "error_code": 429, "description": "Too Many Requests",
				"parameters": map[string]any{"retry_after": 0}}, http.StatusTooManyRequests
		}
		return nil, http.StatusOK
	}
	b.deliveryTick(context.Background())
	limited.Store(false)
	m.handler = nil
	// строка 1: 3 запроса (in-request ретраи), затем пауза батча; строка 2 не тронута
	if n := m.callCount("sendMessage"); n != maxRetries+1 {
		t.Fatalf("only the first row may be attempted, got %d", n)
	}
	rows, _ := store.PendingOutbox(10)
	if len(rows) != 2 {
		t.Fatalf("both rows must stay queued: %+v", rows)
	}
	if rows[0].NextAttemptAt == nil {
		t.Fatal("429 must defer the failed row (P1-4)")
	}
	// тик во время паузы ничего не отправляет
	b.deliveryTick(context.Background())
	if n := m.callCount("sendMessage"); n != maxRetries+1 {
		t.Fatalf("flood pause must hold the batch, got %d sends", n)
	}
	// пауза снята: отложенная строка ещё ждёт retry_after, свежая — уходит
	b.floodUntil.Store(0)
	b.deliveryTick(context.Background())
	if n := m.callCount("sendMessage"); n != maxRetries+2 {
		t.Fatalf("fresh row must be delivered while deferred one waits, got %d sends", n)
	}
	if rows, _ = store.PendingOutbox(10); len(rows) != 1 || rows[0].ChatID != 100 {
		t.Fatalf("only the deferred row may remain: %+v", rows)
	}
}

// D1: гонка дайджеста. ApplyCycle коммитит новое событие тому же чату ВО
// ВРЕМЯ отправки дайджеста: строка не входила в отправленный текст — она
// обязана остаться pending и доехать отдельным сообщением, а не быть
// помеченной отправленной вместе с backlog.
func TestDigestRaceNewRowStaysPending(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	for _, id := range []string{"gpt-6-astra", "glm-5.3"} {
		if err := store.SetSubscription(100, id, true); err != nil {
			t.Fatal(err)
		}
	}
	b.backlogStale = time.Nanosecond // в тесте всё устарело немедленно
	// старый backlog из двух событий
	commitCycle(t, b, cycle(nil, transition("gpt-6-astra", status.Operational, status.Down, time.Now().Add(-40*time.Minute), nil)))
	commitCycle(t, b, cycle(nil, transition("glm-5.3", status.Down, status.Operational, time.Now().Add(-30*time.Minute),
		&storage.Incident{From: status.Down, To: status.Operational, Duration: 15 * time.Minute})))

	var sent []string
	pendingAtSeparateSend := -1
	m.handler = func(method string, body []byte) (any, int) {
		if method != "sendMessage" {
			return map[string]any{"ok": true, "result": true}, http.StatusOK
		}
		var p map[string]any
		json.Unmarshal(body, &p)
		if len(sent) == 0 {
			// дайджест в полёте: цикл коммитит новое событие того же чата
			commitCycle(t, b, cycle(nil, transition("gpt-6-astra", status.Down, status.Operational, time.Now(), nil)))
		} else if pendingAtSeparateSend < 0 {
			// момент отправки отдельного сообщения: новая строка всё ещё pending
			rows, _ := store.PendingOutbox(10)
			pendingAtSeparateSend = len(rows)
		}
		sent = append(sent, fmt.Sprint(p["text"]))
		return map[string]any{"ok": true, "result": map[string]any{"message_id": m.nextMessageID()}}, http.StatusOK
	}
	b.deliveryTick(context.Background())

	if n := m.callCount("sendMessage"); n != 2 {
		t.Fatalf("digest + separate delivery expected, got %d sends", n)
	}
	if len(sent) != 2 {
		t.Fatalf("sent = %v", sent)
	}
	// первое сообщение — дайджест старого backlog, нового события в нём нет
	if !strings.Contains(sent[0], "📬") || !strings.Contains(sent[0], "<code>gpt-6-astra</code> - недоступна") {
		t.Fatalf("first message must be the digest of the old backlog:\n%s", sent[0])
	}
	if strings.Contains(sent[0], "<code>gpt-6-astra</code> - сбой длился") {
		t.Fatalf("digest rendered before the commit must not contain the new event:\n%s", sent[0])
	}
	// второе — отдельное уведомление о новом событии, а не часть дайджеста
	if strings.Contains(sent[1], "📬") {
		t.Fatalf("new row must be delivered separately:\n%s", sent[1])
	}
	if !strings.Contains(sent[1], "🟢 <code>gpt-6-astra</code> - восстановилась") {
		t.Fatalf("separate notification text wrong:\n%s", sent[1])
	}
	if pendingAtSeparateSend != 1 {
		t.Fatalf("the new row must still be pending when its own send starts, got %d rows", pendingAtSeparateSend)
	}
	if rows, _ := store.PendingOutbox(10); len(rows) != 0 {
		t.Fatalf("everything must be delivered after the tick: %+v", rows)
	}
}

// scriptedChecker — минимальная замена api.Client для раннера в e2e-тесте.
type scriptedChecker struct {
	mu     sync.Mutex
	result *api.RawResult
}

func (c *scriptedChecker) ListModels(ctx context.Context) ([]string, error) {
	return []string{"gpt-6-astra"}, nil
}

func (c *scriptedChecker) Generate(ctx context.Context, modelID string, maxTokens int) *api.RawResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	res := *c.result
	res.ModelID = modelID
	return &res
}

// D2 end-to-end: 🟢 → слепота монитора (401 → ⚪) → модель упала за это
// время → API подтверждает 503. Подписчик получает событие о падении с
// честной пометкой о неизвестном времени начала, инцидент открывается;
// восстановление доходит с длительностью.
func TestBlindFallNotifiedEndToEnd(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	f := &scriptedChecker{result: &api.RawResult{ErrorKind: "", HTTPStatus: 200, LatencyUS: 900_000, TPS: ptrF(42), InputTokens: ptr1(10), OutputTokens: ptr1(8), Timestamp: time.Now()}}
	r := monitor.NewRunner(monitor.RunnerCfg{
		API: f, Store: store, Models: []storage.Model{{ID: "gpt-6-astra", Family: "GPT"}},
		Concurrency: 1, MaxTokens: 8, Interval: time.Minute,
		Perf: monitor.PerfCfg{MinSamples: 12, LatencyFactor: 3.0, TPSShare: 0.4},
		OnCycle: func(res monitor.CycleResult) ([]storage.OutboxEvent, error) {
			return b.OnCycle(context.Background(), res)
		},
	})
	setResult := func(kind string, httpStatus int, msg string, latUS int64) {
		f.mu.Lock()
		f.result = &api.RawResult{ErrorKind: kind, HTTPStatus: httpStatus, ErrorMessage: msg, LatencyUS: latUS, Timestamp: time.Now()}
		f.mu.Unlock()
	}

	if _, err := r.RunCycle(context.Background()); err != nil { // 🟢 базлайн
		t.Fatal(err)
	}
	b.deliveryTick(context.Background())
	if n := m.callCount("sendMessage"); n != 0 {
		t.Fatalf("baseline must not notify, got %d", n)
	}
	setResult("http", 401, "Invalid API key", 40_000)
	if _, err := r.RunCycle(context.Background()); err != nil { // ⚪ слепота
		t.Fatal(err)
	}
	b.deliveryTick(context.Background())
	if n := m.callCount("sendMessage"); n != 0 {
		t.Fatalf("blindness must not notify, got %d", n)
	}
	if open, _ := store.HasOpenIncident("gpt-6-astra"); open {
		t.Fatal("blindness must not open incidents")
	}
	setResult("http", 503, "Service Unavailable", 40_000)
	res, err := r.RunCycle(context.Background()) // зрение вернулось: падение
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Transitions) != 1 || res.Transitions[0].To != status.Down {
		t.Fatalf("confirmed fall must produce a subscriber event: %+v", res.Transitions)
	}
	if open, _ := store.HasOpenIncident("gpt-6-astra"); !open {
		t.Fatal("incident must be opened")
	}
	b.deliveryTick(context.Background())
	if n := m.callCount("sendMessage"); n != 1 {
		t.Fatalf("subscriber must learn about the fall, got %d sends", n)
	}
	text := fmt.Sprint(m.lastBody("sendMessage")["text"])
	for _, want := range []string{
		"🔴 <code>gpt-6-astra</code> - недоступна",
		"HTTP 503",
		"Service Unavailable",
		"Точное время начала недоступности неизвестно",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("fall notification missing %q:\n%s", want, text)
		}
	}
	// восстановление доходит с честной длительностью (от момента обнаружения)
	time.Sleep(10 * time.Millisecond)
	setResult("", 200, "", 900_000)
	if _, err := r.RunCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	b.deliveryTick(context.Background())
	if n := m.callCount("sendMessage"); n != 2 {
		t.Fatalf("recovery must be delivered, got %d sends", n)
	}
	text = fmt.Sprint(m.lastBody("sendMessage")["text"])
	if !strings.Contains(text, "🟢 <code>gpt-6-astra</code> - восстановилась") {
		t.Fatalf("recovery text wrong:\n%s", text)
	}
	if !strings.Contains(text, "Длительность сбоя: ") {
		t.Fatalf("recovery must carry the duration:\n%s", text)
	}
	if rows, _ := store.PendingOutbox(10); len(rows) != 0 {
		t.Fatalf("outbox must be drained: %+v", rows)
	}
}

// --- Этап 6.1/6.2/6.3/6.4 ---

// cycleAt — цикл с заданным временем (уникальные dedupe-ключи строк).
func cycleAt(at time.Time, trs ...monitor.Transition) monitor.CycleResult {
	return monitor.CycleResult{CycleAt: at, NextAt: at.Add(15 * time.Minute), Transitions: trs}
}

// firstPendingRow возвращает старейшую pending-строку (для манипуляций).
func firstPendingRow(t *testing.T, store *storage.Store) storage.OutboxRow {
	t.Helper()
	rows, err := store.PendingOutbox(10)
	if err != nil || len(rows) == 0 {
		t.Fatalf("pending row expected: %v %v", rows, err)
	}
	return rows[0]
}

// 6.1: канонический график нарастающего backoff: 30с, 1м, 2м, 4м, 8м,
// потолок 15м (и дальше — потолок).
func TestDeferBackoffSchedule(t *testing.T) {
	want := []time.Duration{30 * time.Second, 60 * time.Second, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 15 * time.Minute, 15 * time.Minute}
	for i, w := range want {
		if got := deferBackoff(i); got != w {
			t.Errorf("deferBackoff(%d) = %v, want %v", i, got, w)
		}
	}
}

// 6.1: временная ошибка доставки → нарастающий backoff, строка не удаляется
// и не ретраится раньше срока.
func TestTemporaryErrorBackoff(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	var fail atomic.Bool
	fail.Store(true)
	m.handler = func(method string, body []byte) (any, int) {
		if method == "sendMessage" && fail.Load() {
			return map[string]any{"ok": false, "error_code": 500, "description": "Internal Server Error"}, http.StatusInternalServerError
		}
		return map[string]any{"ok": true, "result": map[string]any{"message_id": m.nextMessageID()}}, http.StatusOK
	}
	commitCycle(t, b, cycleAt(time.Now(), transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))

	// первая неудача: пауза ~30с
	b.deliveryTick(context.Background())
	row := firstPendingRow(t, store)
	if row.NextAttemptAt == nil {
		t.Fatal("temporary failure must defer the row (backoff)")
	}
	d := time.Until(*row.NextAttemptAt)
	if d < 25*time.Second || d > 35*time.Second {
		t.Fatalf("first backoff = %v, want ~30s", d)
	}
	// до истечения backoff строка не ретраится
	b.deliveryTick(context.Background())
	if n := m.callCount("sendMessage"); n != 1 {
		t.Fatalf("row must not be retried before the backoff elapses, sends = %d", n)
	}
	// «прошло 30 секунд»: вторая неудача — пауза строго больше первой
	if err := store.DeferOutboxAttempt(row.ID, -time.Hour); err != nil {
		t.Fatal(err)
	}
	b.deliveryTick(context.Background())
	row = firstPendingRow(t, store)
	if row.NextAttemptAt == nil {
		t.Fatal("second failure must defer again")
	}
	d2 := time.Until(*row.NextAttemptAt)
	if d2 < 2*d {
		t.Fatalf("backoff must grow: first = %v, second = %v", d, d2)
	}
	// третья — ещё больше, попытки растут, строка НЕ удаляется
	if err := store.DeferOutboxAttempt(row.ID, -time.Hour); err != nil {
		t.Fatal(err)
	}
	b.deliveryTick(context.Background())
	row = firstPendingRow(t, store)
	d3 := time.Until(*row.NextAttemptAt)
	if d3 < 2*d2 || d3 > backoffCap {
		t.Fatalf("backoff must keep growing up to the cap: %v", d3)
	}
	if row.Attempts < 3 {
		t.Fatalf("attempts = %d, want ≥3", row.Attempts)
	}
	// строка дожила и доехала: backoff не удаляет события (TTL — единственный
	// механизм потери, и он не сокращён)
	fail.Store(false)
	m.handler = nil
	if err := store.DeferOutboxAttempt(row.ID, -time.Hour); err != nil {
		t.Fatal(err)
	}
	b.deliveryTick(context.Background())
	if rows, _ := store.PendingOutbox(10); len(rows) != 0 {
		t.Fatalf("row must be delivered after recovery: %+v", rows)
	}
}

// 6.2: недоступные чаты не занимают слоты батча — живой чат получает
// уведомление немедленно, сколько бы ни стояло за головами мёртвых чатов.
func TestUnavailableChatsDoNotStarveLiveOnes(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil { // «мёртвый» чат
		t.Fatal(err)
	}
	if err := store.SetSubscription(200, "glm-5.3", true); err != nil { // живой чат
		t.Fatal(err)
	}
	m.handler = func(method string, body []byte) (any, int) {
		if method == "sendMessage" {
			var p map[string]any
			json.Unmarshal(body, &p)
			if chatID, _ := p["chat_id"].(float64); int64(chatID) == 100 {
				return map[string]any{"ok": false, "error_code": 500, "description": "Internal Server Error"}, http.StatusInternalServerError
			}
		}
		return map[string]any{"ok": true, "result": map[string]any{"message_id": m.nextMessageID()}}, http.StatusOK
	}
	// 55 строк мёртвому чату (больше одного батча) + одна живому
	base := time.Now().Add(-time.Hour)
	for i := 0; i < 55; i++ {
		commitCycle(t, b, cycleAt(base.Add(time.Duration(i)*time.Second),
			transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	}
	commitCycle(t, b, cycleAt(base.Add(time.Minute), transition("glm-5.3", status.Operational, status.Down, time.Now(), nil)))
	m.resetCalls()

	b.deliveryTick(context.Background())
	// голова мёртвого чата попытана один раз и отложена; живой чат получил свою строку
	sent := 0
	for _, c := range m.sentChatIDs("sendMessage") {
		if c == 200 {
			sent++
		}
	}
	if sent != 1 {
		t.Fatalf("live chat must get its notification immediately, got %d", sent)
	}
	if n := m.callCount("sendMessage"); n > 2 {
		t.Fatalf("dead chat must not be hammered, sends = %d", n)
	}
	if rows, _ := store.PendingOutbox(100); len(rows) != 55 {
		t.Fatalf("dead chat rows must stay queued: %d", len(rows))
	}
}

// 6.3 (главный инвариант): более новое восстановление не приходит раньше
// старого падения того же чата — ни в этом тике, ни после рестарта;
// после созревания головы обе строки доезжают по порядку за один тик.
func TestFIFORecoveryNeverOvertakesFall(t *testing.T) {
	m := newMockTG(t)
	path := t.TempDir() + "/bot.db"
	b, store := newBotAt(t, m, path, []storage.Model{{ID: "gpt-6-astra", Family: "GPT"}})
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	commitCycle(t, b, cycleAt(time.Now().Add(-2*time.Minute), transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	commitCycle(t, b, cycleAt(time.Now().Add(-time.Minute), transition("gpt-6-astra", status.Down, status.Operational, time.Now(), nil)))

	var sent []string
	var firstFail atomic.Bool
	firstFail.Store(true)
	m.handler = func(method string, body []byte) (any, int) {
		if method == "sendMessage" {
			var p map[string]any
			json.Unmarshal(body, &p)
			sent = append(sent, fmt.Sprint(p["text"]))
			if firstFail.CompareAndSwap(true, false) {
				return map[string]any{"ok": false, "error_code": 500, "description": "boom"}, http.StatusInternalServerError
			}
		}
		return map[string]any{"ok": true, "result": map[string]any{"message_id": m.nextMessageID()}}, http.StatusOK
	}
	b.deliveryTick(context.Background())
	if len(sent) != 1 || !strings.Contains(sent[0], "недоступна") {
		t.Fatalf("only the older row may be attempted, got %v", sent)
	}

	// рестарт: тот же файл БД, новый бот
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	b2, store2 := newBotAt(t, m, path, []storage.Model{{ID: "gpt-6-astra", Family: "GPT"}})
	m.handler = func(method string, body []byte) (any, int) {
		if method == "sendMessage" {
			var p map[string]any
			json.Unmarshal(body, &p)
			sent = append(sent, fmt.Sprint(p["text"]))
		}
		return map[string]any{"ok": true, "result": map[string]any{"message_id": m.nextMessageID()}}, http.StatusOK
	}
	// голова (падение) отложена backoff'ом: восстановления нет, пока она ждёт
	b2.deliveryTick(context.Background())
	if len(sent) != 1 {
		t.Fatalf("recovery must not overtake the deferred fall notification (FIFO after restart): %v", sent)
	}
	if rows, _ := store2.PendingOutbox(10); len(rows) != 2 {
		t.Fatalf("both rows must stay queued: %+v", rows)
	}

	// голова созрела: обе строки доезжают по порядку за один тик
	row := firstPendingRow(t, store2)
	if err := store2.DeferOutboxAttempt(row.ID, -time.Hour); err != nil {
		t.Fatal(err)
	}
	b2.deliveryTick(context.Background())
	if len(sent) != 3 {
		t.Fatalf("both rows must be delivered in order after maturity, got %v", sent)
	}
	if !strings.Contains(sent[1], "недоступна") || !strings.Contains(sent[2], "восстановилась") {
		t.Fatalf("FIFO order broken: %v", sent)
	}
}

// 6.4: мёртвый чат — очистка памяти: lastText, pinWarn, drafts и привязка
// status_messages, а не только подписки и очередь.
func TestDeadChatCleanupMemory(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	if err := store.SetStatusMessage(100, 777); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	b.lastText[100] = "панель"
	b.pinWarn[100] = true
	b.drafts[100] = map[string]bool{"gpt-6-astra": true}
	b.mu.Unlock()

	m.handler = func(method string, body []byte) (any, int) {
		return map[string]any{"ok": false, "error_code": 403, "description": "Forbidden: bot was blocked by the user"}, http.StatusForbidden
	}
	commitCycle(t, b, cycleAt(time.Now(), transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	b.deliveryTick(context.Background())

	if sm, err := store.StatusMessage(100); err != nil || sm != nil {
		t.Fatalf("panel binding of the dead chat must be dropped: %v %v", sm, err)
	}
	b.mu.Lock()
	lt, pw := b.lastText[100], b.pinWarn[100]
	dr := b.drafts[100]
	b.mu.Unlock()
	if lt != "" || pw || dr != nil {
		t.Fatalf("in-memory state of the dead chat must be cleaned: lastText=%q pinWarn=%v drafts=%v", lt, pw, dr)
	}
}

// 6.2: write-forbidden (бот заглушён в группе) — не «мёртвый» чат:
// подписки и строка остаются, доставка откладывается backoff'ом.
func TestWriteForbiddenKeepsSubscriptions(t *testing.T) {
	m := newMockTG(t)
	b, store := newBot(t, m)
	if err := store.SetSubscription(100, "gpt-6-astra", true); err != nil {
		t.Fatal(err)
	}
	m.handler = func(method string, body []byte) (any, int) {
		return map[string]any{"ok": false, "error_code": 400, "description": "Bad Request: chat_write_forbidden"}, http.StatusBadRequest
	}
	commitCycle(t, b, cycleAt(time.Now(), transition("gpt-6-astra", status.Operational, status.Down, time.Now(), nil)))
	b.deliveryTick(context.Background())
	subs, _ := store.Subscriptions(100)
	if len(subs) != 1 {
		t.Fatalf("write-forbidden is not a confirmed permanent exclusion, subs = %v", subs)
	}
	rows, _ := store.PendingOutbox(10)
	if len(rows) != 1 {
		t.Fatalf("row must stay queued, got %+v", rows)
	}
	if rows[0].NextAttemptAt == nil {
		t.Fatal("row must be deferred by backoff")
	}
}
