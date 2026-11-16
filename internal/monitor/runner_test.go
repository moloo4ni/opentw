package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"opentw/internal/api"
	"opentw/internal/storage"
)

// fakeAPI is a scriptable api.Client stand-in.
type fakeAPI struct {
	mu       sync.Mutex
	catalog  []string
	catErr   error
	hook     func(modelID string) *api.RawResult
	genCalls map[string]int
}

func newFakeAPI(catalog []string) *fakeAPI {
	return &fakeAPI{catalog: catalog, genCalls: map[string]int{}}
}

func (f *fakeAPI) ListModels(ctx context.Context) ([]string, error) {
	if f.catErr != nil {
		return nil, f.catErr
	}
	return f.catalog, nil
}

func (f *fakeAPI) Generate(ctx context.Context, modelID string, maxTokens int) *api.RawResult {
	f.mu.Lock()
	f.genCalls[modelID]++
	f.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return &api.RawResult{ModelID: modelID, ErrorKind: "cancelled", ErrorMessage: err.Error()}
	}
	return f.hook(modelID)
}

func (f *fakeAPI) calls(modelID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.genCalls[modelID]
}

func ptrLat(v int64) *int64 { return &v }

func ptrFloat(v float64) *float64 { return &v }

func okResult(id string, latUS int64, tps float64) *api.RawResult {
	return &api.RawResult{
		ModelID: id, Timestamp: time.Now(), ErrorKind: "", HTTPStatus: 200,
		LatencyUS: latUS, TTFTUS: ptrLat(latUS / 3), GenUS: ptrLat(latUS * 2 / 3),
		TPS: &tps, InputTokens: ptrLat(10), OutputTokens: ptrLat(8),
		Streamed: true,
	}
}

func errResult(id string, status int, msg string) *api.RawResult {
	return &api.RawResult{ModelID: id, Timestamp: time.Now(), ErrorKind: "http", HTTPStatus: status, ErrorMessage: msg, LatencyUS: 40_000}
}

func newRunner(t *testing.T, f *fakeAPI, models []storage.Model) (*Runner, *[]CycleResult) {
	t.Helper()
	s, err := storage.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if err := s.SyncModels(models); err != nil {
		t.Fatal(err)
	}
	results := &[]CycleResult{}
	r := NewRunner(RunnerCfg{
		API:         f,
		Store:       s,
		Models:      models,
		Concurrency: 2,
		MaxTokens:   8,
		Interval:    time.Minute,
		Perf:        PerfCfg{MinSamples: 12, LatencyFactor: 3.0, TPSShare: 0.4},
		OnCycle: func(res CycleResult) ([]storage.OutboxEvent, error) {
			*results = append(*results, res)
			return nil, nil
		},
	})
	return r, results
}

// Расписание циклов привязано к границам интервала, а не к моменту
// старта: при 15м проверки идут в :00/:15/:30/:45 независимо от того,
// когда процесс запущен.
func TestNextTickAligned(t *testing.T) {
	m := func(h, min, s int) time.Time { return time.Date(2026, 11, 16, h, min, s, 0, time.UTC) }
	for _, tc := range []struct {
		from, want time.Time
	}{
		{m(13, 52, 6), m(14, 0, 0)},   // старт между границами → ближайшая
		{m(14, 0, 0), m(14, 15, 0)},   // ровно на границе → следующая
		{m(14, 14, 59), m(14, 15, 0)}, // за секунду до границы
	} {
		if got := NextTick(15*time.Minute, tc.from); !got.Equal(tc.want) {
			t.Fatalf("NextTick(15m, %s) = %s, want %s", tc.from, got, tc.want)
		}
	}
	if got := NextTick(time.Hour, m(13, 30, 0)); !got.Equal(m(14, 0, 0)) {
		t.Fatalf("NextTick(1h, 13:30) = %s, want 14:00", got)
	}
}

// Ошибка чтения предыдущих состояний прерывает цикл: без них корректная
// классификация переходов невозможна (иначе откроются дубли инцидентов).
func TestCycleAbortsWhenStatesUnreadable(t *testing.T) {
	f := newFakeAPI([]string{"model-a", "model-b"})
	f.hook = func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) }
	r, _ := newRunner(t, f, basicModels())
	if _, err := r.RunCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	r.Store().Close() // следующие операции с хранилищем обязаны ошибаться
	_, err := r.RunCycle(context.Background())
	if err == nil {
		t.Fatal("cycle must abort when previous states are unreadable")
	}
	if n := f.calls("model-a"); n != 1 {
		t.Fatalf("no model may be re-checked after failed state read: got %d generate calls", n)
	}
}

func basicModels() []storage.Model {
	return []storage.Model{
		{ID: "model-a", Family: "A"},
		{ID: "model-b", Family: "B"},
	}
}

func TestFirstCycleBaselineNoEvents(t *testing.T) {
	f := newFakeAPI([]string{"model-a", "model-b"})
	f.hook = func(id string) *api.RawResult {
		if id == "model-b" {
			return errResult(id, 503, "Service Unavailable")
		}
		return okResult(id, 1_000_000, 40)
	}
	r, results := newRunner(t, f, basicModels())
	cycle, err := r.RunCycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(cycle.Transitions) != 0 {
		t.Fatalf("first cycle must not emit events: %+v", cycle.Transitions)
	}
	if len(*results) != 1 {
		t.Fatalf("OnCycle called %d times", len(*results))
	}
	m, _ := r.Store().Model("model-a")
	if m.LastStatus != StatusOperational {
		t.Fatalf("model-a = %+v", m)
	}
	mb, _ := r.Store().Model("model-b")
	if mb.LastStatus != StatusDown {
		t.Fatalf("model-b = %+v", mb)
	}
	last, _ := r.Store().LastChecks()
	if len(last) != 2 || last["model-b"].HTTPStatus != 503 {
		t.Fatalf("checks = %+v", last)
	}
	// D2-сценарий B: первое наблюдение Down — базлайн без уведомления,
	// но инцидент (учёт длительности) открывается.
	if open, _ := r.Store().HasOpenIncident("model-b"); !open {
		t.Fatal("first-sight Down must still open an incident (baseline accounting)")
	}
	if last["model-b"].ErrorMessage != "Service Unavailable" {
		t.Fatalf("provider message lost: %+v", last["model-b"])
	}
}

func TestSecondCycleTransitionAndIncident(t *testing.T) {
	f := newFakeAPI([]string{"model-a", "model-b"})
	f.hook = func(id string) *api.RawResult {
		if id == "model-b" {
			return errResult(id, 503, "Service Unavailable")
		}
		return okResult(id, 1_000_000, 40)
	}
	r, _ := newRunner(t, f, basicModels())
	if _, err := r.RunCycle(context.Background()); err != nil {
		t.Fatal(err)
	}

	f.hook = func(id string) *api.RawResult { return okResult(id, 900_000, 55) }
	time.Sleep(5 * time.Millisecond)
	res, err := r.RunCycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Transitions) != 1 {
		t.Fatalf("want exactly 1 transition, got %+v", res.Transitions)
	}
	tr := res.Transitions[0]
	if tr.ModelID != "model-b" || tr.From != StatusDown || tr.To != StatusOperational {
		t.Fatalf("transition = %+v", tr)
	}
	if tr.Check.TPS == nil || *tr.Check.TPS != 55 {
		t.Fatalf("check attached: %+v", tr.Check)
	}
	if tr.Incident == nil || tr.Incident.From != StatusDown {
		t.Fatalf("incident = %+v", tr.Incident)
	}
	if tr.Incident.Duration <= 0 {
		t.Fatalf("incident duration = %v", tr.Incident.Duration)
	}
	for _, tr := range res.Transitions {
		if tr.ModelID == "model-a" {
			t.Fatal("unchanged model must not produce transition")
		}
	}
	if open, _ := r.Store().HasOpenIncident("model-b"); open {
		t.Fatal("incident must be closed on recovery")
	}
}

func TestSameStatusNoRepeatEvents(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) }
	r, _ := newRunner(t, f, []storage.Model{{ID: "model-a", Family: "A"}})
	if _, err := r.RunCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	res, err := r.RunCycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Transitions) != 0 {
		t.Fatalf("same status must not emit: %+v", res.Transitions)
	}
}

func TestDownToDegradedPartialRecovery(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult { return errResult(id, 503, "Service Unavailable") }
	r, _ := newRunner(t, f, []storage.Model{{ID: "model-a", Family: "A"}})
	if _, err := r.RunCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.hook = func(id string) *api.RawResult { return errResult(id, 429, "Upstream rate limit") }
	res, err := r.RunCycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Transitions) != 1 {
		t.Fatalf("transitions = %+v", res.Transitions)
	}
	tr := res.Transitions[0]
	if tr.From != StatusDown || tr.To != StatusDegraded {
		t.Fatalf("transition = %+v", tr)
	}
}

func TestCatalogAbsenceStillChecks(t *testing.T) {
	f := newFakeAPI([]string{"model-a"}) // model-b отсутствует в каталоге
	f.hook = func(id string) *api.RawResult {
		if id == "model-b" {
			return errResult(id, 404, "Invalid model.")
		}
		return okResult(id, 1_000_000, 40)
	}
	r, _ := newRunner(t, f, basicModels())
	res, err := r.RunCycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if f.calls("model-b") != 1 {
		t.Fatalf("absent model must get a real request, got %d calls", f.calls("model-b"))
	}
	last, _ := r.Store().LastChecks()
	if last["model-b"].HTTPStatus != 404 || last["model-b"].ErrorMessage != "Invalid model." {
		t.Fatalf("real API verdict must be recorded, got %+v", last["model-b"])
	}
	if len(res.Checks) != 2 {
		t.Fatalf("checks = %+v", res.Checks)
	}
}

func TestCatalogFailureStillChecks(t *testing.T) {
	f := newFakeAPI(nil)
	f.catErr = errors.New("dial tcp: refused")
	f.hook = func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) }
	r, _ := newRunner(t, f, []storage.Model{{ID: "model-a", Family: "A"}})
	res, err := r.RunCycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if f.calls("model-a") != 1 {
		t.Fatal("catalog error must not block generation checks")
	}
	if len(res.Checks) != 1 {
		t.Fatalf("checks = %+v", res.Checks)
	}
}

func TestPerfDegradationUsesHistory(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) } // latency 1s
	r, _ := newRunner(t, f, []storage.Model{{ID: "model-a", Family: "A"}})
	now := time.Now()
	for i := 0; i < 12; i++ {
		lat := ptrLat(100_000)
		tps := 40.0
		if err := r.Store().InsertCheck(storage.Check{
			ModelID: "model-a", At: now.Add(-time.Duration(24-i) * time.Minute),
			HTTPStatus: 200, Status: StatusOperational, LatencyUS: lat, TPS: &tps,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Store().SetModelState("model-a", StatusOperational, "", "", now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	res, err := r.RunCycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m, _ := r.Store().Model("model-a")
	if m.LastStatus != StatusDegraded {
		t.Fatalf("latency 10x history must degrade, got %q", m.LastStatus)
	}
	if m.ErrorType != "perf" {
		t.Fatalf("error type = %q", m.ErrorType)
	}
	if len(res.Transitions) != 1 || res.Transitions[0].To != StatusDegraded {
		t.Fatalf("transitions = %+v", res.Transitions)
	}
}

func TestPerfDegradationInsufficientHistory(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) }
	r, _ := newRunner(t, f, []storage.Model{{ID: "model-a", Family: "A"}})
	res, err := r.RunCycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m, _ := r.Store().Model("model-a")
	if m.LastStatus != StatusOperational {
		t.Fatalf("no history must stay operational, got %q", m.LastStatus)
	}
	if len(res.Transitions) != 0 {
		t.Fatal("no transition expected")
	}
}

func TestUnknownStatusNoEvents(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) }
	r, _ := newRunner(t, f, []storage.Model{{ID: "model-a", Family: "A"}})
	if _, err := r.RunCycle(context.Background()); err != nil { // operational baseline
		t.Fatal(err)
	}
	f.hook = func(id string) *api.RawResult { return errResult(id, 401, "Invalid API key") }
	res2, err := r.RunCycle(context.Background()) // операционная ошибка монитора → Unknown
	if err != nil {
		t.Fatal(err)
	}
	if len(res2.Transitions) != 0 {
		t.Fatalf("Unknown must not emit: %+v", res2.Transitions)
	}
	m, _ := r.Store().Model("model-a")
	if m.LastStatus != StatusUnknown {
		t.Fatalf("status = %q, want unknown", m.LastStatus)
	}
	res3, err := r.RunCycle(context.Background())
	_ = res3
	_ = err
	m, _ = r.Store().Model("model-a")
	if m.LastStatus != StatusUnknown {
		t.Fatalf("status still = %q", m.LastStatus)
	}
}

func TestCancelledChecksNotRecorded(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) }
	r, _ := newRunner(t, f, []storage.Model{{ID: "model-a", Family: "A"}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.RunCycle(ctx); err != nil {
		t.Fatal(err)
	}
	last, _ := r.Store().LastChecks()
	if len(last) != 0 {
		t.Fatalf("cancelled checks must not be recorded: %+v", last)
	}
	m, _ := r.Store().Model("model-a")
	if m.LastStatus != "" {
		t.Fatalf("cancelled cycle must not alter state: %+v", m)
	}
}

func TestSingleFlight(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	release := make(chan struct{})
	f.hook = func(string) *api.RawResult {
		<-release
		return okResult("model-a", 1_000_000, 40)
	}
	r, _ := newRunner(t, f, []storage.Model{{ID: "model-a", Family: "A"}})

	done := make(chan error, 1)
	go func() {
		_, err := r.RunCycle(context.Background())
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	if _, err := r.RunCycle(context.Background()); !errors.Is(err, ErrCycleRunning) {
		t.Fatalf("concurrent cycle must be rejected, got %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("first cycle failed: %v", err)
	}
	if _, err := r.RunCycle(context.Background()); err != nil {
		t.Fatalf("cycle after completion must run: %v", err)
	}
}

// Сбой транспорта при каталоге + транспортные же ошибки моделей → у всех
// ⚪ Unknown («монитор не может проверить»), а не 27 ложных «модель упала».
func TestCatalogTransportMarksUnknown(t *testing.T) {
	f := newFakeAPI(nil)
	f.catErr = errors.New("api: models: dial tcp: connection refused")
	f.hook = func(id string) *api.RawResult {
		return &api.RawResult{ModelID: id, Timestamp: time.Now(), ErrorKind: "timeout", ErrorMessage: "context deadline exceeded", LatencyUS: 120_000_000}
	}
	r, res0 := newRunner(t, f, basicModels())
	res, err := r.RunCycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Transitions) != 0 {
		t.Fatalf("monitor-wide transport failure must not emit events: %+v", res.Transitions)
	}
	for _, id := range []string{"model-a", "model-b"} {
		m, _ := r.Store().Model(id)
		if m.LastStatus != StatusUnknown {
			t.Fatalf("%s status = %q, want Unknown", id, m.LastStatus)
		}
	}
	last, _ := r.Store().LastChecks()
	if last["model-a"].ErrorType != "timeout" {
		t.Fatalf("raw error must be recorded verbatim: %+v", last["model-a"])
	}
	_ = res0
}

// Каталог в 429 (лимит ключа монитора) + модели в 429 → у моделей ⚪.
func TestCatalogRatelimitMarksUnknown(t *testing.T) {
	f := newFakeAPI(nil)
	f.catErr = errors.New("api: models: HTTP 429: Too Many Requests")
	f.hook = func(id string) *api.RawResult {
		if id == "model-b" { // модель с другой ошибкой остаётся при своём статусе
			return errResult(id, 500, "boom")
		}
		return errResult(id, 429, "Too Many Requests")
	}
	r, _ := newRunner(t, f, basicModels())
	if _, err := r.RunCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	m, _ := r.Store().Model("model-a")
	if m.LastStatus != StatusUnknown {
		t.Fatalf("429 under monitor rate limit = %q, want Unknown", m.LastStatus)
	}
	mb, _ := r.Store().Model("model-b")
	if mb.LastStatus != StatusDown {
		t.Fatalf("unrelated 500 must stay Down, got %q", mb.LastStatus)
	}
}

// Живой каталог: транспортные ошибки моделей — честные 🔴 (не «проблема монитора»).
func TestCatalogOKStillDown(t *testing.T) {
	f := newFakeAPI([]string{"model-a", "model-b"})
	f.hook = func(id string) *api.RawResult {
		return &api.RawResult{ModelID: id, Timestamp: time.Now(), ErrorKind: "timeout", ErrorMessage: "context deadline exceeded"}
	}
	r, _ := newRunner(t, f, basicModels())
	if _, err := r.RunCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	m, _ := r.Store().Model("model-a")
	if m.LastStatus != StatusDown {
		t.Fatalf("live catalog + model timeout must be Down, got %q", m.LastStatus)
	}
}

// Тихое восстановление из Unknown устранено: после ⚪-периода (сбитый ключ)
// восстановление модели доходит подписчикам, длительность инцидента честная.
func TestRecoveryFromUnknownEmitsEvent(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult { return errResult(id, 503, "Service Unavailable") }
	r, _ := newRunner(t, f, []storage.Model{{ID: "model-a", Family: "A"}})
	if _, err := r.RunCycle(context.Background()); err != nil { // первое наблюдение Down
		t.Fatal(err)
	}
	f.hook = func(id string) *api.RawResult { return errResult(id, 500, "keep down") }
	if _, err := r.RunCycle(context.Background()); err != nil { // 503 → 500: Down→Down, но инцидент уже открыт после 503
		t.Fatal(err)
	}
	f.hook = func(id string) *api.RawResult { return errResult(id, 401, "Invalid API key") }
	if _, err := r.RunCycle(context.Background()); err != nil { // ⚪ Unknown
		t.Fatal(err)
	}
	m, _ := r.Store().Model("model-a")
	if m.LastStatus != StatusUnknown {
		t.Fatalf("status after 401 = %q", m.LastStatus)
	}
	f.hook = func(id string) *api.RawResult { return okResult(id, 900_000, 55) }
	res, err := r.RunCycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Transitions) != 1 {
		t.Fatalf("recovery from Unknown must emit one event: %+v", res.Transitions)
	}
	tr := res.Transitions[0]
	if tr.From != StatusUnknown || tr.To != StatusOperational {
		t.Fatalf("transition = %+v", tr)
	}
	if tr.Incident == nil || tr.Incident.From != StatusDown || tr.Incident.Duration <= 0 {
		t.Fatalf("incident duration lost: %+v", tr.Incident)
	}
	if open, _ := r.Store().HasOpenIncident("model-a"); open {
		t.Fatal("incident must be closed")
	}
}

// Кратковременный ⚪ без открытого инцидента (была 🟢) → восстановления-события нет.
func TestUnknownBlipAfterOperationalNoEvent(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) }
	r, _ := newRunner(t, f, []storage.Model{{ID: "model-a", Family: "A"}})
	if _, err := r.RunCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.hook = func(id string) *api.RawResult { return errResult(id, 401, "Invalid API key") }
	if _, err := r.RunCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.hook = func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) }
	res, err := r.RunCycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Transitions) != 0 {
		t.Fatalf("no-incident blip needs no event: %+v", res.Transitions)
	}
}

// Перезапуск во время инцидента: постоянный Down не эмитит повторных событий.
func TestRestartWhileDownNoFalseTransitions(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/test.db"
	mkRunner := func() *Runner {
		s, err := storage.Open(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s.Close() })
		if err := s.SyncModels(basicModels()); err != nil {
			t.Fatal(err)
		}
		f := newFakeAPI([]string{"model-a", "model-b"})
		f.hook = func(id string) *api.RawResult { return errResult(id, 503, "Service Unavailable") }
		return NewRunner(RunnerCfg{API: f, Store: s, Models: basicModels(), Concurrency: 2, MaxTokens: 8, Interval: time.Minute, Perf: PerfCfg{MinSamples: 12, LatencyFactor: 3.0, TPSShare: 0.4}})
	}
	r1 := mkRunner()
	if _, err := r1.RunCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	r2 := mkRunner()
	res, err := r2.RunCycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Transitions) != 0 {
		t.Fatalf("restart must not duplicate a running incident: %+v", res.Transitions)
	}
	if open, _ := r2.Store().HasOpenIncident("model-a"); !open {
		t.Fatal("incident must continue across restart")
	}
}

// Основа perf-деградации видна в данных проверки (числа, не слова).
func TestPerfReasonInCheck(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult { return okResult(id, 10_000_000, 40) } // latency 10s
	r, _ := newRunner(t, f, []storage.Model{{ID: "model-a", Family: "A"}})
	now := time.Now()
	for i := 0; i < 12; i++ {
		if err := r.Store().InsertCheck(storage.Check{
			ModelID: "model-a", At: now.Add(-time.Duration(24-i) * time.Minute),
			HTTPStatus: 200, Status: StatusOperational, LatencyUS: ptrLat(100_000), TPS: ptrFloat(40.0),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Store().SetModelState("model-a", StatusOperational, "", "", now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	res, err := r.RunCycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	c := res.Checks["model-a"]
	if c.ErrorType != "perf" {
		t.Fatalf("check = %+v", c)
	}
	if !strings.Contains(c.ErrorMessage, "latency 10.0s") || !strings.Contains(c.ErrorMessage, "P95") {
		t.Fatalf("perf reason must carry numbers, got %q", c.ErrorMessage)
	}
}

// Цикл вычищает инциденты старше retention (D7).
func TestIncidentRetentionInCycle(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) }
	r, _ := newRunner(t, f, basicModels())
	if err := r.cfg.Store.OpenIncident("model-z", StatusDown, time.Now().Add(-31*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := r.cfg.Store.OpenIncident("model-y", StatusDown, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.RunCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if open, _ := r.Store().HasOpenIncident("model-z"); open {
		t.Fatal("31d-old incident must be cleaned by the cycle")
	}
	if open, _ := r.Store().HasOpenIncident("model-y"); !open {
		t.Fatal("fresh incident must survive")
	}
}

// D9: heartbeat-файл обновляется в конце каждого цикла (основа HEALTHCHECK).
func TestHeartbeatFileWritten(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) }
	path := t.TempDir() + "/opentw.heartbeat"
	s, err := storage.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if err := s.SyncModels([]storage.Model{{ID: "model-a", Family: "A"}}); err != nil {
		t.Fatal(err)
	}
	r := NewRunner(RunnerCfg{
		API: f, Store: s, Models: []storage.Model{{ID: "model-a", Family: "A"}},
		Concurrency: 1, MaxTokens: 8, Interval: time.Minute, HeartbeatPath: path,
	})
	if _, err := os.Stat(path); err == nil {
		t.Fatal("heartbeat before first cycle")
	}
	if _, err := r.RunCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("heartbeat after cycle: %v", err)
	}
	if time.Since(st.ModTime()) > time.Minute {
		t.Fatalf("heartbeat must be fresh: %v", st.ModTime())
	}
}

// Мусорный ответ каталога — не транспортный сбой: совпадающих с ним
// ошибок моделей он не «гасит» (Rev-1).
func TestCatalogInvalidJSONNotTransport(t *testing.T) {
	f := newFakeAPI(nil)
	f.catErr = errors.New("api: models: invalid response: bad json")
	f.hook = func(id string) *api.RawResult {
		return &api.RawResult{ModelID: id, Timestamp: time.Now(), ErrorKind: "timeout", ErrorMessage: "deadline"}
	}
	r, _ := newRunner(t, f, basicModels())
	if _, err := r.RunCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	m, _ := r.Store().Model("model-a")
	if m.LastStatus != StatusDown {
		t.Fatalf("unparseable catalog must not mask a real outage: %q", m.LastStatus)
	}
}

// Unknown→Down не перезапускает инцидент: длительность считается от
// исходного падения, а не от «прозрения» монитора (Rev-5).
func TestUnknownToDownKeepsOriginalIncident(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult { return errResult(id, 503, "Service Unavailable") }
	r, _ := newRunner(t, f, []storage.Model{{ID: "model-a", Family: "A"}})
	if _, err := r.RunCycle(context.Background()); err != nil { // Down
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	f.hook = func(id string) *api.RawResult { return errResult(id, 401, "Invalid API key") }
	if _, err := r.RunCycle(context.Background()); err != nil { // ⚪ (инцидент остаётся открытым)
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	f.hook = func(id string) *api.RawResult { return errResult(id, 503, "Service Unavailable") }
	if _, err := r.RunCycle(context.Background()); err != nil { // снова Down
		t.Fatal(err)
	}
	// D2-сценарий C: старый инцидент не дублируется и остаётся открытым —
	// длительность считается от исходного падения, не от «прозрения».
	if open, _ := r.Store().HasOpenIncident("model-a"); !open {
		t.Fatal("original incident must stay open through the blind period")
	}
	time.Sleep(10 * time.Millisecond)
	f.hook = func(id string) *api.RawResult { return okResult(id, 900_000, 55) }
	res, err := r.RunCycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Transitions) != 1 {
		t.Fatalf("recovery expected: %+v", res.Transitions)
	}
	inc := res.Transitions[0].Incident
	if inc == nil || inc.Duration < 20*time.Millisecond {
		t.Fatalf("duration must span from the original fall, not the monitor blip: %+v", inc)
	}
}

// P0-1: сбой подготовки событий (подписки недоступны) не должен фиксировать
// состояния — иначе события цикла терялись бы навсегда (следующий цикл видел
// prev == cur). Следующий цикл обязан переоткрыть переход.
func TestOnCycleErrorKeepsState(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) }
	models := []storage.Model{{ID: "model-a", Family: "A"}}
	s, err := storage.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if err := s.SyncModels(models); err != nil {
		t.Fatal(err)
	}

	var fail bool
	r := NewRunner(RunnerCfg{
		API: f, Store: s, Models: models, Concurrency: 2, MaxTokens: 8, Interval: time.Minute,
		OnCycle: func(res CycleResult) ([]storage.OutboxEvent, error) {
			if fail {
				return nil, errors.New("subscribers unavailable")
			}
			return nil, nil
		},
	})

	// базлайн: модель работает
	if _, err := r.RunCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	// модель падает, но фиксация невозможна
	fail = true
	f.hook = func(id string) *api.RawResult { return errResult(id, 503, "boom") }
	if _, err := r.RunCycle(context.Background()); err == nil {
		t.Fatal("cycle must fail when events cannot be prepared")
	}
	m, err := s.Model("model-a")
	if err != nil {
		t.Fatal(err)
	}
	if m.LastStatus != StatusOperational {
		t.Fatalf("failed cycle must not commit state, got %q", m.LastStatus)
	}
	// доставка восстановилась: следующий цикл обязан переоткрыть переход
	fail = false
	res, err := r.RunCycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Transitions) != 1 || res.Transitions[0].To != StatusDown {
		t.Fatalf("transition must be re-opened: %+v", res.Transitions)
	}
	m, err = s.Model("model-a")
	if err != nil {
		t.Fatal(err)
	}
	if m.LastStatus != StatusDown {
		t.Fatalf("state must be committed now, got %q", m.LastStatus)
	}
}

// I1: проваленный цикл (мёртвая база) не освежает heartbeat — HEALTHCHECK
// обязан краснеть, а не показывать вечно живой монитор.
func TestHeartbeatNotRefreshedByFailedCycle(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult { return okResult("model-a", 1_000_000, 40) }
	path := t.TempDir() + "/opentw.heartbeat"
	s, err := storage.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatal(err)
	}
	models := basicModels()
	if err := s.SyncModels(models); err != nil {
		t.Fatal(err)
	}
	r := NewRunner(RunnerCfg{
		API: f, Store: s, Models: models, Concurrency: 1, MaxTokens: 8, Interval: time.Minute,
		HeartbeatPath: path,
	})
	if _, err := r.RunCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	fresh := st.ModTime()

	// база умерла: чтение предыдущих состояний проваливается. Первый
	// проваленный цикл ещё имел право на стартовый heartbeat (прошый цикл
	// был успешен, монитор был жив на старте), второй подряд — уже нет.
	s.Close()
	if _, err := r.RunCycle(context.Background()); err == nil {
		t.Fatal("cycle must fail with closed store")
	}
	st, err = os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	afterFirst := st.ModTime()
	if _, err := r.RunCycle(context.Background()); err == nil {
		t.Fatal("second cycle must fail too")
	}
	st, err = os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !st.ModTime().Equal(afterFirst) {
		t.Fatalf("consecutively failed cycles must not refresh heartbeat: %v -> %v", afterFirst, st.ModTime())
	}
	if !afterFirst.After(fresh.Add(-time.Second)) {
		t.Fatalf("sanity: first failing cycle ran after baseline: %v", afterFirst)
	}
}

// Рестарт посреди цикла (P0-1): состояния, зафиксированные до рестарта,
// не опережают события — применяются одним коммитом в конце цикла.
// Проверяем через OnCycle: события, подготовленные вместе с состояниями,
// оказываются в outbox тем же коммитом.
func TestCycleCommitsStateAndEventsTogether(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) }
	models := basicModels()
	s, err := storage.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if err := s.SyncModels(models); err != nil {
		t.Fatal(err)
	}
	if err := s.SetModelState("model-a", StatusOperational, "", "", time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	r := NewRunner(RunnerCfg{
		API: f, Store: s, Models: models, Concurrency: 1, MaxTokens: 8, Interval: time.Minute,
		OnCycle: func(res CycleResult) ([]storage.OutboxEvent, error) {
			var evs []storage.OutboxEvent
			for _, tr := range res.Transitions {
				payload, _ := json.Marshal(tr)
				evs = append(evs, storage.OutboxEvent{ChatID: 42, Kind: "single", DedupeKey: fmt.Sprintf("%d", res.CycleAt.UnixNano()), Payload: payload})
			}
			return evs, nil
		},
	})
	// первый цикл: без переходов
	if _, err := r.RunCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	// модель падает — событие готовится и фиксируется коммитом цикла
	f.hook = func(id string) *api.RawResult { return errResult(id, 503, "boom") }
	if _, err := r.RunCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	rows, err := s.PendingOutbox(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ChatID != 42 {
		t.Fatalf("cycle must commit its event: %+v", rows)
	}
	m, _ := s.Model("model-a")
	if m.LastStatus != StatusDown {
		t.Fatalf("state = %q", m.LastStatus)
	}
}

// D2-сценарий A: модель была 🟢, монитор временно ослеп (401 → ⚪), за это
// время модель реально упала, зрение вернулось и API подтверждает 🔴.
// Падение обязано дойти подписчику событием, инцидент — открыться; время
// начала недоступности неизвестно и длительность не выдумывается.
func TestFallDuringBlindnessNotified(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) }
	r, _ := newRunner(t, f, []storage.Model{{ID: "model-a", Family: "A"}})
	if _, err := r.RunCycle(context.Background()); err != nil { // 🟢 базлайн
		t.Fatal(err)
	}
	f.hook = func(id string) *api.RawResult { return errResult(id, 401, "Invalid API key") }
	res, err := r.RunCycle(context.Background()) // ⚪ слепота монитора
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Transitions) != 0 {
		t.Fatalf("blindness must not emit events: %+v", res.Transitions)
	}
	m, _ := r.Store().Model("model-a")
	if m.LastStatus != StatusUnknown {
		t.Fatalf("status after 401 = %q", m.LastStatus)
	}
	f.hook = func(id string) *api.RawResult { return errResult(id, 503, "Service Unavailable") }
	res, err = r.RunCycle(context.Background()) // зрение вернулось: подтверждённое падение
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Transitions) != 1 {
		t.Fatalf("confirmed fall during blindness must reach subscribers: %+v", res.Transitions)
	}
	tr := res.Transitions[0]
	if tr.From != StatusUnknown || tr.To != StatusDown {
		t.Fatalf("transition = %+v", tr)
	}
	if tr.Incident != nil && tr.Incident.Duration > 0 {
		t.Fatalf("start time is unknown — duration must not be fabricated: %+v", tr.Incident)
	}
	if open, _ := r.Store().HasOpenIncident("model-a"); !open {
		t.Fatal("incident must be opened on the discovered fall")
	}
	m, _ = r.Store().Model("model-a")
	if m.LastStatus != StatusDown {
		t.Fatalf("state must be committed as Down: %+v", m)
	}
	last, _ := r.Store().LastChecks()
	if last["model-a"].ErrorMessage != "Service Unavailable" {
		t.Fatalf("provider message lost: %+v", last["model-a"])
	}
	// D2-сценарий D: восстановление приходит с честной длительностью —
	// от момента обнаружения падения (не выдуманной).
	time.Sleep(10 * time.Millisecond)
	f.hook = func(id string) *api.RawResult { return okResult(id, 900_000, 55) }
	res, err = r.RunCycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Transitions) != 1 {
		t.Fatalf("recovery must emit: %+v", res.Transitions)
	}
	rec := res.Transitions[0]
	if rec.From != StatusDown || rec.To != StatusOperational {
		t.Fatalf("recovery transition = %+v", rec)
	}
	if rec.Incident == nil || rec.Incident.Duration <= 0 {
		t.Fatalf("recovery must carry the incident duration: %+v", rec.Incident)
	}
	if open, _ := r.Store().HasOpenIncident("model-a"); open {
		t.Fatal("incident must be closed on recovery")
	}
}

// D3: инциденты применяются только вместе с коммитом цикла. Сбой подготовки
// событий после планирования не оставляет ни открытого инцидента, ни дублей
// после успешного повтора; восстановление закрывает ровно один инцидент.
func TestIncidentsAtomicWithCycleCommit(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult { return errResult(id, 503, "Service Unavailable") }
	models := []storage.Model{{ID: "model-a", Family: "A"}}
	s, err := storage.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if err := s.SyncModels(models); err != nil {
		t.Fatal(err)
	}
	var fail bool
	r := NewRunner(RunnerCfg{
		API: f, Store: s, Models: models, Concurrency: 2, MaxTokens: 8, Interval: time.Minute,
		OnCycle: func(res CycleResult) ([]storage.OutboxEvent, error) {
			if fail {
				return nil, errors.New("subscribers unavailable")
			}
			return nil, nil
		},
	})
	// цикл спланировал инцидент, но провалился до коммита: ничего не записано
	fail = true
	if _, err := r.RunCycle(context.Background()); err == nil {
		t.Fatal("cycle must fail when events cannot be prepared")
	}
	if open, _ := s.HasOpenIncident("model-a"); open {
		t.Fatal("failed cycle must not leave an open incident (D3)")
	}
	m, _ := s.Model("model-a")
	if m.LastStatus != "" {
		t.Fatalf("failed cycle must not commit state, got %q", m.LastStatus)
	}
	// успешный повтор открывает инцидент ровно один раз
	fail = false
	if _, err := r.RunCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if open, _ := s.HasOpenIncident("model-a"); !open {
		t.Fatal("successful cycle must open the incident")
	}
	// восстановление закрывает его; при дубликатах открытым остался бы
	// старый (закрывается только новейший) — длительность не теряется
	f.hook = func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) }
	res, err := r.RunCycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Transitions) != 1 {
		t.Fatalf("recovery expected: %+v", res.Transitions)
	}
	if res.Transitions[0].Incident == nil || res.Transitions[0].Incident.Duration <= 0 {
		t.Fatalf("recovery must carry duration: %+v", res.Transitions[0])
	}
	if open, _ := s.HasOpenIncident("model-a"); open {
		t.Fatal("recovery must close the incident; duplicates would leave it open (D3)")
	}
}

// Классификация offline-заявлений провайдера: живой ответ kimi-k2.6
// «The model kimi-k2.6 has been offline» (HTTP 400) — это 🔴, а не ⚪:
// событие подписчикам, инцидент, дословно сохранённая причина.
func TestProviderOfflineMessageIsDown(t *testing.T) {
	f := newFakeAPI([]string{"kimi-k2.6"})
	f.hook = func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) }
	r, _ := newRunner(t, f, []storage.Model{{ID: "kimi-k2.6", Family: "KIMI"}})
	if _, err := r.RunCycle(context.Background()); err != nil { // 🟢 базлайн
		t.Fatal(err)
	}
	f.hook = func(id string) *api.RawResult {
		return errResult(id, 400, "The model kimi-k2.6 has been offline")
	}
	res, err := r.RunCycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m, _ := r.Store().Model("kimi-k2.6")
	if m.LastStatus != StatusDown {
		t.Fatalf("explicit provider offline must be Down, got %q", m.LastStatus)
	}
	if len(res.Transitions) != 1 || res.Transitions[0].To != StatusDown {
		t.Fatalf("subscribers must learn the model is down: %+v", res.Transitions)
	}
	if open, _ := r.Store().HasOpenIncident("kimi-k2.6"); !open {
		t.Fatal("incident must be opened")
	}
	last, _ := r.Store().LastChecks()
	if last["kimi-k2.6"].ErrorMessage != "The model kimi-k2.6 has been offline" {
		t.Fatalf("provider wording must be preserved verbatim: %+v", last["kimi-k2.6"])
	}
}

// --- Этап 2: главный цикл Runner.Run ---

// newRunnerAt создаёт раннер с настраиваемым интервалом и OnCycle (для тестов Run).
func newRunnerAt(t *testing.T, f *fakeAPI, models []storage.Model, interval time.Duration, onCycle func(CycleResult) ([]storage.OutboxEvent, error)) (*Runner, *storage.Store) {
	t.Helper()
	s, err := storage.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if err := s.SyncModels(models); err != nil {
		t.Fatal(err)
	}
	r := NewRunner(RunnerCfg{
		API: f, Store: s, Models: models, Concurrency: 2, MaxTokens: 8,
		Interval: interval, Perf: PerfCfg{MinSamples: 12, LatencyFactor: 3.0, TPSShare: 0.4},
		OnCycle: onCycle,
	})
	return r, s
}

func waitForCond(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

// Первый цикл выполняется немедленно, не дожидаясь интервала.
func TestRunFirstCycleImmediate(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) }
	r, _ := newRunnerAt(t, f, []storage.Model{{ID: "model-a", Family: "A"}}, 10*time.Second, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	waitForCond(t, 2*time.Second, func() bool { return f.calls("model-a") >= 1 })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// Run выполняет несколько циклов по расписанию сетки интервала.
func TestRunRunsMultipleCycles(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) }
	r, _ := newRunnerAt(t, f, []storage.Model{{ID: "model-a", Family: "A"}}, 40*time.Millisecond, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	waitForCond(t, 3*time.Second, func() bool { return f.calls("model-a") >= 3 })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// Ошибка одного цикла не останавливает цикл последующие.
func TestRunContinuesAfterFailedCycle(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) }
	var calls, okCycles int32
	r, _ := newRunnerAt(t, f, []storage.Model{{ID: "model-a", Family: "A"}}, 40*time.Millisecond,
		func(res CycleResult) ([]storage.OutboxEvent, error) {
			if atomic.AddInt32(&calls, 1) == 1 {
				return nil, errors.New("prepare events failed")
			}
			atomic.AddInt32(&okCycles, 1)
			return nil, nil
		})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	waitForCond(t, 3*time.Second, func() bool { return atomic.LoadInt32(&okCycles) >= 2 })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// Отмена ctx во время выполняющегося цикла: цикл завершается и фиксируется,
// Run возвращается только после его конца (не посередине).
func TestRunCancelDuringCycleWaitsForIt(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	entered := make(chan struct{})
	release := make(chan struct{})
	var hookCalls int32
	f.hook = func(id string) *api.RawResult {
		if atomic.AddInt32(&hookCalls, 1) == 1 {
			close(entered) // первый цикл завис в проверке
			<-release
		}
		return okResult(id, 1_000_000, 40)
	}
	r, s := newRunnerAt(t, f, []storage.Model{{ID: "model-a", Family: "A"}}, 40*time.Millisecond, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	<-entered
	cancel() // штатная остановка во время выполняющегося цикла
	select {
	case <-done:
		t.Fatal("Run must not return while the in-flight cycle is still running")
	case <-time.After(150 * time.Millisecond):
	}
	close(release) // цикл может завершиться
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	// цикл обязан был зафиксироваться до возврата Run
	m, err := s.Model("model-a")
	if err != nil {
		t.Fatal(err)
	}
	if m == nil || m.LastStatus != StatusOperational {
		t.Fatalf("in-flight cycle must be committed before Run returns: %+v", m)
	}
}

// --- Этап 2: переходы состояний ---

// 2.1: 🟢 → ⚪ → 🟡 — деградация, обнаруженная после слепого окна,
// обязана дойти до подписчика: событие From=⚪ (длительность неизвестна —
// не выдумываем), инцидент открывается.
func TestDegradationAfterBlindnessNotified(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) }
	r, _ := newRunner(t, f, []storage.Model{{ID: "model-a", Family: "A"}})
	if _, err := r.RunCycle(context.Background()); err != nil { // 🟢 базлайн
		t.Fatal(err)
	}
	f.hook = func(id string) *api.RawResult { return errResult(id, 401, "Invalid API key") }
	if _, err := r.RunCycle(context.Background()); err != nil { // ⚪ слепота
		t.Fatal(err)
	}
	f.hook = func(id string) *api.RawResult { return errResult(id, 429, "rate limit hit") }
	res, err := r.RunCycle(context.Background()) // зрение вернулось: 🟡
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Transitions) != 1 {
		t.Fatalf("degradation after blindness must reach subscribers: %+v", res.Transitions)
	}
	tr := res.Transitions[0]
	if tr.From != StatusUnknown || tr.To != StatusDegraded {
		t.Fatalf("transition = %+v", tr)
	}
	if tr.Incident != nil && tr.Incident.Duration > 0 {
		t.Fatalf("start time unseen by the monitor — duration must not be fabricated: %+v", tr.Incident)
	}
	if open, _ := r.Store().HasOpenIncident("model-a"); !open {
		t.Fatal("incident must open on the discovered degradation")
	}
	m, _ := r.Store().Model("model-a")
	if m.LastStatus != StatusDegraded {
		t.Fatalf("state must be committed as Degraded: %+v", m)
	}
}

// 2.2: 🟡 → ⚪ → 🟡 при живом согласованном инциденте: ни повторного события,
// ни второго инцидента, ни сброса длительности; история переживает рестарт.
func TestDegradedBlindDegradedKeepsIncident(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/test.db"
	mkRunner := func() *Runner {
		s, err := storage.Open(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.SyncModels([]storage.Model{{ID: "model-a", Family: "A"}}); err != nil {
			t.Fatal(err)
		}
		f := newFakeAPI([]string{"model-a"})
		f.hook = func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) }
		return NewRunner(RunnerCfg{API: f, Store: s, Models: []storage.Model{{ID: "model-a", Family: "A"}},
			Concurrency: 2, MaxTokens: 8, Interval: time.Minute, Perf: PerfCfg{MinSamples: 12, LatencyFactor: 3.0, TPSShare: 0.4}})
	}
	r := mkRunner()
	if _, err := r.RunCycle(context.Background()); err != nil { // 🟢
		t.Fatal(err)
	}
	r.cfg.API.(*fakeAPI).hook = func(id string) *api.RawResult { return errResult(id, 429, "rate limit") }
	if _, err := r.RunCycle(context.Background()); err != nil { // 🟡: событие + инцидент
		t.Fatal(err)
	}
	inc, err := r.Store().CurrentIncident("model-a")
	if err != nil || inc == nil {
		t.Fatalf("degradation must open an incident: %v %v", inc, err)
	}
	origID := inc.ID
	r.cfg.API.(*fakeAPI).hook = func(id string) *api.RawResult { return errResult(id, 401, "Invalid API key") }
	if _, err := r.RunCycle(context.Background()); err != nil { // ⚪ слепота
		t.Fatal(err)
	}
	// рестарт: раннер на той же базе
	r2 := mkRunner()
	r2.cfg.API.(*fakeAPI).hook = func(id string) *api.RawResult { return errResult(id, 429, "rate limit") }
	res, err := r2.RunCycle(context.Background()) // снова 🟡
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Transitions) != 0 {
		t.Fatalf("same-state degradation must not produce a repeat event: %+v", res.Transitions)
	}
	inc2, err := r2.Store().CurrentIncident("model-a")
	if err != nil || inc2 == nil {
		t.Fatalf("incident must stay open: %v %v", inc2, err)
	}
	if inc2.ID != origID {
		t.Fatalf("same continuous degradation must keep the SAME incident: was %d, now %d (duplicated)", origID, inc2.ID)
	}
	// восстановление: событие с длительностью от исходной деградации
	time.Sleep(10 * time.Millisecond)
	r2.cfg.API.(*fakeAPI).hook = func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) }
	res, err = r2.RunCycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Transitions) != 1 {
		t.Fatalf("recovery must emit: %+v", res.Transitions)
	}
	rec := res.Transitions[0]
	if rec.To != StatusOperational {
		t.Fatalf("recovery transition = %+v", rec)
	}
	if rec.Incident == nil || rec.Incident.ID != origID {
		t.Fatalf("recovery must close the original incident: %+v", rec.Incident)
	}
	if !rec.Incident.Started.Equal(inc.Started) {
		t.Fatalf("incident start must not be reset: was %v, got %v", inc.Started, rec.Incident.Started)
	}
	if rec.Incident.Duration <= 0 {
		t.Fatalf("continuous degradation duration must survive: %+v", rec.Incident)
	}
	if open, _ := r2.Store().HasOpenIncident("model-a"); open {
		t.Fatal("recovery must close the incident")
	}
}

// 2.3: инцидент модели, покинувшей конфигурацию, не остаётся открытым
// навсегда — закрывается при синхронизации, возврат модели ничего не ломает.
func TestRemovedModelIncidentNotOrphaned(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult { return errResult(id, 503, "Service Unavailable") }
	models := []storage.Model{{ID: "model-a", Family: "A"}}
	s, err := storage.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if err := s.SyncModels(models); err != nil {
		t.Fatal(err)
	}
	r := NewRunner(RunnerCfg{API: f, Store: s, Models: models, Concurrency: 1, MaxTokens: 8,
		Interval: time.Minute, Perf: PerfCfg{MinSamples: 12, LatencyFactor: 3.0, TPSShare: 0.4}})
	if _, err := r.RunCycle(context.Background()); err != nil { // 🔴 + инцидент
		t.Fatal(err)
	}
	if open, _ := s.HasOpenIncident("model-a"); !open {
		t.Fatal("setup: incident must be open while the model is down")
	}
	// model-a покидает конфигурацию
	if err := s.SyncModels([]storage.Model{{ID: "model-b", Family: "B"}}); err != nil {
		t.Fatal(err)
	}
	if open, _ := s.HasOpenIncident("model-a"); open {
		t.Fatal("removed model must not leave an orphaned open incident (2.3)")
	}
	// возврат модели в конфиг и Operational: сирот не заводится
	if err := s.SyncModels(models); err != nil {
		t.Fatal(err)
	}
	r2 := NewRunner(RunnerCfg{API: f, Store: s, Models: models, Concurrency: 1, MaxTokens: 8,
		Interval: time.Minute, Perf: PerfCfg{MinSamples: 12, LatencyFactor: 3.0, TPSShare: 0.4}})
	f.hook = func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) }
	res, err := r2.RunCycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if open, _ := s.HasOpenIncident("model-a"); open {
		t.Fatal("returned Operational model must not have an open incident")
	}
	if len(res.Transitions) != 0 {
		t.Fatalf("first sighting after return must not emit: %+v", res.Transitions)
	}
}

// 2.4: очистка старых инцидентов не должна ломать фиксацию текущего цикла:
// цикл, закрывающий инцидент ровно до границы retention, обязан коммититься.
func TestRetentionCleanupDoesNotBreakCycleCommit(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) }
	models := []storage.Model{{ID: "model-a", Family: "A"}}
	s, err := storage.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if err := s.SyncModels(models); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-31 * 24 * time.Hour)
	if err := s.SetModelState("model-a", StatusDown, "http", "Service Unavailable", old); err != nil {
		t.Fatal(err)
	}
	if err := s.OpenIncident("model-a", StatusDown, old); err != nil {
		t.Fatal(err)
	}
	r := NewRunner(RunnerCfg{API: f, Store: s, Models: models, Concurrency: 1, MaxTokens: 8,
		Interval: time.Minute, Perf: PerfCfg{MinSamples: 12, LatencyFactor: 3.0, TPSShare: 0.4}})
	res, err := r.RunCycle(context.Background()) // восстановление закрывает старый инцидент
	if err != nil {
		t.Fatalf("retention cleanup must not break the cycle commit: %v", err)
	}
	if len(res.Transitions) != 1 || res.Transitions[0].To != StatusOperational {
		t.Fatalf("recovery expected: %+v", res.Transitions)
	}
	if res.Transitions[0].Incident == nil || res.Transitions[0].Incident.Duration < 30*24*time.Hour {
		t.Fatalf("incident duration must span from the original fall: %+v", res.Transitions[0].Incident)
	}
	if open, _ := s.HasOpenIncident("model-a"); open {
		t.Fatal("incident must be closed by the recovery")
	}
	m, _ := s.Model("model-a")
	if m.LastStatus != StatusOperational {
		t.Fatalf("state must be committed: %+v", m)
	}
}

// Матрица значимых переходов prev × cur: согласованные событие и инциденты.
func TestTransitionMatrix(t *testing.T) {
	type tcase struct {
		name       string
		prev, cur  Status
		seedInc    Status // "" — инцидент не сеем (prev считается живым состоянием)
		wantEvents int
		wantOpen   bool
		keepID     bool // открытый инцидент сохраняет ID после цикла
	}
	cases := []tcase{
		{"green→green", StatusOperational, StatusOperational, "", 0, false, false},
		{"yellow→yellow", StatusDegraded, StatusDegraded, StatusDegraded, 0, true, true},
		{"red→red", StatusDown, StatusDown, StatusDown, 0, true, true},
		{"unknown→unknown", StatusUnknown, StatusUnknown, "", 0, false, false},
		{"green→yellow", StatusOperational, StatusDegraded, "", 1, true, false},
		{"green→red", StatusOperational, StatusDown, "", 1, true, false},
		{"yellow→green", StatusDegraded, StatusOperational, StatusDegraded, 1, false, false},
		{"red→green", StatusDown, StatusOperational, StatusDown, 1, false, false},
		{"yellow→red", StatusDegraded, StatusDown, StatusDegraded, 1, true, false},
		{"red→yellow", StatusDown, StatusDegraded, StatusDown, 1, true, false},
		{"green→unknown", StatusOperational, StatusUnknown, "", 0, false, false},
		{"yellow→unknown", StatusDegraded, StatusUnknown, StatusDegraded, 0, true, true},
		{"red→unknown", StatusDown, StatusUnknown, StatusDown, 0, true, true},
		// восстановление из ⚪ без инцидента (был 🟢): события нет
		{"unknown→green,no-inc", StatusUnknown, StatusOperational, "", 0, false, false},
		// восстановление из ⚪ с открытым инцидентом: событие + закрытие
		{"unknown→green,inc", StatusUnknown, StatusOperational, StatusDown, 1, false, false},
		// падение в слепое окно без инцидента: событие + инцидент (D2)
		{"unknown→red,no-inc", StatusUnknown, StatusDown, "", 1, true, false},
		// 🔴 живёт через ⚪: инцидент тот же, события нет (Rev-5)
		{"unknown→red,inc", StatusUnknown, StatusDown, StatusDown, 0, true, true},
		// 2.2: 🟡 живёт через ⚪: инцидент тот же, события нет
		{"unknown→yellow,inc", StatusUnknown, StatusDegraded, StatusDegraded, 0, true, true},
		// 2.1: 🟡 обнаружена в слепое окно: событие + инцидент
		{"unknown→yellow,no-inc", StatusUnknown, StatusDegraded, "", 1, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeAPI([]string{"model-a"})
			f.hook = resultHook(tc.cur)

			models := []storage.Model{{ID: "model-a", Family: "A"}}
			r, _ := newRunner(t, f, models) // SyncModels здесь
			if err := r.Store().SetModelState("model-a", tc.prev, "", "", time.Now().Add(-time.Minute)); err != nil {
				t.Fatal(err)
			}
			var origID int64
			if tc.seedInc != "" {
				if err := r.Store().OpenIncident("model-a", tc.seedInc, time.Now().Add(-time.Minute)); err != nil {
					t.Fatal(err)
				}
				inc, err := r.Store().CurrentIncident("model-a")
				if err != nil || inc == nil {
					t.Fatalf("seed incident: %v %v", inc, err)
				}
				origID = inc.ID
			}
			res, err := r.RunCycle(context.Background())
			if err != nil {
				t.Fatalf("cycle: %v", err)
			}
			if len(res.Transitions) != tc.wantEvents {
				t.Fatalf("events = %d, want %d: %+v", len(res.Transitions), tc.wantEvents, res.Transitions)
			}
			inc, _ := r.Store().CurrentIncident("model-a")
			if tc.wantOpen && inc == nil {
				t.Fatalf("incident must be open after the cycle")
			}
			if !tc.wantOpen && inc != nil {
				t.Fatalf("incident must not be open after the cycle: %+v", inc)
			}
			if tc.keepID {
				if inc == nil || inc.ID != origID {
					t.Fatalf("incident ID must be preserved (was %d, now %v)", origID, inc)
				}
			}
			m, _ := r.Store().Model("model-a")
			if m.LastStatus != tc.cur {
				t.Fatalf("committed state = %q, want %q", m.LastStatus, tc.cur)
			}
		})
	}
}

// resultHook возвращает API-ответ, дающий нужный статус классификации.
func resultHook(st Status) func(string) *api.RawResult {
	switch st {
	case StatusOperational:
		return func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) }
	case StatusDegraded:
		return func(id string) *api.RawResult { return errResult(id, 429, "most likely rate limit") }
	case StatusDown:
		return func(id string) *api.RawResult { return errResult(id, 503, "Service Unavailable") }
	default:
		return func(id string) *api.RawResult { return errResult(id, 401, "Invalid API key") }
	}
}
