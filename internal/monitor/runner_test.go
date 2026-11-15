package monitor

import (
	"context"
	"errors"
	"sync"
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
		OnCycle:     func(res CycleResult) { *results = append(*results, res) },
	})
	return r, results
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
