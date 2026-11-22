package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moloo4ni/opentw/internal/api"
	"github.com/moloo4ni/opentw/internal/storage"
)

// fakeAPI is a scriptable api.Client stand-in.
type fakeAPI struct {
	mu            sync.Mutex
	catalog       []string
	catErr        error
	hook          func(modelID string) *api.RawResult
	genCalls      map[string]int
	lastMaxTokens int
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
	f.lastMaxTokens = maxTokens
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

func (f *fakeAPI) usedMaxTokens() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastMaxTokens
}

func ptrLat(v int64) *int64 { return &v }

// okResult — a successful check: the final answer is exactly "OK"
// (Content/Response as api.Client fills them).
func okResult(id string, latUS int64, tps float64) *api.RawResult {
	return &api.RawResult{
		ModelID: id, Timestamp: time.Now(), ErrorKind: "", HTTPStatus: 200,
		LatencyUS: latUS, TTFTUS: ptrLat(latUS / 3), GenUS: ptrLat(latUS * 2 / 3),
		TPS: &tps, InputTokens: ptrLat(10), OutputTokens: ptrLat(8),
		Content: "OK", Response: "OK",
		Streamed: true,
	}
}

// errResult — the provider's HTTP error: Response carries its text verbatim.
func errResult(id string, status int, msg string) *api.RawResult {
	return &api.RawResult{ModelID: id, Timestamp: time.Now(), ErrorKind: "http", HTTPStatus: status, ErrorMessage: msg, Response: msg, LatencyUS: 40_000}
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
		OnCycle: func(res CycleResult) ([]storage.OutboxEvent, error) {
			*results = append(*results, res)
			return nil, nil
		},
	})
	return r, results
}

// The cycle schedule is bound to interval-grid boundaries, not to the start
// moment: at 15m checks run at :00/:15/:30/:45 regardless of when the
// process started.
func TestNextTickAligned(t *testing.T) {
	m := func(h, min, s int) time.Time { return time.Date(2026, 11, 16, h, min, s, 0, time.UTC) }
	for _, tc := range []struct {
		from, want time.Time
	}{
		{m(13, 52, 6), m(14, 0, 0)},   // start between boundaries → nearest one
		{m(14, 0, 0), m(14, 15, 0)},   // exactly on a boundary → next one
		{m(14, 14, 59), m(14, 15, 0)}, // a second before the boundary
	} {
		if got := NextTick(15*time.Minute, tc.from); !got.Equal(tc.want) {
			t.Fatalf("NextTick(15m, %s) = %s, want %s", tc.from, got, tc.want)
		}
	}
	if got := NextTick(time.Hour, m(13, 30, 0)); !got.Equal(m(14, 0, 0)) {
		t.Fatalf("NextTick(1h, 13:30) = %s, want 14:00", got)
	}
}

// A previous-states read failure aborts the cycle: without them a correct
// transition classification is impossible (duplicate incidents would open).
func TestCycleAbortsWhenStatesUnreadable(t *testing.T) {
	f := newFakeAPI([]string{"model-a", "model-b"})
	f.hook = func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) }
	r, _ := newRunner(t, f, basicModels())
	if _, err := r.RunCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	r.Store().Close() // further storage operations must fail
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

// The configured limit reaches every real generation without per-model exceptions.
func TestRunnerUsesConfiguredMaxTokens(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) }
	r, _ := newRunner(t, f, []storage.Model{{ID: "model-a", Family: "A"}})
	r.cfg.MaxTokens = 256
	if _, err := r.RunCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.usedMaxTokens(); got != 256 {
		t.Fatalf("Generate received max_tokens=%d, want 256", got)
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
	// Blind-window case B: the first sighting of Down is the baseline without
	// a notification, but the incident (duration accounting) opens.
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

// HTTP 200 with a foreign text instead of OK: Down, error_type "response",
// Response is the verbatim text, ErrorMessage empty.
func TestForeignTextDownVerbatimResponse(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	foreign := "抱歉，无法处理该请求。"
	f.hook = func(id string) *api.RawResult {
		return &api.RawResult{ModelID: id, Timestamp: time.Now(), ErrorKind: "", HTTPStatus: 200,
			Content: foreign, Response: foreign, LatencyUS: 500_000}
	}
	r, _ := newRunner(t, f, []storage.Model{{ID: "model-a", Family: "A"}})
	res, err := r.RunCycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	c := res.Checks["model-a"]
	if c.Status != StatusDown {
		t.Fatalf("status = %q, want down", c.Status)
	}
	if c.ErrorType != "response" {
		t.Fatalf("error type = %q, want response", c.ErrorType)
	}
	if c.ErrorMessage != "" {
		t.Fatalf("no reason is invented for a bad answer: %q", c.ErrorMessage)
	}
	if c.Response != foreign {
		t.Fatalf("response must keep the verbatim text: %q", c.Response)
	}
	m, _ := r.Store().Model("model-a")
	if m.LastStatus != StatusDown {
		t.Fatalf("state = %q, want down", m.LastStatus)
	}
	last, _ := r.Store().LastChecks()
	if last["model-a"].Response != foreign || last["model-a"].ErrorType != "response" {
		t.Fatalf("check row lost the verbatim response: %+v", last["model-a"])
	}
}

// The check row carries the verbatim Response and the short ErrorType from
// RawResult: the provider's text on an http error, an empty Response on a
// transport failure.
func TestCheckRowCarriesResponseAndErrorType(t *testing.T) {
	f := newFakeAPI([]string{"model-a", "model-b", "model-c"})
	f.hook = func(id string) *api.RawResult {
		switch id {
		case "model-a":
			return errResult(id, 429, "Too Many Requests")
		case "model-b":
			return &api.RawResult{ModelID: id, Timestamp: time.Now(), ErrorKind: "timeout",
				ErrorMessage: "context deadline exceeded", LatencyUS: 9_000_000}
		default:
			return okResult(id, 1_000_000, 40)
		}
	}
	r, _ := newRunner(t, f, basicModelsPlusC())
	if _, err := r.RunCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	last, _ := r.Store().LastChecks()
	if c := last["model-a"]; c.ErrorType != "http" || c.Response != "Too Many Requests" {
		t.Fatalf("http check must carry the provider text verbatim: %+v", c)
	}
	if c := last["model-b"]; c.ErrorType != "timeout" || c.Response != "" {
		t.Fatalf("transport error has no quotable response: %+v", c)
	}
	if c := last["model-c"]; c.Response != "OK" || c.ErrorType != "" {
		t.Fatalf("successful check must carry the generated answer: %+v", c)
	}
}

// An empty final answer with finish_reason=length stays Down, but the
// completion reason is preserved via the existing diagnostic field, without
// a migration.
func TestEmptyResponseFinishReasonPersisted(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult {
		return &api.RawResult{
			ModelID: id, Timestamp: time.Now(), ErrorKind: "", HTTPStatus: 200,
			LatencyUS: 4_100_000, OutputTokens: ptrLat(64), FinishReason: "length",
		}
	}
	r, _ := newRunner(t, f, []storage.Model{{ID: "model-a", Family: "A"}})
	res, err := r.RunCycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	c := res.Checks["model-a"]
	if c.Status != StatusDown {
		t.Fatalf("empty final content must stay Down, got %q", c.Status)
	}
	if c.Response != "" {
		t.Fatalf("empty final content must not be replaced: %q", c.Response)
	}
	if c.ErrorType != "response" {
		t.Fatalf("error type = %q, want response", c.ErrorType)
	}
	if c.ErrorMessage != "finish_reason=length" {
		t.Fatalf("finish reason diagnostic = %q, want finish_reason=length", c.ErrorMessage)
	}
	last, _ := r.Store().LastChecks()
	if last["model-a"].ErrorMessage != "finish_reason=length" {
		t.Fatalf("persisted finish reason = %q, want finish_reason=length", last["model-a"].ErrorMessage)
	}
}

// A provider message must not be overwritten with the completion diagnostic.
func TestEmptyResponseFinishReasonPreservesProviderMessage(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult {
		return &api.RawResult{
			ModelID: id, Timestamp: time.Now(), ErrorKind: "empty", HTTPStatus: 200,
			LatencyUS: 4_100_000, ErrorMessage: "No available accounts", Response: "No available accounts",
			FinishReason: "length",
		}
	}
	r, _ := newRunner(t, f, []storage.Model{{ID: "model-a", Family: "A"}})
	res, err := r.RunCycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	c := res.Checks["model-a"]
	if c.Status != StatusDown {
		t.Fatalf("status = %q, want Down", c.Status)
	}
	if c.ErrorMessage != "No available accounts; finish_reason=length" {
		t.Fatalf("provider message was overwritten: %q", c.ErrorMessage)
	}
}

func basicModelsPlusC() []storage.Model {
	return append(basicModels(), storage.Model{ID: "model-c", Family: "C"})
}

// A very high latency with an OK answer is Operational: no
// "performance degradation" exists anymore.
func TestHighLatencyOKStaysOperational(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult { return okResult(id, 120_000_000, 1) }
	r, _ := newRunner(t, f, []storage.Model{{ID: "model-a", Family: "A"}})
	res, err := r.RunCycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	c := res.Checks["model-a"]
	if c.Status != StatusOperational {
		t.Fatalf("high latency with OK must stay operational, got %q", c.Status)
	}
	if c.LatencyUS == nil || *c.LatencyUS != 120_000_000 {
		t.Fatalf("latency must still be recorded: %+v", c)
	}
}

func TestCatalogAbsenceStillChecks(t *testing.T) {
	f := newFakeAPI([]string{"model-a"}) // model-b is absent from the catalog
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

func TestUnknownStatusNoEvents(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) }
	r, _ := newRunner(t, f, []storage.Model{{ID: "model-a", Family: "A"}})
	if _, err := r.RunCycle(context.Background()); err != nil { // operational baseline
		t.Fatal(err)
	}
	f.hook = func(id string) *api.RawResult { return errResult(id, 401, "Invalid API key") }
	res2, err := r.RunCycle(context.Background()) // a monitor-side failure → Unknown
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

// A transport failure on the catalog + transport errors on the models → all
// ⚪ Unknown ("the monitor cannot check"), not 27 false "model down".
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

// A 429 catalog (the monitor's own key limit) + models in 429 → Unknown for
// the models.
func TestCatalogRatelimitMarksUnknown(t *testing.T) {
	f := newFakeAPI(nil)
	f.catErr = errors.New("api: models: HTTP 429: Too Many Requests")
	f.hook = func(id string) *api.RawResult {
		if id == "model-b" { // a model with a different error keeps its own status
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

// A live catalog: the models' transport errors are honest Down (not
// "monitor problem").
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

// Silent recovery from Unknown is gone: after an Unknown period (a broken
// key) the model's recovery reaches subscribers, the incident duration is
// honest.
func TestRecoveryFromUnknownEmitsEvent(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult { return errResult(id, 503, "Service Unavailable") }
	r, _ := newRunner(t, f, []storage.Model{{ID: "model-a", Family: "A"}})
	if _, err := r.RunCycle(context.Background()); err != nil { // first sighting of Down
		t.Fatal(err)
	}
	f.hook = func(id string) *api.RawResult { return errResult(id, 500, "keep down") }
	if _, err := r.RunCycle(context.Background()); err != nil { // 503 → 500: Down→Down, the incident already opened at 503
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

// A short ⚪ blip without an open incident (previously 🟢) → no recovery event.
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

// A restart during an incident: a persistent Down emits no repeated events.
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
		return NewRunner(RunnerCfg{API: f, Store: s, Models: basicModels(), Concurrency: 2, MaxTokens: 8, Interval: time.Minute})
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

// The cycle cleans up incidents older than retention.
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

// The heartbeat file is updated at the end of every cycle (the HEALTHCHECK basis).
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

// A garbage catalog response is not a transport failure: it must not mask
// matching model errors.
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

// Unknown→Down does not restart the incident: the duration counts from the
// original fall, not from the monitor "regaining sight".
func TestUnknownToDownKeepsOriginalIncident(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult { return errResult(id, 503, "Service Unavailable") }
	r, _ := newRunner(t, f, []storage.Model{{ID: "model-a", Family: "A"}})
	if _, err := r.RunCycle(context.Background()); err != nil { // Down
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	f.hook = func(id string) *api.RawResult { return errResult(id, 401, "Invalid API key") }
	if _, err := r.RunCycle(context.Background()); err != nil { // ⚪ (the incident stays open)
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	f.hook = func(id string) *api.RawResult { return errResult(id, 503, "Service Unavailable") }
	if _, err := r.RunCycle(context.Background()); err != nil { // Down again
		t.Fatal(err)
	}
	// Blind-window case C: the old incident is not duplicated and stays open —
	// the duration counts from the original fall, not from "regaining sight".
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

// An event-preparation failure (subscribers unavailable) must not commit
// states — otherwise the cycle's events would be lost forever (the next
// cycle would see prev == cur). The next cycle must reopen the transition.
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

	// baseline: the model works
	if _, err := r.RunCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	// the model falls, but committing is impossible
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
	// delivery recovered: the next cycle must reopen the transition
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

// A failed cycle (dead database) does not refresh the heartbeat — the
// HEALTHCHECK must go red instead of showing an eternally live monitor.
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

	// The database is dead: reading previous states fails. The first failed
	// cycle still had a right to the startup heartbeat (the previous cycle
	// succeeded, the monitor was alive at start), a second consecutive one
	// does not.
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

// A restart mid-cycle: states committed before the restart do not outrun
// the events — they are applied by one commit at the end of the cycle.
// Verified through OnCycle: events prepared together with the states land
// in the outbox by the same commit.
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
	// the first cycle: no transitions
	if _, err := r.RunCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	// the model falls — the event is prepared and committed by the cycle's commit
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

// Blind-window case A: the model was 🟢, the monitor went temporarily blind
// (401 → ⚪), the model really fell during it, sight returned and the API
// confirms Down. The fall must reach the subscriber as an event, the
// incident must open; the unavailability start time is unknown and the
// duration is not fabricated.
func TestFallDuringBlindnessNotified(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) }
	r, _ := newRunner(t, f, []storage.Model{{ID: "model-a", Family: "A"}})
	if _, err := r.RunCycle(context.Background()); err != nil { // 🟢 baseline
		t.Fatal(err)
	}
	f.hook = func(id string) *api.RawResult { return errResult(id, 401, "Invalid API key") }
	res, err := r.RunCycle(context.Background()) // ⚪ the monitor is blind
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
	res, err = r.RunCycle(context.Background()) // sight returned: a confirmed fall
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
	// Blind-window case D: the recovery carries an honest duration — from
	// the moment the fall was discovered (not fabricated).
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

// Incidents are applied only together with the cycle's commit. An
// event-preparation failure after planning leaves neither an open incident
// nor duplicates after a successful retry; the recovery closes exactly one
// incident.
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
	// the cycle planned the incident but failed before the commit: nothing is recorded
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
	// a successful retry opens the incident exactly once
	fail = false
	if _, err := r.RunCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if open, _ := s.HasOpenIncident("model-a"); !open {
		t.Fatal("successful cycle must open the incident")
	}
	// the recovery closes it; with duplicates the old one would stay open
	// (only the newest is closed) — the duration would be lost
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

// Classification of provider offline statements: the live kimi-k2.6 reply
// "The model kimi-k2.6 has been offline" (HTTP 400) is Down, not Unknown:
// an event to subscribers, an incident, the verbatim reason.
func TestProviderOfflineMessageIsDown(t *testing.T) {
	f := newFakeAPI([]string{"kimi-k2.6"})
	f.hook = func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) }
	r, _ := newRunner(t, f, []storage.Model{{ID: "kimi-k2.6", Family: "KIMI"}})
	if _, err := r.RunCycle(context.Background()); err != nil { // 🟢 baseline
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

// --- Stage: the Runner.Run main loop ---

// newRunnerAt creates a runner with a configurable interval and OnCycle (for Run tests).
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
		Interval: interval,
		OnCycle:  onCycle,
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

// The first cycle runs immediately, without waiting for the interval.
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

// Run performs several cycles on the interval-grid schedule.
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

// One cycle's failure does not stop the following cycles.
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

// Cancelling ctx during an in-flight cycle: the cycle finishes and commits;
// Run returns only after its end (not midway).
func TestRunCancelDuringCycleWaitsForIt(t *testing.T) {
	f := newFakeAPI([]string{"model-a"})
	entered := make(chan struct{})
	release := make(chan struct{})
	var hookCalls int32
	f.hook = func(id string) *api.RawResult {
		if atomic.AddInt32(&hookCalls, 1) == 1 {
			close(entered) // the first cycle is stuck in a check
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
	cancel() // a normal shutdown during an in-flight cycle
	select {
	case <-done:
		t.Fatal("Run must not return while the in-flight cycle is still running")
	case <-time.After(150 * time.Millisecond):
	}
	close(release) // the cycle may finish
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	// the cycle must have been committed before Run returned
	m, err := s.Model("model-a")
	if err != nil {
		t.Fatal(err)
	}
	if m == nil || m.LastStatus != StatusOperational {
		t.Fatalf("in-flight cycle must be committed before Run returns: %+v", m)
	}
}

// --- Stage: state transitions ---

// The incident of a model that left the configuration does not stay open
// forever — it is closed on sync; the model's return breaks nothing.
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
		Interval: time.Minute})
	if _, err := r.RunCycle(context.Background()); err != nil { // Down + an incident
		t.Fatal(err)
	}
	if open, _ := s.HasOpenIncident("model-a"); !open {
		t.Fatal("setup: incident must be open while the model is down")
	}
	// model-a leaves the configuration
	if err := s.SyncModels([]storage.Model{{ID: "model-b", Family: "B"}}); err != nil {
		t.Fatal(err)
	}
	if open, _ := s.HasOpenIncident("model-a"); open {
		t.Fatal("removed model must not leave an orphaned open incident (2.3)")
	}
	// the model returns to the config and is Operational: no orphan appears
	if err := s.SyncModels(models); err != nil {
		t.Fatal(err)
	}
	r2 := NewRunner(RunnerCfg{API: f, Store: s, Models: models, Concurrency: 1, MaxTokens: 8,
		Interval: time.Minute})
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

// Retention cleanup must not break the current cycle's commit: a cycle
// closing an incident right at the retention boundary must commit.
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
		Interval: time.Minute})
	res, err := r.RunCycle(context.Background()) // the recovery closes the old incident
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

// The matrix of meaningful prev × cur transitions (three statuses):
// consistent events and incidents. Degraded does not exist.
func TestTransitionMatrix(t *testing.T) {
	type tcase struct {
		name       string
		prev, cur  Status
		seedInc    Status // "" — no seeded incident (prev counts as the live state)
		wantEvents int
		wantOpen   bool
		keepID     bool // the open incident keeps its ID after the cycle
	}
	cases := []tcase{
		{"green→green", StatusOperational, StatusOperational, "", 0, false, false},
		{"red→red", StatusDown, StatusDown, StatusDown, 0, true, true},
		{"unknown→unknown", StatusUnknown, StatusUnknown, "", 0, false, false},
		{"green→red", StatusOperational, StatusDown, "", 1, true, false},
		{"red→green", StatusDown, StatusOperational, StatusDown, 1, false, false},
		{"green→unknown", StatusOperational, StatusUnknown, "", 0, false, false},
		// the incident survives the monitor's blindness: no event, no duplicate
		{"red→unknown,inc", StatusDown, StatusUnknown, StatusDown, 0, true, true},
		// recovery from ⚪ without an incident (was 🟢): no event
		{"unknown→green,no-inc", StatusUnknown, StatusOperational, "", 0, false, false},
		// recovery from ⚪ with an open incident: event + closing
		{"unknown→green,inc", StatusUnknown, StatusOperational, StatusDown, 1, false, false},
		// a fall into the blind window without an incident: event + incident
		{"unknown→red,no-inc", StatusUnknown, StatusDown, "", 1, true, false},
		// Down lives through ⚪: same incident, no event
		{"unknown→red,inc", StatusUnknown, StatusDown, StatusDown, 0, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeAPI([]string{"model-a"})
			f.hook = resultHook(tc.cur)

			models := []storage.Model{{ID: "model-a", Family: "A"}}
			r, _ := newRunner(t, f, models) // includes SyncModels
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

// resultHook returns an API answer yielding the wanted classification.
func resultHook(st Status) func(string) *api.RawResult {
	switch st {
	case StatusOperational:
		return func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) }
	case StatusDown:
		return func(id string) *api.RawResult { return errResult(id, 503, "Service Unavailable") }
	default:
		return func(id string) *api.RawResult { return errResult(id, 401, "Invalid API key") }
	}
}

// --- Operational signal on "all Unknown" ---

// The key is dead — all models ⚪, cycles succeed, the heartbeat is alive
// (the container looks healthy). The operator must see a dedicated ERROR
// signal: "no meaningful checks for N consecutive cycles". It changes
// neither model statuses nor /status.
func TestProlongedAllUnknownSignalsOperations(t *testing.T) {
	var buf syncBuffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))
	f := newFakeAPI([]string{"model-a"})
	f.hook = func(id string) *api.RawResult { return errResult(id, 401, "Invalid API key") }
	r, _ := newRunner(t, f, []storage.Model{{ID: "model-a", Family: "A"}})
	r.log = log

	for i := 0; i < 3; i++ {
		if _, err := r.RunCycle(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	m, _ := r.Store().Model("model-a")
	if m.LastStatus != StatusUnknown {
		t.Fatalf("model semantics must not change: %q", m.LastStatus)
	}
	msgs := strings.Split(buf.String(), "\n")
	signal := 0
	for _, ln := range msgs {
		if strings.Contains(ln, "no meaningful checks") {
			signal++
		}
	}
	if signal < 2 { // the second and third consecutive cycles — a signal on each
		t.Fatalf("operational ERROR signal expected on consecutive all-unknown cycles, got %d:\n%s", signal, buf.String())
	}
	// meaning restored: the signal resets
	f.hook = func(id string) *api.RawResult { return okResult(id, 1_000_000, 40) }
	if _, err := r.RunCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Count(buf.String(), "no meaningful checks") != signal {
		t.Fatal("streak must reset on a meaningful cycle")
	}
}

// syncBuffer is a goroutine-safe slog buffer (the cycle writes from one
// goroutine, but slog.NewJSONHandler requires a race-free Writer).
type syncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
