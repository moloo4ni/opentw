package monitor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"opentw/internal/api"
	"opentw/internal/status"
	"opentw/internal/storage"
)

// ErrCycleRunning means another monitoring cycle is in progress.
var ErrCycleRunning = errors.New("monitor: cycle already running")

// ModelChecker is the API surface the runner needs (satisfied by api.Client).
type ModelChecker interface {
	ListModels(ctx context.Context) ([]string, error)
	Generate(ctx context.Context, modelID string, maxTokens int) *api.RawResult
}

// Transition is one model state change observed in a cycle.
type Transition struct {
	ModelID  string
	From, To status.Status
	Check    storage.Check
	Incident *storage.Incident // incident of the previous state (its duration)
}

// CycleResult is the snapshot handed to the bot after each cycle.
type CycleResult struct {
	CycleAt     time.Time
	NextAt      time.Time
	Checks      map[string]storage.Check
	Transitions []Transition
}

// RunnerCfg configures the monitoring runner.
type RunnerCfg struct {
	API         ModelChecker
	Store       *storage.Store
	Models      []storage.Model // configured models from config.yaml
	Concurrency int
	MaxTokens   int
	Interval    time.Duration
	Perf        PerfCfg
	Retention   time.Duration // checks older than this are deleted; default 7d
	OnCycle     func(CycleResult)
	Logger      *slog.Logger
}

// Runner runs monitoring cycles: first immediately on start, then every Interval.
type Runner struct {
	cfg     RunnerCfg
	running chan struct{} // closed when idle; used as a single-flight token
	log     *slog.Logger
}

// Store exposes the storage handle (для бота и тестов).
func (r *Runner) Store() *storage.Store { return r.cfg.Store }

// NewRunner builds a runner.
func NewRunner(cfg RunnerCfg) *Runner {
	if cfg.Retention <= 0 {
		cfg.Retention = 7 * 24 * time.Hour
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Runner{cfg: cfg, running: make(chan struct{}, 1), log: log}
}

// Run starts the first cycle immediately and repeats every Interval until ctx
// is cancelled. Errors never stop the loop: a failed cycle is logged and
// retried on the next tick.
func (r *Runner) Run(ctx context.Context) error {
	r.log.Info("monitor: starting", "first_cycle", "immediate", "interval", r.cfg.Interval.String())
	r.runOne(ctx)
	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			r.runOne(ctx)
		}
	}
}

func (r *Runner) runOne(ctx context.Context) {
	if _, err := r.RunCycle(ctx); err != nil && !errors.Is(err, ErrCycleRunning) {
		r.log.Error("monitor: cycle failed", "err", err)
	}
}

// RunCycle performs one monitoring cycle: catalog fetch, one check per
// configured model (bounded concurrency), persistence, incidents and events.
func (r *Runner) RunCycle(ctx context.Context) (*CycleResult, error) {
	select {
	case r.running <- struct{}{}:
		// acquired; released in defer below
	default:
		return nil, ErrCycleRunning
	}
	defer func() { <-r.running }()

	now := time.Now()

	catalog := map[string]bool{}
	catalogOK := true
	ids, err := r.cfg.API.ListModels(ctx)
	if err != nil {
		catalogOK = false
		r.log.Warn("monitor: catalog unavailable, checking all models anyway", "err", err)
	} else {
		for _, id := range ids {
			catalog[id] = true
		}
	}

	prevStates := map[string]storage.Model{}
	if models, err := r.cfg.Store.Models(); err == nil {
		for _, m := range models {
			prevStates[m.ID] = m
		}
	} else {
		r.log.Error("monitor: cannot read models", "err", err)
	}

	var (
		mu          sync.Mutex
		wg          sync.WaitGroup
		sem         = make(chan struct{}, r.cfg.Concurrency)
		checks      = map[string]storage.Check{}
		transitions []Transition
	)

	for _, m := range r.cfg.Models {
		wg.Add(1)
		go func(m storage.Model) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			check, tr, err := r.checkModel(ctx, m, catalog, catalogOK, prevStates[m.ID], now)
			if err != nil {
				r.log.Error("monitor: check failed", "model", m.ID, "err", err)
				return
			}
			mu.Lock()
			checks[check.ModelID] = check
			if tr != nil {
				transitions = append(transitions, *tr)
			}
			mu.Unlock()
		}(m)
	}
	wg.Wait()

	if _, err := r.cfg.Store.DeleteOldChecks(r.cfg.Retention); err != nil {
		r.log.Error("monitor: retention cleanup failed", "err", err)
	}

	res := &CycleResult{
		CycleAt:     now,
		NextAt:      now.Add(r.cfg.Interval),
		Checks:      checks,
		Transitions: transitions,
	}
	if r.cfg.OnCycle != nil {
		r.cfg.OnCycle(*res)
	}
	return res, nil
}

// checkModel runs one model check and records it; it returns the stored check
// and, when a notifiable state change occurred, the transition.
func (r *Runner) checkModel(ctx context.Context, m storage.Model, catalog map[string]bool, catalogOK bool, prev storage.Model, now time.Time) (storage.Check, *Transition, error) {
	if catalogOK && !catalog[m.ID] {
		// исчезла из каталога — проверяем всё равно: вердикт должен
		// быть реальным ответом API, а не синтетической заглушкой
		r.log.Warn("monitor: model missing from catalog, probing anyway", "model", m.ID)
	}
	raw := r.cfg.API.Generate(ctx, m.ID, r.cfg.MaxTokens)
	if raw.ErrorKind == "cancelled" {
		return storage.Check{}, nil, errCancelled
	}

	st := Classify(*raw)
	check := storage.Check{
		ModelID:      m.ID,
		At:           raw.Timestamp,
		HTTPStatus:   raw.HTTPStatus,
		Status:       st,
		LatencyUS:    r.measuredLatency(raw),
		TTFTUS:       raw.TTFTUS,
		GenUS:        raw.GenUS,
		TPS:          raw.TPS,
		InputTokens:  raw.InputTokens,
		OutputTokens: raw.OutputTokens,
		ErrorType:    raw.ErrorKind,
		ErrorMessage: raw.ErrorMessage,
	}

	if st == StatusOperational {
		if r.perfDegraded(m.ID, raw) {
			st = StatusDegraded
			check.Status = StatusDegraded
			check.ErrorType = "perf"
			check.ErrorMessage = "performance degradation vs. history"
		}
	}

	if err := r.cfg.Store.InsertCheck(check); err != nil {
		return check, nil, fmt.Errorf("insert check: %w", err)
	}
	if err := r.cfg.Store.SetModelState(m.ID, st, check.ErrorType, check.ErrorMessage, check.At); err != nil {
		return check, nil, fmt.Errorf("set model state: %w", err)
	}

	prevStatus := prev.LastStatus
	closed, opened := r.applyIncident(m.ID, prevStatus, st, check.At)

	if prevStatus.Notifiable() && st.Notifiable() && prevStatus != st {
		return check, &Transition{
			ModelID:  m.ID,
			From:     prevStatus,
			To:       st,
			Check:    check,
			Incident: closed,
		}, nil
	}
	_ = opened
	return check, nil, nil
}

// applyIncident maintains incident records. Rules:
//   - Unknown states change nothing: an open incident stays open (uncertain).
//   - First-sight non-Operational state opens an incident without an event.
//   - A notifiable change closes the previous incident and opens a new one.
//   - Unknown → notifiable closes the open incident silently.
func (r *Runner) applyIncident(modelID string, prev, cur status.Status, at time.Time) (closed *storage.Incident, opened bool) {
	if cur == StatusUnknown {
		return nil, false
	}
	if prev == "" {
		// Первое наблюдение: события нет, но неоперациональное состояние
		// начинает отсчёт длительности инцидента.
		if cur != StatusOperational {
			if err := r.cfg.Store.OpenIncident(modelID, cur, at); err != nil {
				r.log.Error("monitor: open incident failed", "model", modelID, "err", err)
			}
			return nil, true
		}
		return nil, false
	}
	if prev == cur {
		return nil, false
	}
	if !cur.Notifiable() {
		return nil, false // prev != cur and cur is Unknown — handled above
	}
	if d, err := r.cfg.Store.CloseIncident(modelID, cur, at); err != nil {
		r.log.Error("monitor: close incident failed", "model", modelID, "err", err)
	} else {
		closed = d
	}
	if cur != StatusOperational {
		if err := r.cfg.Store.OpenIncident(modelID, cur, at); err != nil {
			r.log.Error("monitor: open incident failed", "model", modelID, "err", err)
		}
		opened = true
	}
	return closed, opened
}

// measuredLatency returns the measured latency of the real request.
func (r *Runner) measuredLatency(raw *api.RawResult) *int64 {
	if raw.ErrorKind == "" || raw.ErrorKind == "empty" || raw.ErrorKind == "http" || raw.ErrorKind == "timeout" || raw.ErrorKind == "connrefused" {
		v := raw.LatencyUS
		return &v
	}
	v := raw.LatencyUS
	return &v
}

var errCancelled = errors.New("check cancelled")

// perfDegraded compares the current successful result against the model's own
// recent successful history.
func (r *Runner) perfDegraded(modelID string, raw *api.RawResult) bool {
	since := time.Now().Add(-24 * time.Hour)
	recent, err := r.cfg.Store.RecentChecks(modelID, since)
	if err != nil {
		return false
	}
	samples := make([]PerfSample, 0, len(recent))
	for _, c := range recent {
		if c.Status != StatusOperational || c.LatencyUS == nil || *c.LatencyUS <= 0 {
			continue
		}
		s := PerfSample{LatencyUS: *c.LatencyUS}
		if c.TPS != nil {
			s.TPS = *c.TPS
			s.HasTPS = true
		}
		samples = append(samples, s)
	}
	cur := PerfSample{LatencyUS: raw.LatencyUS}
	if raw.TPS != nil {
		cur.TPS = *raw.TPS
		cur.HasTPS = true
	}
	return DegradedByPerf(r.cfg.Perf, samples, cur)
}
