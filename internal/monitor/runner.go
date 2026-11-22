package monitor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/moloo4ni/opentw/internal/api"
	"github.com/moloo4ni/opentw/internal/status"
	"github.com/moloo4ni/opentw/internal/storage"
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
	API           ModelChecker
	Store         *storage.Store
	Models        []storage.Model // configured models from config.yaml
	Concurrency   int
	MaxTokens     int
	Interval      time.Duration
	Retention     time.Duration // checks older than this are deleted; default 7d
	HeartbeatPath string        // non-empty: file mtime is the freshness signal for the HEALTHCHECK
	// OnCycle prepares the cycle's notification events (reads only, no DB
	// writes): the runner commits them atomically with model states via
	// Store.ApplyCycle.
	OnCycle func(CycleResult) ([]storage.OutboxEvent, error)
	Logger  *slog.Logger
}

// Runner runs monitoring cycles: first immediately on start, then every Interval.
type Runner struct {
	cfg     RunnerCfg
	running chan struct{} // closed when idle; used as a single-flight token
	lastOK  bool          // the last cycle ran to completion (gates the startup heartbeat)
	log     *slog.Logger

	// allUnknownStreak counts consecutive cycles with zero definitive
	// verdicts: a live monitor with a dead key. Operational signal only;
	// it never changes status semantics or /status output.
	allUnknownStreak int
}

// Store exposes the storage handle (for the bot and tests).
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

// Run starts the first cycle immediately and then one at every interval
// boundary of the clock (:00/:15/:30/:45 for 15m) until ctx is cancelled:
// the schedule does not depend on the start moment. Errors never stop the
// loop: a failed cycle is logged and retried on the next tick.
func (r *Runner) Run(ctx context.Context) error {
	r.log.Info("monitor: starting", "first_cycle", "immediate", "interval", r.cfg.Interval.String())
	r.runOne(ctx)
	timer := time.NewTimer(time.Until(NextTick(r.cfg.Interval, time.Now())))
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			r.runOne(ctx)
			// a cycle that overran its slot: the next tick is the nearest
			// not-yet-run boundary from the current moment
			timer.Reset(time.Until(NextTick(r.cfg.Interval, time.Now())))
		}
	}
}

// NextTick is the first interval-grid boundary after from (15m → :00/:15/:30/:45).
func NextTick(interval time.Duration, from time.Time) time.Time {
	return from.Truncate(interval).Add(interval)
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

	// Startup heartbeat only when the previous cycle succeeded: a cycle
	// against a dead database fails instantly and must not keep the
	// HEALTHCHECK alive. The successful outcome is recorded below, with the
	// heartbeat at the cycle's end.
	prevOK := r.lastOK
	r.lastOK = false // anything but a successful completion leaves it false
	if prevOK {
		r.writeHeartbeat()
	}

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
	// A catalog failure signature is the "the monitor itself cannot check"
	// probe: a matching model error turns Down into Unknown instead of a
	// false avalanche of events.
	catIssue := catalogIssue(err)

	prevStates := map[string]storage.Model{}
	prevModels, err := r.cfg.Store.Models()
	if err != nil {
		// without previous states every change would look like a first
		// sighting — reopening incidents and breaking events; abort the cycle
		return nil, fmt.Errorf("read previous states: %w", err)
	}
	for _, m := range prevModels {
		prevStates[m.ID] = m
	}

	var (
		mu          sync.Mutex
		wg          sync.WaitGroup
		sem         = make(chan struct{}, r.cfg.Concurrency)
		checks      = map[string]storage.Check{}
		transitions []Transition
		incidents   []storage.IncidentOp
	)

	for _, m := range r.cfg.Models {
		wg.Add(1)
		go func(m storage.Model) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			check, tr, op, err := r.checkModel(ctx, m, catalog, catalogOK, catIssue, prevStates[m.ID], now)
			if err != nil {
				r.log.Error("monitor: check failed", "model", m.ID, "err", err)
				return
			}
			mu.Lock()
			checks[check.ModelID] = check
			if tr != nil {
				transitions = append(transitions, *tr)
			}
			if op != nil {
				incidents = append(incidents, *op)
			}
			mu.Unlock()
		}(m)
	}
	wg.Wait()

	res := &CycleResult{
		CycleAt:     now,
		NextAt:      NextTick(r.cfg.Interval, now),
		Checks:      checks,
		Transitions: transitions,
	}
	// The callback prepares events (subscriber reads, serialization); DB
	// writes are forbidden: the only link is already in the runner's hands,
	// and ApplyCycle commits them together with the states in one commit.
	var events []storage.OutboxEvent
	if r.cfg.OnCycle != nil {
		var err error
		if events, err = r.cfg.OnCycle(*res); err != nil {
			return nil, fmt.Errorf("prepare events: %w", err)
		}
	}
	// Atomic outcome: new states, events and incidents all land together or
	// nothing does — a restart/crash mid-cycle loses no events: the next
	// cycle sees the previous states and reopens the same transitions (and
	// the same idempotent incident operations).
	if err := r.cfg.Store.ApplyCycle(res.Checks, events, incidents...); err != nil {
		return nil, fmt.Errorf("commit cycle: %w", err)
	}
	// Retention runs strictly AFTER the cycle commit: before it, cleanup
	// could delete an incident the planner read → ApplyCycle would fail
	// ("already closed") and break the cycle's state commit. ApplyCycle's
	// atomicity is untouched: cleanup runs in separate transactions.
	if _, err := r.cfg.Store.DeleteOldChecks(r.cfg.Retention); err != nil {
		r.log.Error("monitor: retention cleanup failed", "err", err)
	}
	// flapping models grow ~48 incident rows/day: cut at 30 days
	if _, err := r.cfg.Store.DeleteOldIncidents(30 * 24 * time.Hour); err != nil {
		r.log.Error("monitor: incident cleanup failed", "err", err)
	}
	r.logCycle(res.Checks, events)
	r.lastOK = true
	r.writeHeartbeat()
	return res, nil
}

// logCycle emits the operator's single line per successful cycle: without
// it a healthy system is silent and regressions surface only afterwards.
func (r *Runner) logCycle(checks map[string]storage.Check, events []storage.OutboxEvent) {
	var nOK, nDown, nUnknown int
	for _, c := range checks {
		switch c.Status {
		case StatusOperational:
			nOK++
		case StatusDown:
			nDown++
		default:
			nUnknown++
		}
	}
	pending, err := r.cfg.Store.PendingOutboxCount()
	if err != nil {
		r.log.Warn("monitor: outbox count failed", "err", err)
	}
	r.log.Info("monitor: cycle done",
		"checked", len(checks), "ok", nOK, "down", nDown, "unknown", nUnknown,
		"events", len(events), "outbox_pending", pending)

	// Cycles succeed (heartbeat alive, container healthy) yet no definitive
	// verdicts — a key/gateway problem betrays itself only through this
	// signal. Model statuses stay honest Unknown; no fake falls are created.
	if len(checks) > 0 && nOK+nDown == 0 {
		r.allUnknownStreak++
		if r.allUnknownStreak >= 2 {
			r.log.Error("monitor: no meaningful checks for consecutive cycles - monitor/API problem (key or gateway), not a model outage",
				"streak", r.allUnknownStreak, "unknown", nUnknown)
		}
	} else {
		r.allUnknownStreak = 0
	}
}

// writeHeartbeat touches the freshness file (the HEALTHCHECK basis).
func (r *Runner) writeHeartbeat() {
	if r.cfg.HeartbeatPath == "" {
		return
	}
	if err := os.WriteFile(r.cfg.HeartbeatPath, []byte(time.Now().UTC().Format(time.RFC3339)), 0o644); err != nil {
		r.log.Warn("monitor: heartbeat write failed", "err", err)
	}
}

// checkModel runs one model check and records it; it returns the stored check,
// the transition (when a notifiable state change occurred) and the incident
// operation for the cycle's transaction (nil when nothing changes).
func (r *Runner) checkModel(ctx context.Context, m storage.Model, catalog map[string]bool, catalogOK bool, catIssue string, prev storage.Model, now time.Time) (storage.Check, *Transition, *storage.IncidentOp, error) {
	if catalogOK && !catalog[m.ID] {
		// gone from the catalog — probe anyway: the verdict must be a real
		// API answer, not a synthetic stub
		r.log.Warn("monitor: model missing from catalog, probing anyway", "model", m.ID)
	}
	raw := r.cfg.API.Generate(ctx, m.ID, r.cfg.MaxTokens)
	if raw.ErrorKind == "cancelled" {
		return storage.Check{}, nil, nil, errCancelled
	}

	st := Classify(*raw)
	if catIssue != "" && matchesCatalogIssue(catIssue, raw) {
		// The model's error matches the catalog's error with the same
		// transport/key: a monitor problem, not this model's fall. The real
		// error is kept in the check — details are visible in /status model-id.
		st = StatusUnknown
	}
	check := storage.Check{
		ModelID:      m.ID,
		At:           raw.Timestamp,
		HTTPStatus:   raw.HTTPStatus,
		Status:       st,
		LatencyUS:    &raw.LatencyUS,
		TTFTUS:       raw.TTFTUS,
		GenUS:        raw.GenUS,
		TPS:          raw.TPS,
		InputTokens:  raw.InputTokens,
		OutputTokens: raw.OutputTokens,
		ErrorType:    raw.ErrorKind,
		ErrorMessage: raw.ErrorMessage,
		// Verbatim model/API response text; also the diagnostic answer on the
		// model card.
		Response: raw.Response,
	}
	if st == StatusDown && raw.ErrorKind == "" {
		// HTTP 200 but the model did not answer the expected "OK": short
		// internal error type "response"; the verbatim text is already in
		// Response, the cause is not invented.
		check.ErrorType = "response"
		check.ErrorMessage = ""
	}

	if st == StatusDown && raw.HTTPStatus == 200 && raw.FinishReason != "" {
		// finish_reason is a diagnostic fact of the terminal chunk, not the
		// model's answer and not a new ErrorKind. The existing ErrorMessage
		// field carries it without a migration; provider text is not overwritten.
		check.ErrorMessage = appendFinishReason(check.ErrorMessage, raw.FinishReason)
	}

	if err := r.cfg.Store.InsertCheck(check); err != nil {
		return check, nil, nil, fmt.Errorf("insert check: %w", err)
	}
	// State and incidents are NOT written here: ApplyCycle commits them at
	// the end of the cycle together with events — otherwise an early restart
	// or DB failure would lose events and leave spurious incidents.
	prevStatus := prev.LastStatus
	op, closed, blindFall := r.planIncident(m.ID, prevStatus, st, check.At)

	var tr *Transition
	switch {
	case prevStatus.Notifiable() && st.Notifiable() && prevStatus != st:
		tr = &Transition{
			ModelID:  m.ID,
			From:     prevStatus,
			To:       st,
			Check:    check,
			Incident: closed,
		}
	case prevStatus == StatusUnknown && st == StatusOperational && closed != nil:
		// Recovery from an Unknown period (a monitor problem): the incident
		// closed — the event must reach subscribers, or they never learn about
		// the recovery.
		tr = &Transition{
			ModelID:  m.ID,
			From:     prevStatus,
			To:       st,
			Check:    check,
			Incident: closed,
		}
	case blindFall:
		// The model fell while the monitor was blind and the fall is confirmed
		// by a real API answer: the event is mandatory, or subscribers learn
		// about the failure only from the panel. The incident's duration is
		// unknown and not invented: the renderer marks it via From=Unknown.
		tr = &Transition{
			ModelID: m.ID,
			From:    prevStatus,
			To:      st,
			Check:   check,
		}
	}
	return check, tr, op, nil
}

// appendFinishReason stores the generation completion reason in the
// existing diagnostic string. Not a classification, not a Response replacement.
func appendFinishReason(message, reason string) string {
	diag := "finish_reason=" + reason
	if message == "" {
		return diag
	}
	if strings.Contains(message, diag) {
		return message
	}
	return message + "; " + diag
}

// planIncident decides one model's incident changes for a cycle; the
// writes are performed only by Store.ApplyCycle in the cycle's shared
// transaction. Returns the operation (nil — nothing to change), the closed
// incident (its duration goes into the event) and the flag of a fall
// detected after a blind window.
// Rules:
//   - Unknown check result changes nothing: an open incident stays open
//     (the monitor cannot judge).
//   - First-sight non-Operational state opens an incident without an event.
//   - A notifiable change closes the previous incident and opens a new one.
//   - Recovery from ⚪ closes the still-open incident (event carries duration).
//   - ⚪→🔴 with a matching open incident: the model was in trouble
//     BEFORE the blindness — the incident lives on from the original fall,
//     no duplicate, no event.
//   - ⚪→🔴 with no open incident: the model fell DURING the blindness
//     — an incident opens from this check (true start unknown) and the event
//     reaches subscribers.
func (r *Runner) planIncident(modelID string, prev, cur status.Status, at time.Time) (op *storage.IncidentOp, closed *storage.Incident, blindFall bool) {
	if cur == StatusUnknown {
		return nil, nil, false
	}
	if prev == "" {
		// First sighting: no event, but a non-operational state starts the
		// incident clock. No duplicate: op.Open is idempotent (the model may
		// be back in the config with a live incident).
		if cur == StatusOperational {
			return nil, nil, false
		}
		return &storage.IncidentOp{ModelID: modelID, Open: true, OpenFrom: cur, OpenAt: at}, nil, false
	}
	if prev == cur {
		return nil, nil, false
	}
	if prev == StatusUnknown && cur != StatusOperational {
		open, err := r.cfg.Store.CurrentIncident(modelID)
		if err != nil {
			r.log.Error("monitor: read open incident failed", "model", modelID, "err", err)
			return nil, nil, false
		}
		if open != nil {
			// The blind window changes nothing: an open incident of the same
			// state (Down→Unknown→Down) lives on from the original fall — no
			// repeated event, no second incident, no duration reset.
			// Continuity of the duration beats a redundant event.
			if open.From == cur || cur == StatusDown {
				return nil, nil, false
			}
			// With three statuses no other "improvements" from Unknown exist:
			// the only non-operational state is Down and is handled above; the
			// path below is theoretical.
		} else {
			// The state arose DURING the blind window — the incident starts at
			// discovery (the true start is unknown), the event is mandatory.
			return &storage.IncidentOp{ModelID: modelID, Open: true, OpenFrom: cur, OpenAt: at}, nil, true
		}
	}
	if !cur.Notifiable() {
		return nil, nil, false // prev != cur and cur is Unknown — handled above
	}
	op = &storage.IncidentOp{ModelID: modelID}
	if open, err := r.cfg.Store.CurrentIncident(modelID); err != nil {
		r.log.Error("monitor: read open incident failed", "model", modelID, "err", err)
	} else if open != nil {
		op.CloseID = open.ID
		op.CloseTo = cur
		op.CloseAt = at
		open.To = cur
		open.Duration = at.Sub(open.Started)
		closed = open
	}
	if cur != StatusOperational {
		op.Open = true
		op.OpenFrom = cur
		op.OpenAt = at
	}
	if op.CloseID == 0 && !op.Open {
		return nil, nil, false
	}
	return op, closed, false
}

var errCancelled = errors.New("check cancelled")

// catalogIssue classifies a catalog failure from its error string
// (ListModels formats HTTP errors as "HTTP <code>", the rest is transport).
func catalogIssue(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "HTTP 401"), strings.Contains(msg, "HTTP 403"):
		return "auth"
	case strings.Contains(msg, "HTTP 429"):
		return "ratelimit"
	case strings.Contains(msg, "HTTP "):
		return "" // a different HTTP failure proves nothing by itself
	case strings.Contains(msg, "invalid response"), strings.Contains(msg, "build request"):
		// A garbage catalog response proves no model problem either:
		// masking real timeouts with it would hide an outage.
		return ""
	default:
		return "transport"
	}
}

// matchesCatalogIssue reports whether the model's error matches the
// catalog failure signature.
func matchesCatalogIssue(catIssue string, raw *api.RawResult) bool {
	switch catIssue {
	case "transport":
		return raw.ErrorKind == "timeout" || raw.ErrorKind == "connrefused"
	case "ratelimit":
		return raw.ErrorKind == "http" && raw.HTTPStatus == 429
	case "auth":
		return raw.ErrorKind == "http" && (raw.HTTPStatus == 401 || raw.HTTPStatus == 403)
	}
	return false
}
