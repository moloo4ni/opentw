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
	API           ModelChecker
	Store         *storage.Store
	Models        []storage.Model // configured models from config.yaml
	Concurrency   int
	MaxTokens     int
	Interval      time.Duration
	Perf          PerfCfg
	Retention     time.Duration // checks older than this are deleted; default 7d
	HeartbeatPath string        // НЕ пусто — mtime файла = свежесть мониторинга (HEALTHCHECK)
	// OnCycle готовит события уведомлений цикла (чтения, без записи в БД):
	// раннер фиксирует их атомарно с состояниями моделей через Store.ApplyCycle.
	OnCycle func(CycleResult) ([]storage.OutboxEvent, error)
	Logger  *slog.Logger
}

// Runner runs monitoring cycles: first immediately on start, then every Interval.
type Runner struct {
	cfg     RunnerCfg
	running chan struct{} // closed when idle; used as a single-flight token
	lastOK  bool          // последний цикл дожил до конца (управляет стартовым heartbeat)
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

// Run starts the first cycle immediately and then one at every interval
// boundary of the clock (:00/:15/:30/:45 for 15m) until ctx is cancelled:
// расписание не зависит от момента запуска. Errors never stop the loop: a
// failed cycle is logged and retried on the next tick.
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
			// затянувшийся за границу цикл: следующий тик — ближайшая
			// незапущенная граница от текущего момента
			timer.Reset(time.Until(NextTick(r.cfg.Interval, time.Now())))
		}
	}
}

// NextTick — ближайшая после from граница сетки интервала (15м → :00/:15/:30/:45).
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

	// Стартовый heartbeat — только если прошлый цикл был успешен: цикл с
	// мёртвой базой проваливается мгновенно и не должен продлевать жизнь
	// HEALTHCHECK (I1: красный статус должен наступать при отказах БД). Итог
	// успешного цикла фиксируется ниже — вместе с heartbeat в его конце.
	prevOK := r.lastOK
	r.lastOK = false // любой исход, кроме успешного конца, оставит false
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
	// Сигнатура сбоя каталога — контрольный зонд «монитор сам не может проверить»:
	// совпадающая ошибка у моделей превращает 🔴/🟡 в ⚪, а не в ложный вал событий.
	catIssue := catalogIssue(err)

	prevStates := map[string]storage.Model{}
	prevModels, err := r.cfg.Store.Models()
	if err != nil {
		// без предыдущих состояний переходы были бы о первом наблюдении —
		// это переоткрыло бы инциденты и сломало события; прерываем цикл
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
	// События готовит колбэк (чтения подписчиков, сериализация), записи —
	// запрещены: единственная связь уже в руках раннера, и ApplyCycle
	// фиксирует их вместе с состояниями единым коммитом (P0-1).
	var events []storage.OutboxEvent
	if r.cfg.OnCycle != nil {
		var err error
		if events, err = r.cfg.OnCycle(*res); err != nil {
			return nil, fmt.Errorf("prepare events: %w", err)
		}
	}
	// Атомарный итог: либо новые состояния, события и инциденты вместе, либо
	// ничего — рестарт/сбой посреди цикла не теряет события: следующий цикл
	// увидит прежние состояния и переоткроет те же переходы (и те же
	// инцидентные операции — они идемпотентны, D3).
	if err := r.cfg.Store.ApplyCycle(res.Checks, events, incidents...); err != nil {
		return nil, fmt.Errorf("commit cycle: %w", err)
	}
	// Очистка старых измерений и инцидентов — строго ПОСЛЕ коммита цикла (2.4):
	// до коммита она могла удалить инцидент, прочитанный планровщиком →
	// ApplyCycle падал бы («уже закрыт») и ломал фиксацию состояния цикла.
	// Атомарность ApplyCycle не затрагивается: очистка — отдельные транзакции.
	if _, err := r.cfg.Store.DeleteOldChecks(r.cfg.Retention); err != nil {
		r.log.Error("monitor: retention cleanup failed", "err", err)
	}
	// инциденты флэппинг-моделей растут по ~48 строк/сутки: режем по 30 дням
	if _, err := r.cfg.Store.DeleteOldIncidents(30 * 24 * time.Hour); err != nil {
		r.log.Error("monitor: incident cleanup failed", "err", err)
	}
	r.logCycle(res.Checks, events)
	r.lastOK = true
	r.writeHeartbeat()
	return res, nil
}

// logCycle — единственная строка успешного цикла для оператора (I5): без неё
// здоровая система «молчит», а деградация видна только постфактум.
func (r *Runner) logCycle(checks map[string]storage.Check, events []storage.OutboxEvent) {
	var nOK, nDegr, nDown, nUnknown int
	for _, c := range checks {
		switch c.Status {
		case StatusOperational:
			nOK++
		case StatusDegraded:
			nDegr++
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
		"checked", len(checks), "ok", nOK, "degraded", nDegr, "down", nDown, "unknown", nUnknown,
		"events", len(events), "outbox_pending", pending)
}

// writeHeartbeat обновляет mtime файла живости (основа HEALTHCHECK).
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
		// исчезла из каталога — проверяем всё равно: вердикт должен
		// быть реальным ответом API, а не синтетической заглушкой
		r.log.Warn("monitor: model missing from catalog, probing anyway", "model", m.ID)
	}
	raw := r.cfg.API.Generate(ctx, m.ID, r.cfg.MaxTokens)
	if raw.ErrorKind == "cancelled" {
		return storage.Check{}, nil, nil, errCancelled
	}

	st := Classify(*raw)
	if catIssue != "" && matchesCatalogIssue(catIssue, raw) {
		// Ошибка модели совпала с ошибкой каталога тем же транспортом/ключом:
		// это проблема монитора, а не падение конкретной модели. Реальная ошибка
		// сохраняется в check — детали видны в /status model-id.
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
	}

	if st == StatusOperational {
		if degraded, reason := r.perfDegraded(m.ID, raw); degraded {
			st = StatusDegraded
			check.Status = StatusDegraded
			check.ErrorType = "perf"
			check.ErrorMessage = reason
		}
	}

	if err := r.cfg.Store.InsertCheck(check); err != nil {
		return check, nil, nil, fmt.Errorf("insert check: %w", err)
	}
	// Состояние и инциденты НЕ пишутся здесь: их фиксирует ApplyCycle в конце
	// цикла одним коммитом с событиями (P0-1, D3) — иначе ранний рестарт/сбой
	// БД терял бы события и оставлял мусорные инциденты.
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
	case prevStatus == StatusUnknown && (st == StatusOperational || st == StatusDegraded) && closed != nil:
		// Восстановление из ⚪-периода (проблема монитора): инцидент закрылся —
		// событие должно дойти, иначе подписчик никогда не узнает о восстановлении.
		tr = &Transition{
			ModelID:  m.ID,
			From:     prevStatus,
			To:       st,
			Check:    check,
			Incident: closed,
		}
	case blindFall:
		// Модель упала, пока монитор был слеп (⚪), и падение подтверждено
		// реальным ответом API: событие обязательно — иначе подписчик узнает о
		// сбое только по панели. Длительность инцидента неизвестна и не
		// выдумывается: renderer честно помечает это через From=Unknown (D2).
		tr = &Transition{
			ModelID: m.ID,
			From:    prevStatus,
			To:      st,
			Check:   check,
		}
	}
	return check, tr, op, nil
}

// planIncident решает изменения инцидентов модели на цикл (D3): записи
// выполняет только Store.ApplyCycle в общей транзакции цикла. Возвращает
// операцию (nil — менять нечего), закрываемый инцидент (его длительность —
// в событии) и признак падения, обнаруженного после слепого окна монитора (D2).
// Rules:
//   - Unknown check result changes nothing: an open incident stays open
//     (the monitor cannot judge).
//   - First-sight non-Operational state opens an incident without an event.
//   - A notifiable change closes the previous incident and opens a new one.
//   - Recovery from ⚪ closes the still-open incident (event carries duration).
//   - ⚪→🔴/⚪→🟡 with a matching open incident: the model was in trouble
//     BEFORE the blindness — the incident lives on from the original fall,
//     no duplicate, no event (2.2).
//   - ⚪→🔴/⚪→🟡 with no open incident: the model fell DURING the blindness
//     — an incident opens from this check (true start unknown) and the event
//     reaches subscribers (2.1, D2).
func (r *Runner) planIncident(modelID string, prev, cur status.Status, at time.Time) (op *storage.IncidentOp, closed *storage.Incident, blindFall bool) {
	if cur == StatusUnknown {
		return nil, nil, false
	}
	if prev == "" {
		// Первое наблюдение: события нет, но неоперациональное состояние
		// начинает отсчёт длительности инцидента. Дубликат не создаётся: op.Open
		// идемпотентен (модель могла вернуться в конфиг с живым инцидентом).
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
			// слепое окно не меняет картину: открытый инцидент того же состояния
			// (🟡→⚪→🟡, 🔴→⚪→🔴) живёт с исходного события — без повторного
			// события, без второго инцидента и без сброса длительности (2.2).
			// Ухудшение после ⚪ (🟡→⚪→🔴) тоже молчит: инцидент уже открыт,
			// непрерывность длительности важнее лишнего события.
			if open.From == cur || cur == StatusDown {
				return nil, nil, false
			}
			// open.From «хуже» cur (🔴→⚪→🟡): улучшение обязано дойти —
			// обрабатывается общим путём ниже (закрытие + новый инцидент + событие)
		} else {
			// состояние возникло В слепое окно (2.1, D2) — инцидент с момента
			// обнаружения (истинное начало неизвестно), событие обязательно.
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

// catalogIssue классифицирует сбой каталога по строке его ошибки
// (ListModels форматирует HTTP-ошибки как «HTTP <код>», прочее — транспорт).
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
		return "" // иной HTTP-сбой каталога: сам по себе ничего не доказывает
	case strings.Contains(msg, "invalid response"), strings.Contains(msg, "build request"):
		// мусорный ответ каталога — тоже не доказательство проблемы моделей:
		// гасить им реальные таймауты значило бы маскировать аварию
		return ""
	default:
		return "transport"
	}
}

// matchesCatalogIssue — сигнатура ошибки модели совпала с ошибкой каталога.
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

// perfDegraded compares the current successful result against the model's own
// recent successful history; the reason explains the decision with numbers.
func (r *Runner) perfDegraded(modelID string, raw *api.RawResult) (bool, string) {
	since := time.Now().Add(-24 * time.Hour)
	recent, err := r.cfg.Store.RecentChecks(modelID, since)
	if err != nil {
		return false, ""
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
