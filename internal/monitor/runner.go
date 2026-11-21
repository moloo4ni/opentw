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

	// allUnknownStreak — подряд идущие циклы без единого осмысленного
	// вердикта (A01-5): живой монитор, мёртвый ключ. Операционный сигнал,
	// семантику статусов и /status не меняет.
	allUnknownStreak int
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
	// совпадающая ошибка у моделей превращает 🔴 в ⚪, а не в ложный вал событий.
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
	// фиксирует их вместе с состояниями единым коммитом.
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
	// инцидентные операции — они идемпотентны).
	if err := r.cfg.Store.ApplyCycle(res.Checks, events, incidents...); err != nil {
		return nil, fmt.Errorf("commit cycle: %w", err)
	}
	// Очистка старых измерений и инцидентов — строго ПОСЛЕ коммита цикла:
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

	// A01-5: циклы успешны (heartbeat жив, контейнер healthy), но осмысленных
	// вердиктов нет — проблемы ключа/шлюза выдают себя только этим сигналом.
	// Статусы моделей остаются честными ⚪; ложных падений не создаём.
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
		// Оригинальный текст ответа модели/API — дословно; он же
		// диагностический ответ в карточке модели.
		Response: raw.Response,
	}
	if st == StatusDown && raw.ErrorKind == "" {
		// HTTP 200, но модель не ответила ожидаемым «OK»: краткий внутренний
		// тип ошибки — «response»; дословный текст уже в Response, причина
		// не выдумывается. Названию краткой ошибки место не в длинном тексте.
		check.ErrorType = "response"
		check.ErrorMessage = ""
	}

	if st == StatusDown && raw.HTTPStatus == 200 && raw.FinishReason != "" {
		// finish_reason — диагностический факт терминального чанка, а не ответ
		// модели и не новый ErrorKind. Существующее поле ErrorMessage позволяет
		// сохранить его без миграции; содержательный текст провайдера не затирается.
		check.ErrorMessage = appendFinishReason(check.ErrorMessage, raw.FinishReason)
	}

	if err := r.cfg.Store.InsertCheck(check); err != nil {
		return check, nil, nil, fmt.Errorf("insert check: %w", err)
	}
	// Состояние и инциденты НЕ пишутся здесь: их фиксирует ApplyCycle в конце
	// цикла одним коммитом с событиями — иначе ранний рестарт/сбой
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
	case prevStatus == StatusUnknown && st == StatusOperational && closed != nil:
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
		// выдумывается: renderer честно помечает это через From=Unknown.
		tr = &Transition{
			ModelID: m.ID,
			From:    prevStatus,
			To:      st,
			Check:   check,
		}
	}
	return check, tr, op, nil
}

// appendFinishReason сохраняет причину завершения генерации в существующей
// диагностической строке. Это не классификация и не замена Response.
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

// planIncident решает изменения инцидентов модели на цикл: записи
// выполняет только Store.ApplyCycle в общей транзакции цикла. Возвращает
// операцию (nil — менять нечего), закрываемый инцидент (его длительность —
// в событии) и признак падения, обнаруженного после слепого окна монитора.
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
			// (🔴→⚪→🔴) живёт с исходного события — без повторного
			// события, без второго инцидента и без сброса длительности.
			// Инцидент при этом мог открыться до ⚪; непрерывность
			// длительности важнее лишнего события.
			if open.From == cur || cur == StatusDown {
				return nil, nil, false
			}
			// (с тремя статусами иных «улучшений» из ⚪ не существует: неоперациональное
			// состояние только 🔴, и оно погашено выше; путь ниже — теоретический)
		} else {
			// состояние возникло В слепое окно — инцидент с момента
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
