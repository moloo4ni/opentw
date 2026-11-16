package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"opentw/internal/status"
)

// Model is a monitored model with its last known state.
type Model struct {
	ID           string
	Family       string
	LastStatus   status.Status // "" until the first completed check
	ErrorType    string
	ErrorMessage string
	LastCheckAt  *time.Time
}

// Check is one persisted check result.
type Check struct {
	ModelID      string
	At           time.Time
	HTTPStatus   int
	Status       status.Status
	LatencyUS    *int64
	TTFTUS       *int64
	GenUS        *int64
	TPS          *float64
	InputTokens  *int64
	OutputTokens *int64
	ErrorType    string
	ErrorMessage string
}

// Subscription is one chat's subscription to a model's events.
type Subscription struct {
	ChatID    int64
	ModelID   string
	CreatedAt time.Time
}

// StatusMessage binds a chat to its pinned status panel message.
type StatusMessage struct {
	ChatID    int64
	MessageID int64
	CreatedAt time.Time
}

// Incident is a state transition with its timeline.
type Incident struct {
	ID       int64
	ModelID  string
	From     status.Status
	To       status.Status // empty while open
	Started  time.Time
	Ended    *time.Time
	Duration time.Duration
}

// IncidentOp — запланированное изменение инцидентов одной модели: ApplyCycle
// выполняет его в той же транзакции, что состояния и события (D3): сбой
// коммита не оставляет инциденты «наполовину обновлёнными».
type IncidentOp struct {
	ModelID string
	// Close закрывает именно этот открытый инцидент (ID прочитан до
	// коммита; если он уже закрыт — транзакция падает, цикл повторяется).
	CloseID int64
	CloseTo status.Status
	CloseAt time.Time
	// Open открывает новый инцидент, только если открытого нет (идемпотентно:
	// повтор цикла после сбоя не создаёт дубликат).
	Open     bool
	OpenFrom status.Status
	OpenAt   time.Time
}

// SyncModels upserts the configured models and removes models that are no
// longer configured. Existing state rows keep their last status.
func (s *Store) SyncModels(models []Model) error {
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("storage: sync models: %w", err)
	}
	defer tx.Rollback()
	for _, m := range models {
		if _, err := tx.ExecContext(ctx, `INSERT INTO models (model_id, family) VALUES (?, ?)
			ON CONFLICT(model_id) DO UPDATE SET family = excluded.family`, m.ID, m.Family); err != nil {
			return fmt.Errorf("storage: sync models: %w", err)
		}
	}
	if len(models) > 0 {
		placeholders := ""
		args := make([]any, 0, len(models))
		for i, m := range models {
			if i > 0 {
				placeholders += ","
			}
			placeholders += "?"
			args = append(args, m.ID)
		}
		// Модель покинула конфигурацию: строка состояния уходит, а вместе с ней —
		// подписки на неё (2.5, иначе события возвратившейся модели молча
		// «оживали» бы старые подписки) и открытые инциденты (2.3, иначе они
		// оставались открытыми навсегда и воровали длительность возврата).
		// Инцидент закрывается без перехода (to = from): модель просто перестала
		// наблюдаться, история сохраняется.
		for _, q := range []struct {
			sql  string
			args []any
		}{
			{`DELETE FROM models WHERE model_id NOT IN (` + placeholders + `)`, args},
			{`DELETE FROM subscriptions WHERE model_id NOT IN (` + placeholders + `)`, args},
			{`UPDATE incidents SET to_status = from_status, ended_at = ? WHERE model_id NOT IN (` + placeholders + `) AND ended_at IS NULL`,
				append([]any{fmtTime(time.Now())}, args...)},
		} {
			if _, err := tx.ExecContext(ctx, q.sql, q.args...); err != nil {
				return fmt.Errorf("storage: sync models: %w", err)
			}
		}
	}
	return tx.Commit()
}

// Models returns all configured models ordered by family, then ID.
func (s *Store) Models() ([]Model, error) {
	rows, err := s.db.Query(`SELECT model_id, family, COALESCE(last_status,''), COALESCE(error_type,''), COALESCE(error_message,''), COALESCE(last_check_at,'') FROM models ORDER BY family, model_id`)
	if err != nil {
		return nil, fmt.Errorf("storage: models: %w", err)
	}
	defer rows.Close()
	var out []Model
	for rows.Next() {
		m := Model{}
		var lastCheckAt string
		if err := rows.Scan(&m.ID, &m.Family, &m.LastStatus, &m.ErrorType, &m.ErrorMessage, &lastCheckAt); err != nil {
			return nil, fmt.Errorf("storage: models: %w", err)
		}
		if t, ok := parseTime(lastCheckAt); ok {
			m.LastCheckAt = &t
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// Model returns one model or nil when unknown.
func (s *Store) Model(id string) (*Model, error) {
	models, err := s.Models()
	if err != nil {
		return nil, err
	}
	for i := range models {
		if models[i].ID == id {
			return &models[i], nil
		}
	}
	return nil, nil
}

// SetModelState updates the cached state of one model. Только для тестов
// и ручной коррекции состояния; рантайм фиксирует состояния цикла
// исключительно через ApplyCycle — атомарно с событиями уведомлений.
func (s *Store) SetModelState(modelID string, status status.Status, errType, errMsg string, at time.Time) error {
	q := `UPDATE models SET last_status = ?, error_type = ?, error_message = ?, last_check_at = ? WHERE model_id = ?`
	return s.exec(context.Background(), q, string(status), errType, errMsg, fmtTime(at), modelID)
}

// InsertCheck persists one check.
func (s *Store) InsertCheck(c Check) error {
	q := `INSERT INTO checks (model_id, ts, http_status, status, latency_us, ttft_us, gen_us, tps, input_tokens, output_tokens, error_type, error_message)
	      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	return s.exec(context.Background(), q, c.ModelID, fmtTime(c.At), c.HTTPStatus, string(c.Status), c.LatencyUS, c.TTFTUS, c.GenUS, c.TPS, c.InputTokens, c.OutputTokens, c.ErrorType, c.ErrorMessage)
}

const checkColumns = `model_id, ts, http_status, status, latency_us, ttft_us, gen_us, tps, input_tokens, output_tokens, error_type, error_message`

func scanCheck(scanner interface{ Scan(...any) error }) (Check, error) {
	var c Check
	var ts string
	err := scanner.Scan(&c.ModelID, &ts, &c.HTTPStatus, &c.Status, &c.LatencyUS, &c.TTFTUS, &c.GenUS, &c.TPS, &c.InputTokens, &c.OutputTokens, &c.ErrorType, &c.ErrorMessage)
	if err != nil {
		return c, err
	}
	t, _ := parseTime(ts)
	c.At = t
	return c, nil
}

// LastChecks returns the newest check per model.
func (s *Store) LastChecks() (map[string]Check, error) {
	rows, err := s.db.Query(`SELECT ` + checkColumns + ` FROM checks c
		WHERE ts = (SELECT MAX(ts) FROM checks WHERE model_id = c.model_id)`)
	if err != nil {
		return nil, fmt.Errorf("storage: last checks: %w", err)
	}
	defer rows.Close()
	out := map[string]Check{}
	for rows.Next() {
		c, err := scanCheck(rows)
		if err != nil {
			return nil, fmt.Errorf("storage: last checks: %w", err)
		}
		out[c.ModelID] = c
	}
	return out, rows.Err()
}

// RecentChecks returns checks of one model at or after since, oldest first.
func (s *Store) RecentChecks(modelID string, since time.Time) ([]Check, error) {
	rows, err := s.db.Query(`SELECT `+checkColumns+` FROM checks WHERE model_id = ? AND ts >= ? ORDER BY ts ASC`, modelID, fmtTime(since))
	if err != nil {
		return nil, fmt.Errorf("storage: recent checks: %w", err)
	}
	defer rows.Close()
	var out []Check
	for rows.Next() {
		c, err := scanCheck(rows)
		if err != nil {
			return nil, fmt.Errorf("storage: recent checks: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// DeleteOldChecks removes checks older than the retention window.
func (s *Store) DeleteOldChecks(olderThan time.Duration) (int64, error) {
	res, err := s.db.Exec(`DELETE FROM checks WHERE ts < ?`, fmtTime(time.Now().Add(-olderThan)))
	if err != nil {
		return 0, fmt.Errorf("storage: delete old checks: %w", err)
	}
	return res.RowsAffected()
}

// SetSubscription adds or removes a chat's subscription to a model.
func (s *Store) SetSubscription(chatID int64, modelID string, on bool) error {
	if on {
		q := `INSERT INTO subscriptions (chat_id, model_id, created_at) VALUES (?, ?, ?)
		      ON CONFLICT(chat_id, model_id) DO NOTHING`
		return s.exec(context.Background(), q, chatID, modelID, fmtTime(time.Now()))
	}
	return s.exec(context.Background(), `DELETE FROM subscriptions WHERE chat_id = ? AND model_id = ?`, chatID, modelID)
}

// Subscriptions lists subscribed model IDs of a chat.
func (s *Store) Subscriptions(chatID int64) ([]string, error) {
	rows, err := s.db.Query(`SELECT model_id FROM subscriptions WHERE chat_id = ? ORDER BY model_id`, chatID)
	if err != nil {
		return nil, fmt.Errorf("storage: subscriptions: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// Subscribers lists chats subscribed to a model's events.
func (s *Store) Subscribers(modelID string) ([]int64, error) {
	rows, err := s.db.Query(`SELECT chat_id FROM subscriptions WHERE model_id = ? ORDER BY chat_id`, modelID)
	if err != nil {
		return nil, fmt.Errorf("storage: subscribers: %w", err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// DeleteSubscriptions removes all subscriptions of a chat (dead chat:
// бот заблокирован/исключён и не может доставлять уведомления).
func (s *Store) DeleteSubscriptions(chatID int64) error {
	return s.exec(context.Background(), `DELETE FROM subscriptions WHERE chat_id = ?`, chatID)
}

// SetStatusMessage binds (or rebinds) the pinned panel message of a chat.
func (s *Store) SetStatusMessage(chatID, messageID int64) error {
	q := `INSERT INTO status_messages (chat_id, message_id, created_at) VALUES (?, ?, ?)
	      ON CONFLICT(chat_id) DO UPDATE SET message_id = excluded.message_id`
	return s.exec(context.Background(), q, chatID, messageID, fmtTime(time.Now()))
}

// StatusMessage returns the chat's current panel binding or nil.
func (s *Store) StatusMessage(chatID int64) (*StatusMessage, error) {
	var sm StatusMessage
	var created string
	err := s.db.QueryRow(`SELECT chat_id, message_id, created_at FROM status_messages WHERE chat_id = ?`, chatID).Scan(&sm.ChatID, &sm.MessageID, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("storage: status message: %w", err)
	}
	if t, ok := parseTime(created); ok {
		sm.CreatedAt = t
	}
	return &sm, nil
}

// StatusChats lists chats that have a pinned panel binding.
func (s *Store) StatusChats() ([]int64, error) {
	rows, err := s.db.Query(`SELECT chat_id FROM status_messages ORDER BY chat_id`)
	if err != nil {
		return nil, fmt.Errorf("storage: status chats: %w", err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// DeleteStatusMessage forgets the chat's panel binding.
func (s *Store) DeleteStatusMessage(chatID int64) error {
	return s.exec(context.Background(), `DELETE FROM status_messages WHERE chat_id = ?`, chatID)
}

// OpenIncident starts an incident when a model leaves its previous state.
func (s *Store) OpenIncident(modelID string, from status.Status, at time.Time) error {
	return s.exec(context.Background(), `INSERT INTO incidents (model_id, from_status, started_at) VALUES (?, ?, ?)`, modelID, string(from), fmtTime(at))
}

// CurrentIncident returns the model's newest open incident or nil —
// планирование инцидентов цикла читает его до транзакции ApplyCycle (D2/D3).
func (s *Store) CurrentIncident(modelID string) (*Incident, error) {
	var inc Incident
	var from, started string
	err := s.db.QueryRow(`SELECT id, from_status, started_at FROM incidents WHERE model_id = ? AND ended_at IS NULL ORDER BY id DESC LIMIT 1`, modelID).
		Scan(&inc.ID, &from, &started)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("storage: current incident: %w", err)
	}
	inc.ModelID = modelID
	inc.From = status.Status(from)
	if t, ok := parseTime(started); ok {
		inc.Started = t
	}
	return &inc, nil
}

// CloseIncident closes the newest open incident of the model and returns it.
func (s *Store) CloseIncident(modelID string, to status.Status, at time.Time) (*Incident, error) {
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("storage: close incident: %w", err)
	}
	defer tx.Rollback()
	var id int64
	var from string
	var started string
	err = tx.QueryRowContext(ctx, `SELECT id, from_status, started_at FROM incidents WHERE model_id = ? AND ended_at IS NULL ORDER BY id DESC LIMIT 1`, modelID).Scan(&id, &from, &started)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("storage: close incident: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE incidents SET to_status = ?, ended_at = ? WHERE id = ?`, string(to), fmtTime(at), id); err != nil {
		return nil, fmt.Errorf("storage: close incident: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("storage: close incident: %w", err)
	}
	inc := &Incident{ID: id, ModelID: modelID, From: status.Status(from), To: to}
	if st, ok := parseTime(started); ok {
		inc.Started = st
		inc.Duration = at.Sub(st)
	}
	return inc, nil
}

// HasOpenIncident reports whether the model currently has an open incident.
func (s *Store) HasOpenIncident(modelID string) (bool, error) {
	var one int
	err := s.db.QueryRow(`SELECT 1 FROM incidents WHERE model_id = ? AND ended_at IS NULL LIMIT 1`, modelID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("storage: has incident: %w", err)
	}
	return true, nil
}

// OutboxRow is one pending notification.
type OutboxRow struct {
	ID            int64
	ChatID        int64
	Kind          string
	DedupeKey     string
	Payload       []byte
	CreatedAt     time.Time
	NextAttemptAt *time.Time // не раньше этого момента (retry_after от Telegram)
}

// OutboxEvent is a prepared notification for one chat: the runner commits it
// atomically together with the cycle's model states.
type OutboxEvent struct {
	ChatID    int64
	Kind      string
	DedupeKey string
	Payload   []byte
}

// ApplyCycle atomically commits one monitoring cycle: last states of all
// checked models, the cycle's notification events and the incident changes
// (D3: инциденты обновляются только вместе с фиксацией цикла — сбой/краш
// посреди цикла не оставляет ни мусорных закрытий, ни дубликатов).
// Событие не может существовать без нового состояния и наоборот
// (P0-1 outbox-аудита: рестарт или сбой SQLite посреди цикла не теряет
// события — следующий цикл переоткрывает переходы от прежнего состояния).
// Сбой — не зафиксировано ничего.
func (s *Store) ApplyCycle(checks map[string]Check, events []OutboxEvent, incidents ...IncidentOp) error {
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("storage: apply cycle: %w", err)
	}
	defer tx.Rollback()
	for _, op := range incidents {
		if op.CloseID > 0 {
			res, err := tx.ExecContext(ctx, `UPDATE incidents SET to_status = ?, ended_at = ? WHERE id = ? AND ended_at IS NULL`,
				string(op.CloseTo), fmtTime(op.CloseAt), op.CloseID)
			if err != nil {
				return fmt.Errorf("storage: apply cycle: %w", err)
			}
			if n, _ := res.RowsAffected(); n != 1 {
				// единственный пишущий в incidents — раннер (single-flight);
				// расхождение значит внешнего писателя — цикл проваливается и повторяется
				return fmt.Errorf("storage: apply cycle: incident %d of %s already closed", op.CloseID, op.ModelID)
			}
		}
		if op.Open {
			// только при отсутствии открытого: повтор цикла после сбоя не дублирует
			if _, err := tx.ExecContext(ctx, `INSERT INTO incidents (model_id, from_status, started_at)
				SELECT ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM incidents WHERE model_id = ? AND ended_at IS NULL)`,
				op.ModelID, string(op.OpenFrom), fmtTime(op.OpenAt), op.ModelID); err != nil {
				return fmt.Errorf("storage: apply cycle: %w", err)
			}
		}
	}
	for _, c := range checks {
		if _, err := tx.ExecContext(ctx, `UPDATE models SET last_status = ?, error_type = ?, error_message = ?, last_check_at = ? WHERE model_id = ?`,
			string(c.Status), c.ErrorType, c.ErrorMessage, fmtTime(c.At), c.ModelID); err != nil {
			return fmt.Errorf("storage: apply cycle: %w", err)
		}
	}
	now := fmtTime(time.Now())
	for _, e := range events {
		if _, err := tx.ExecContext(ctx, outboxInsertSQL, e.ChatID, e.Kind, e.Payload, e.DedupeKey, now); err != nil {
			return fmt.Errorf("storage: apply cycle: %w", err)
		}
	}
	return tx.Commit()
}

// outboxInsertSQL — единственный INSERT очереди: и прямой enqueue, и коммит
// цикла дают одинаковую форму строк, дедуп — на (chat_id, dedupe_key).
const outboxInsertSQL = `INSERT INTO notification_outbox (chat_id, kind, payload, dedupe_key, created_at)
	VALUES (?, ?, ?, ?, ?)
	ON CONFLICT(chat_id, dedupe_key) DO NOTHING`

// EnqueueOutbox stores one notification for durable delivery. Duplicate
// (chat_id, dedupe_key) is a no-op — повторная постановка цикла не дублирует.
func (s *Store) EnqueueOutbox(chatID int64, kind, dedupeKey string, payload []byte) error {
	return s.exec(context.Background(), outboxInsertSQL, chatID, kind, payload, dedupeKey, fmtTime(time.Now()))
}

// PendingOutbox returns unsent notifications in FIFO order.
func (s *Store) PendingOutbox(limit int) ([]OutboxRow, error) {
	rows, err := s.db.Query(`SELECT id, chat_id, kind, dedupe_key, payload, created_at, next_attempt_at FROM notification_outbox
		WHERE sent_at IS NULL ORDER BY id LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("storage: pending outbox: %w", err)
	}
	defer rows.Close()
	var out []OutboxRow
	for rows.Next() {
		r, err := scanOutbox(rows)
		if err != nil {
			return nil, fmt.Errorf("storage: pending outbox: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func scanOutbox(scanner interface{ Scan(...any) error }) (OutboxRow, error) {
	var r OutboxRow
	var created, next sql.NullString
	if err := scanner.Scan(&r.ID, &r.ChatID, &r.Kind, &r.DedupeKey, &r.Payload, &created, &next); err != nil {
		return r, err
	}
	if created.Valid {
		if t, ok := parseTime(created.String); ok {
			r.CreatedAt = t
		}
	}
	if next.Valid {
		if t, ok := parseTime(next.String); ok {
			r.NextAttemptAt = &t
		}
	}
	return r, nil
}

// StaleOutboxChats returns chats whose oldest undelivered row is older than
// the threshold: их backlog сворачивается в один дайджест вместо лавины
// устаревших сообщений (P1-2 outbox-аудита).
func (s *Store) StaleOutboxChats(olderThan time.Duration) ([]int64, error) {
	rows, err := s.db.Query(`SELECT chat_id FROM notification_outbox
		WHERE sent_at IS NULL AND created_at < ?
		GROUP BY chat_id ORDER BY MIN(id)`, fmtTime(time.Now().Add(-olderThan)))
	if err != nil {
		return nil, fmt.Errorf("storage: stale outbox chats: %w", err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// PendingOutboxChat returns all undelivered rows of one chat in FIFO order.
func (s *Store) PendingOutboxChat(chatID int64) ([]OutboxRow, error) {
	rows, err := s.db.Query(`SELECT id, chat_id, kind, dedupe_key, payload, created_at, next_attempt_at FROM notification_outbox
		WHERE sent_at IS NULL AND chat_id = ? ORDER BY id`, chatID)
	if err != nil {
		return nil, fmt.Errorf("storage: pending outbox chat: %w", err)
	}
	defer rows.Close()
	var out []OutboxRow
	for rows.Next() {
		r, err := scanOutbox(rows)
		if err != nil {
			return nil, fmt.Errorf("storage: pending outbox chat: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// PendingOutboxCount returns the number of undelivered rows (лог цикла).
func (s *Store) PendingOutboxCount() (int, error) {
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM notification_outbox WHERE sent_at IS NULL`).Scan(&n); err != nil {
		return 0, fmt.Errorf("storage: pending outbox count: %w", err)
	}
	return n, nil
}

// MarkOutboxSent records successful delivery.
func (s *Store) MarkOutboxSent(id int64) error {
	return s.exec(context.Background(), `UPDATE notification_outbox SET sent_at = ? WHERE id = ?`, fmtTime(time.Now()), id)
}

// BumpOutboxAttempt counts a failed delivery attempt (диагностика).
func (s *Store) BumpOutboxAttempt(id int64) error {
	return s.exec(context.Background(), `UPDATE notification_outbox SET attempts = attempts + 1 WHERE id = ?`, id)
}

// DeferOutboxAttempt counts a failed attempt and respects Telegram's
// retry_after: the row is not retried before the deadline.
func (s *Store) DeferOutboxAttempt(id int64, retryAfter time.Duration) error {
	return s.exec(context.Background(),
		`UPDATE notification_outbox SET attempts = attempts + 1, next_attempt_at = ? WHERE id = ?`,
		fmtTime(time.Now().Add(retryAfter)), id)
}

// DeleteOldOutbox removes delivered rows older than the retention window.
func (s *Store) DeleteOldOutbox(olderThan time.Duration) (int64, error) {
	res, err := s.db.Exec(`DELETE FROM notification_outbox WHERE sent_at IS NOT NULL AND sent_at < ?`, fmtTime(time.Now().Add(-olderThan)))
	if err != nil {
		return 0, fmt.Errorf("storage: delete old outbox: %w", err)
	}
	return res.RowsAffected()
}

// DropStaleOutbox removes undelivered rows older than the TTL: событие
// «модель упала» недельной давности после восстановления только путает.
// Возвращает число удалённых строк — событие не должно исчезать молча.
func (s *Store) DropStaleOutbox(olderThan time.Duration) (int64, error) {
	res, err := s.db.Exec(`DELETE FROM notification_outbox WHERE sent_at IS NULL AND created_at < ?`, fmtTime(time.Now().Add(-olderThan)))
	if err != nil {
		return 0, fmt.Errorf("storage: drop stale outbox: %w", err)
	}
	return res.RowsAffected()
}

// MarkOutboxSentByIDs marks exactly the given rows as sent — только те,
// что вошли в отправленный дайджест; строки, добавленные чату во время
// отправки, остаются pending и доезжают отдельно (D1).
func (s *Store) MarkOutboxSentByIDs(ids []int64) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	q := `UPDATE notification_outbox SET sent_at = ? WHERE sent_at IS NULL AND id IN (`
	args := make([]any, 0, len(ids)+1)
	args = append(args, fmtTime(time.Now()))
	for i, id := range ids {
		if i > 0 {
			q += ","
		}
		q += "?"
		args = append(args, id)
	}
	q += ")"
	res, err := s.db.Exec(q, args...)
	if err != nil {
		return 0, fmt.Errorf("storage: mark outbox sent: %w", err)
	}
	return res.RowsAffected()
}

// DeleteOutboxChat drops all undelivered rows of a dead chat.
func (s *Store) DeleteOutboxChat(chatID int64) (int64, error) {
	res, err := s.db.Exec(`DELETE FROM notification_outbox WHERE chat_id = ? AND sent_at IS NULL`, chatID)
	if err != nil {
		return 0, fmt.Errorf("storage: delete outbox chat: %w", err)
	}
	return res.RowsAffected()
}

// ReplaceSubscriptions atomically sets the chat's exact subscription list.
func (s *Store) ReplaceSubscriptions(chatID int64, modelIDs []string) error {
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("storage: replace subscriptions: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM subscriptions WHERE chat_id = ?`, chatID); err != nil {
		return fmt.Errorf("storage: replace subscriptions: %w", err)
	}
	for _, id := range modelIDs {
		created := fmtTime(time.Now())
		if _, err := tx.ExecContext(ctx, `INSERT INTO subscriptions (chat_id, model_id, created_at) VALUES (?, ?, ?)
			ON CONFLICT(chat_id, model_id) DO NOTHING`, chatID, id, created); err != nil {
			return fmt.Errorf("storage: replace subscriptions: %w", err)
		}
	}
	return tx.Commit()
}

// DeleteOldIncidents removes incidents older than the retention window,
// including still-open ones (otherwise endless growth on flapping models).
func (s *Store) DeleteOldIncidents(olderThan time.Duration) (int64, error) {
	res, err := s.db.Exec(`DELETE FROM incidents WHERE started_at < ?`, fmtTime(time.Now().Add(-olderThan)))
	if err != nil {
		return 0, fmt.Errorf("storage: delete old incidents: %w", err)
	}
	return res.RowsAffected()
}

// SetBotOffset persists the getUpdates offset.
func (s *Store) SetBotOffset(offset int64) error {
	return s.exec(context.Background(),
		`INSERT INTO bot_state (id, offset) VALUES (1, ?)
		 ON CONFLICT(id) DO UPDATE SET offset = excluded.offset`, offset)
}

// BotOffset returns the persisted getUpdates offset (0 when absent).
func (s *Store) BotOffset() (int64, error) {
	var off int64
	err := s.db.QueryRow(`SELECT offset FROM bot_state WHERE id = 1`).Scan(&off)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("storage: bot offset: %w", err)
	}
	return off, nil
}
