package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"opentw/internal/monitor"
)

// Model is a monitored model with its last known state.
type Model struct {
	ID           string
	Family       string
	LastStatus   monitor.Status // "" until the first completed check
	ErrorType    string
	ErrorMessage string
	LastCheckAt  *time.Time
}

// Check is one persisted check result.
type Check struct {
	ModelID      string
	At           time.Time
	HTTPStatus   int
	Status       monitor.Status
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
	ModelID  string
	From     monitor.Status
	To       monitor.Status // empty while open
	Started  time.Time
	Ended    *time.Time
	Duration time.Duration
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
	var configured string
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
		configured = fmt.Sprintf("DELETE FROM models WHERE model_id NOT IN (%s)", placeholders)
		if _, err := tx.ExecContext(ctx, configured, args...); err != nil {
			return fmt.Errorf("storage: sync models: %w", err)
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

// SetModelState updates the cached state of a model after a check.
func (s *Store) SetModelState(modelID string, status monitor.Status, errType, errMsg string, at time.Time) error {
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

// DeleteStatusMessage forgets the chat's panel binding.
func (s *Store) DeleteStatusMessage(chatID int64) error {
	return s.exec(context.Background(), `DELETE FROM status_messages WHERE chat_id = ?`, chatID)
}

// OpenIncident starts an incident when a model leaves its previous state.
func (s *Store) OpenIncident(modelID string, from monitor.Status, at time.Time) error {
	return s.exec(context.Background(), `INSERT INTO incidents (model_id, from_status, started_at) VALUES (?, ?, ?)`, modelID, string(from), fmtTime(at))
}

// CloseIncident closes the newest open incident of the model and returns it.
func (s *Store) CloseIncident(modelID string, to monitor.Status, at time.Time) (*Incident, error) {
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
	inc := &Incident{ModelID: modelID, From: monitor.Status(from), To: to}
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
