// Package storage persists OpenTW state in SQLite.
package storage

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"strconv"
	"time"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schemaFS embed.FS

// schemaVersion — маркер актуальности схемы (PRAGMA user_version в заголовке
// файла). Отделяет корректную базу от старой/чужой без системы версионирования:
// миграций нет, схема одна, при изменении schema.sql число меняется.
const schemaVersion = 1

// Store wraps the SQLite database.
type Store struct {
	db   *sql.DB
	path string
}

// Open opens the database at path. Пустая (несуществующая) база создаётся с
// единственной актуальной схемой; корректная существующая используется как
// есть; старая несовместимая отвергается ошибкой — без удаления и без
// попытки тихо «долечить» (решение о чистой базе остаётся за оператором).
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(10000)&_pragma=foreign_keys(1)")
	if err != nil {
		return nil, fmt.Errorf("storage: open: %w", err)
	}
	// Один процесс, один писатель: сериализация соединений убирает SQLITE_BUSY.
	db.SetMaxOpenConns(1)
	s := &Store{db: db, path: path}
	if err := s.initSchema(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

// initSchema применяет schema.sql к пустой базе и ставит маркер user_version
// в той же транзакции. База с маркером текущей версии не трогается вовсе
// (повторный запуск ничего не пересоздаёт); база с таблицами, но без маркера
// — историческая, честно отвергается.
func (s *Store) initSchema() error {
	var v int
	if err := s.db.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil {
		return fmt.Errorf("storage: open database %s: %w", s.path, err)
	}
	if v == schemaVersion {
		return nil
	}
	if v > schemaVersion {
		return fmt.Errorf("storage: database schema version %d is newer than this binary supports (%d): use a matching opentw binary", v, schemaVersion)
	}
	var tables int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`).Scan(&tables); err != nil {
		return fmt.Errorf("storage: schema: %w", err)
	}
	if tables > 0 {
		return fmt.Errorf("storage: database at %q is not compatible with this opentw version (legacy or unknown schema): start with a clean database - remove the file together with its -wal/-shm sidecars after stopping the instance, or point sqlite_path to a new location", s.path)
	}
	body, err := schemaFS.ReadFile("schema.sql")
	if err != nil {
		return fmt.Errorf("storage: schema: %w", err)
	}
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("storage: schema: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, string(body)); err != nil {
		return fmt.Errorf("storage: schema: %w", err)
	}
	// параметризовать PRAGMA нельзя — но schemaVersion это int-константа
	if _, err := tx.ExecContext(ctx, `PRAGMA user_version = `+strconv.Itoa(schemaVersion)); err != nil {
		return fmt.Errorf("storage: schema: %w", err)
	}
	return tx.Commit()
}

// BackupTo writes a consistent snapshot of the (possibly live WAL) database
// to dst via VACUUM INTO. Fails when dst already exists.
func (s *Store) BackupTo(dst string) error {
	if _, err := s.db.Exec(`VACUUM INTO ?`, dst); err != nil {
		return fmt.Errorf("storage: backup: %w", err)
	}
	return nil
}

func (s *Store) exec(ctx context.Context, query string, args ...any) error {
	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}

func fmtTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func parseTime(v string) (time.Time, bool) {
	if v == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}
