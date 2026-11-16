// Package storage persists OpenTW state in SQLite.
package storage

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Store wraps the SQLite database.
type Store struct {
	db *sql.DB
}

// Open opens (and migrates) the database at path.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(10000)&_pragma=foreign_keys(1)")
	if err != nil {
		return nil, fmt.Errorf("storage: open: %w", err)
	}
	// Один процесс, один писатель: сериализация соединений убирает SQLITE_BUSY.
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	if err := s.assertSchema(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	if _, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)`); err != nil {
		return fmt.Errorf("storage: migrate: %w", err)
	}
	var current int
	if err := s.db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&current); err != nil {
		return fmt.Errorf("storage: migrate: %w", err)
	}
	names, err := fs.Glob(migrationsFS, "migrations/*.sql")
	if err != nil {
		return fmt.Errorf("storage: migrate: %w", err)
	}
	sort.Strings(names)
	for _, name := range names {
		// Версия — из числового префикса имени, а не позиции в списке:
		// вставка/удаление файла не должна сдвигать версии чужих миграций (I6).
		base := strings.TrimPrefix(name, "migrations/")
		cut := strings.IndexByte(base, '_')
		if cut <= 0 {
			return fmt.Errorf("storage: migrate: bad migration name %q (want NNN_desc.sql)", name)
		}
		version, err := strconv.Atoi(base[:cut])
		if err != nil {
			return fmt.Errorf("storage: migrate: bad migration name %q (want NNN_desc.sql)", name)
		}
		if version <= current {
			continue
		}
		body, err := migrationsFS.ReadFile(name)
		if err != nil {
			return fmt.Errorf("storage: migrate: %w", err)
		}
		tx, err := s.db.Begin()
		if err != nil {
			return fmt.Errorf("storage: migrate: %w", err)
		}
		if _, err := tx.Exec(string(body)); err != nil {
			tx.Rollback()
			return fmt.Errorf("storage: migrate %s: %w", name, err)
		}
		if _, err := tx.Exec(`INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)`, version, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			tx.Rollback()
			return fmt.Errorf("storage: migrate %s: %w", name, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("storage: migrate %s: %w", name, err)
		}
	}
	return nil
}

// assertSchema защищает от баз интерим-сборок: CREATE TABLE IF NOT EXISTS
// пропускает чужую форму таблицы, а рантайм упал бы «no such column» позже.
func (s *Store) assertSchema() error {
	for _, col := range []string{"chat_id", "sent_at", "attempts", "next_attempt_at"} {
		var one int
		err := s.db.QueryRow(`SELECT 1 FROM pragma_table_info('notification_outbox') WHERE name = ?`, col).Scan(&one)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("storage: notification_outbox lacks column %q (legacy interim database? restore from a consistent backup)", col)
		}
		if err != nil {
			return fmt.Errorf("storage: schema check: %w", err)
		}
	}
	return nil
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
