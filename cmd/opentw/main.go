// OpenTW — Telegram bot monitoring model availability through the Tonworld API.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/moloo4ni/opentw/internal/api"
	"github.com/moloo4ni/opentw/internal/config"
	"github.com/moloo4ni/opentw/internal/monitor"
	"github.com/moloo4ni/opentw/internal/storage"
	"github.com/moloo4ni/opentw/internal/telegram"
)

// testPrompt is the standardized probe request, identical for every model.
const testPrompt = "Reply with exactly \"OK\""

func main() {
	if err := run(); err != nil {
		slog.Error("opentw: fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	// A separate FlagSet (not the global flag.CommandLine): run() is called
	// repeatedly in tests, the global one re-registers flags and panics.
	fs := flag.NewFlagSet("opentw", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	configPath := fs.String("config", "config.yaml", "path to config.yaml")
	backup := fs.String("backup", "", "write a consistent SQLite snapshot to this path and exit")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	log.Info("opentw: starting",
		"models", len(cfg.ModelIDs()),
		"interval", cfg.Interval.String(),
		"streaming", cfg.Streaming,
		"concurrency", cfg.Concurrency,
	)

	store, err := storage.Open(cfg.SQLitePath)
	if err != nil {
		return err
	}
	defer store.Close()

	if *backup != "" {
		// VACUUM INTO: an atomic snapshot of a live WAL database, without
		// stopping and without sqlite3 in the image; fails if the file exists.
		if err := store.BackupTo(*backup); err != nil {
			return err
		}
		log.Info("opentw: backup written", "path", *backup)
		return nil
	}

	models := make([]storage.Model, 0, len(cfg.ModelIDs()))
	for _, family := range cfg.Models {
		for _, id := range family.IDs {
			models = append(models, storage.Model{ID: id, Family: family.Family})
		}
	}
	if err := store.SyncModels(models); err != nil {
		return err
	}

	apiClient := &api.Client{
		BaseURL:    cfg.APIBaseURL,
		APIKey:     cfg.APIKey,
		HTTPClient: &http.Client{Timeout: cfg.HTTPTimeout},
		Prompt:     testPrompt,
		Stream:     cfg.Streaming,
	}

	bot := telegram.NewBot(telegram.BotCfg{
		API:      telegram.NewClient(cfg.BotToken),
		Store:    store,
		Models:   models,
		Logger:   log,
		Interval: cfg.Interval,
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// The freshness threshold is computed from the actual config and written
	// next to the heartbeat: the HEALTHCHECK reads it, so the image needs no
	// rebuild when the interval changes. 2× interval + worst possible cycle
	// + one minute.
	worstCycle := worstCycleTime(len(models), cfg.Concurrency, cfg.HTTPTimeout)
	maxAgePath := filepath.Join(filepath.Dir(cfg.SQLitePath), "opentw.heartbeat.maxage")
	if err := os.WriteFile(maxAgePath, []byte(fmt.Sprintf("%d\n", int64((2*cfg.Interval+worstCycle+time.Minute).Seconds()))), 0o644); err != nil {
		log.Warn("opentw: heartbeat maxage write failed", "err", err)
	}

	runner := monitor.NewRunner(monitor.RunnerCfg{
		API:           apiClient,
		Store:         store,
		Models:        models,
		Concurrency:   cfg.Concurrency,
		MaxTokens:     cfg.MaxTokens,
		Interval:      cfg.Interval,
		HeartbeatPath: filepath.Join(filepath.Dir(cfg.SQLitePath), "opentw.heartbeat"),
		// The bot prepares the events; the runner commits them atomically with
		// the states.
		OnCycle: func(res monitor.CycleResult) ([]storage.OutboxEvent, error) {
			return bot.OnCycle(context.Background(), res)
		},
		Logger: log,
	})

	go func() {
		<-ctx.Done()
		log.Info("opentw: shutting down")
	}()

	// RunDelivery: panels and notifications are delivered asynchronously
	// from the cycles — an unreachable Telegram does not stop monitoring,
	// undelivered work waits in the outbox.
	return supervise(ctx, stop, []func(context.Context) error{bot.Run, bot.RunDelivery, runner.Run})
}

// worstCycleTime is the confirmed maximum duration of one cycle: the
// catalog request + waves of ceil(N/concurrency) checks, each wave bounded
// by http_timeout. The formula matches the Containerfile HEALTHCHECK
// (start-period covers the worst first cycle) and the maxage file.
func worstCycleTime(models, concurrency int, httpTimeout time.Duration) time.Duration {
	waves := (models + concurrency - 1) / concurrency
	if waves < 1 {
		waves = 1
	}
	return time.Duration(waves+1) * httpTimeout
}

// supervise keeps run() open until both components fully finish: cancel
// (SIGINT/SIGTERM or one component's failure) → then wg.Wait() — and only
// then run() returns, where the deferred store.Close() closes the database.
// The wait is finite: the bot's network operations are bounded by its HTTP
// client and shutdownFlush.
func supervise(ctx context.Context, stop context.CancelFunc, components []func(context.Context) error) error {
	errCh := make(chan error, len(components))
	var wg sync.WaitGroup
	for _, run := range components {
		wg.Add(1)
		go func(run func(context.Context) error) {
			defer wg.Done()
			errCh <- run(ctx)
		}(run)
	}
	var runErr error
	select {
	case <-ctx.Done(): // stop signal
	case err := <-errCh: // a component stopped on its own
		if err != nil {
			runErr = fmt.Errorf("opentw: component stopped: %w", err)
		}
		stop() // cancel the other component so run() does not wait for it forever
	}
	wg.Wait()
	return runErr
}
