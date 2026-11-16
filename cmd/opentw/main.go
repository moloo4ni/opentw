// OpenTW — Telegram bot monitoring model availability through the Tonworld API.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"opentw/internal/api"
	"opentw/internal/config"
	"opentw/internal/monitor"
	"opentw/internal/storage"
	"opentw/internal/telegram"
)

// testPrompt — стандартизированный запрос проверки, одинаковый для всех моделей.
const testPrompt = "Reply with exactly OK."

func main() {
	if err := run(); err != nil {
		slog.Error("opentw: fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "config.yaml", "path to config.yaml")
	backup := flag.String("backup", "", "write a consistent SQLite snapshot to this path and exit")
	flag.Parse()

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
		// VACUUM INTO: атомарный снапшот живой WAL-базы без остановки и без
		// sqlite3 в образе; отказ, если файл уже существует (I3)
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

	// Порог живости вычисляется из фактического конфига и пишется рядом с
	// heartbeat: HEALTHCHECK читает его, образ не требует пересборки при
	// смене интервала (I2). 2×интервал + худший возможный цикл + минута.
	worstCycle := time.Duration((len(models)+cfg.Concurrency-1)/cfg.Concurrency) * cfg.HTTPTimeout
	maxAgePath := filepath.Join(filepath.Dir(cfg.SQLitePath), "opentw.heartbeat.maxage")
	if err := os.WriteFile(maxAgePath, []byte(fmt.Sprintf("%d\n", int64((2*cfg.Interval+worstCycle+time.Minute).Seconds()))), 0o644); err != nil {
		log.Warn("opentw: heartbeat maxage write failed", "err", err)
	}

	runner := monitor.NewRunner(monitor.RunnerCfg{
		API:         apiClient,
		Store:       store,
		Models:      models,
		Concurrency: cfg.Concurrency,
		MaxTokens:   cfg.MaxTokens,
		Interval:    cfg.Interval,
		Perf: monitor.PerfCfg{
			MinSamples:    cfg.Degradation.MinSamples,
			LatencyFactor: cfg.Degradation.LatencyFactor,
			TPSShare:      cfg.Degradation.TPSShare,
		},
		HeartbeatPath: filepath.Join(filepath.Dir(cfg.SQLitePath), "opentw.heartbeat"),
		// События готовит бот; раннер зафиксирует их атомарно с состояниями (P0-1)
		OnCycle: func(res monitor.CycleResult) ([]storage.OutboxEvent, error) {
			return bot.OnCycle(context.Background(), res)
		},
		Logger: log,
	})

	go func() {
		<-ctx.Done()
		log.Info("opentw: shutting down")
	}()

	// RunDelivery: панели и уведомления доставляются асинхронно от циклов —
	// недоступный Telegram не останавливает мониторинг, недоставленное уходит в outbox.
	return supervise(ctx, stop, []func(context.Context) error{bot.Run, bot.RunDelivery, runner.Run})
}

// supervise держит run() открытой до полного завершения обоих компонентов:
// отмена (SIGINT/SIGTERM или падение одного из них) → затем wg.Wait() — и
// только потом run() возвращает управление, где defer store.Close() закрывает
// базу. Ожидание конечно: сетевые операции бота ограничены его HTTP-клиентом
// и shutdownFlush.
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
	case <-ctx.Done(): // сигнал остановки
	case err := <-errCh: // компонент остановился сам
		if err != nil {
			runErr = fmt.Errorf("opentw: component stopped: %w", err)
		}
		stop() // отменяем второй компонент, чтобы run() не ждал его вечно
	}
	wg.Wait()
	return runErr
}
