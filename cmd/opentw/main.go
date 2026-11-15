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
	"syscall"

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
		OnCycle: func(res monitor.CycleResult) {
			bot.OnCycle(context.Background(), res)
		},
		Logger: log,
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 2)
	go func() { errCh <- bot.Run(ctx) }()
	go func() { errCh <- runner.Run(ctx) }()

	select {
	case <-ctx.Done():
		log.Info("opentw: shutting down")
		return nil
	case err := <-errCh:
		if err != nil {
			stop()
			return fmt.Errorf("opentw: component stopped: %w", err)
		}
		return nil
	}
}
