//go:build live

package monitor

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	"opentw/internal/api"
	"opentw/internal/storage"
)

// TestLiveCycle runs one real cycle against the API from the environment.
// Run: LIVE=1 go test -tags live ./internal/monitor -run TestLiveCycle -v
func TestLiveCycle(t *testing.T) {
	if os.Getenv("LIVE") == "" {
		t.Skip("set LIVE=1 to run against the real API")
	}
	key := os.Getenv("API_KEY")
	base := os.Getenv("API_BASE_URL")
	if key == "" || base == "" {
		t.Fatal("API_KEY and API_BASE_URL are required")
	}
	path := "/tmp/opentw-livecheck.db"
	os.Remove(path)
	store, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	families := []struct {
		family string
		ids    []string
	}{
		{"GPT", []string{"gpt-6-astra", "gpt-6.1-sol"}},
		{"GLM", []string{"glm-5.3", "glm-5.3-fast"}},
		{"DeepSeek", []string{"deepseek-v4.1-flash", "deepseek-v4-pro"}},
		{"Kimi", []string{"kimi-k3", "kimi-k2.6"}},
		{"Grok", []string{"grok-4.7", "grok-4.5"}},
	}
	var models []storage.Model
	for _, f := range families {
		for _, id := range f.ids {
			models = append(models, storage.Model{ID: id, Family: f.family})
		}
	}
	if err := store.SyncModels(models); err != nil {
		t.Fatal(err)
	}
	client := &api.Client{
		BaseURL:    base,
		APIKey:     key,
		HTTPClient: &http.Client{Timeout: 60 * time.Second},
		Prompt:     "Reply with exactly \"OK\"",
		Stream:     true,
	}
	r := NewRunner(RunnerCfg{
		API: client, Store: store, Models: models,
		Concurrency: 2, MaxTokens: 64, Interval: 15 * time.Minute,
		Logger: slog.Default(),
	})
	res, err := r.RunCycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range models {
		c := res.Checks[m.ID]
		latency := "N/A"
		if c.LatencyUS != nil {
			latency = fmt.Sprintf("%.2fs", float64(*c.LatencyUS)/1e6)
		}
		ttft := "N/A"
		if c.TTFTUS != nil {
			ttft = fmt.Sprintf("%.2fs", float64(*c.TTFTUS)/1e6)
		}
		tps := "N/A"
		if c.TPS != nil {
			tps = fmt.Sprintf("%.1f", *c.TPS)
		}
		t.Logf("%-28s %-12s HTTP=%-3d lat=%-7s ttft=%-7s tps=%-6s err=%q",
			m.ID, c.Status, c.HTTPStatus, latency, ttft, tps, c.ErrorMessage)
	}
}
