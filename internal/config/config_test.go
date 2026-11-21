package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func setEnv(t *testing.T) {
	t.Setenv("BOT_TOKEN", "123:test-token")
	t.Setenv("API_KEY", "test-api-key")
	t.Setenv("API_BASE_URL", "https://kube943.prod.tonworld.ru")
}

func TestLoadFullConfig(t *testing.T) {
	setEnv(t)
	path := writeFile(t, "config.yaml", `
api_base_url: https://fallback.example
interval: 10m
http_timeout: 45s
concurrency: 6
max_tokens: 12
streaming: false
sqlite_path: /var/lib/opentw/opentw.db
degradation:
  min_samples: 20
  latency_factor: 2.5
  tps_share: 0.5
# блок degradation больше не используется (три статуса без деградации):
# неизвестные yaml-поля игнорируются — старые конфиги продолжают загружаться
models:
  - family: GPT
    ids: [gpt-6-astra, gpt-6.1-sol]
  - family: GLM
    ids: [glm-5.3]
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.BotToken != "123:test-token" || cfg.APIKey != "test-api-key" {
		t.Fatalf("secrets not loaded from env")
	}
	if cfg.APIBaseURL != "https://kube943.prod.tonworld.ru" {
		t.Fatalf("env must override yaml, got %q", cfg.APIBaseURL)
	}
	if cfg.Interval != 10*time.Minute {
		t.Fatalf("interval = %v", cfg.Interval)
	}
	if cfg.HTTPTimeout != 45*time.Second {
		t.Fatalf("http_timeout = %v", cfg.HTTPTimeout)
	}
	if cfg.Concurrency != 6 || cfg.MaxTokens != 12 {
		t.Fatalf("concurrency/max_tokens = %d/%d", cfg.Concurrency, cfg.MaxTokens)
	}
	if cfg.Streaming {
		t.Fatal("streaming should be false")
	}
	if cfg.SQLitePath != "/var/lib/opentw/opentw.db" {
		t.Fatalf("sqlite_path = %q", cfg.SQLitePath)
	}
	if len(cfg.Models) != 2 {
		t.Fatalf("models families = %d", len(cfg.Models))
	}
	if len(cfg.ModelIDs()) != 3 {
		t.Fatalf("flattened model ids = %v", cfg.ModelIDs())
	}
}

func TestLoadDefaults(t *testing.T) {
	setEnv(t)
	path := writeFile(t, "config.yaml", `
models:
  - family: Qwen
    ids: [qwen3.8-max]
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Interval != 15*time.Minute {
		t.Fatalf("default interval = %v", cfg.Interval)
	}
	if cfg.HTTPTimeout != 120*time.Second {
		t.Fatalf("default http_timeout = %v", cfg.HTTPTimeout)
	}
	if cfg.Concurrency != 4 || cfg.MaxTokens != 256 {
		t.Fatalf("default concurrency/max_tokens = %d/%d", cfg.Concurrency, cfg.MaxTokens)
	}
	if !cfg.Streaming {
		t.Fatal("default streaming must be true")
	}
	if cfg.SQLitePath != "opentw.db" {
		t.Fatalf("default sqlite_path = %q", cfg.SQLitePath)
	}
}

func TestLoadEnvFallbackToYAML(t *testing.T) {
	t.Setenv("BOT_TOKEN", "t")
	t.Setenv("API_KEY", "k")
	t.Setenv("API_BASE_URL", "")
	path := writeFile(t, "config.yaml", `
api_base_url: https://yaml.example
models:
  - family: GLM
    ids: [glm-5.3]
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.APIBaseURL != "https://yaml.example" {
		t.Fatalf("yaml fallback expected, got %q", cfg.APIBaseURL)
	}
}

func TestLoadValidationErrors(t *testing.T) {
	base := `
api_base_url: https://yaml.example
models:
  - family: GLM
    ids: [glm-5.3]
`
	t.Run("missing BOT_TOKEN", func(t *testing.T) {
		t.Setenv("BOT_TOKEN", "")
		t.Setenv("API_KEY", "k")
		_, err := Load(writeFile(t, "config.yaml", base))
		if err == nil || err.Error() != "BOT_TOKEN is required" {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("missing API_KEY", func(t *testing.T) {
		t.Setenv("BOT_TOKEN", "t")
		t.Setenv("API_KEY", "")
		_, err := Load(writeFile(t, "config.yaml", base))
		if err == nil || err.Error() != "API_KEY is required" {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("missing file", func(t *testing.T) {
		setEnv(t)
		if _, err := Load(filepath.Join(t.TempDir(), "nope.yaml")); err == nil {
			t.Fatal("expected error for missing file")
		}
	})
	t.Run("no models", func(t *testing.T) {
		setEnv(t)
		_, err := Load(writeFile(t, "config.yaml", "interval: 15m\n"))
		if err == nil || err.Error() != "config: at least one model is required" {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("duplicate model id", func(t *testing.T) {
		setEnv(t)
		_, err := Load(writeFile(t, "config.yaml", `
models:
  - family: GLM
    ids: [glm-5.3, glm-5.3]
`))
		if err == nil || err.Error() != "config: duplicate model id: glm-5.3" {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("bad interval", func(t *testing.T) {
		setEnv(t)
		_, err := Load(writeFile(t, "config.yaml", "interval: 1m\n"+base))
		if err == nil || err.Error() != "config: interval must be >= 5m" {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("empty model id", func(t *testing.T) {
		setEnv(t)
		_, err := Load(writeFile(t, "config.yaml", `
models:
  - family: GLM
    ids: [glm-5.3, ""]
`))
		if err == nil || err.Error() != "config: empty model id in family GLM" {
			t.Fatalf("err = %v", err)
		}
	})
}
