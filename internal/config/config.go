// Package config loads OpenTW configuration: secrets strictly from ENV,
// the rest from a YAML file.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	envBotToken    = "BOT_TOKEN"
	envAPIKey      = "API_KEY"
	envAPIBase     = "API_BASE_URL"
	minIntervalStr = "5m"
	minTimeoutStr  = "5s"
	minTokens      = 1
)

// Degradation holds conservative performance-degradation thresholds.
type Degradation struct {
	MinSamples    int     `yaml:"min_samples"`
	LatencyFactor float64 `yaml:"latency_factor"`
	TPSShare      float64 `yaml:"tps_share"`
}

// ModelCfg is one model family group with exact model IDs.
type ModelCfg struct {
	Family string   `yaml:"family"`
	IDs    []string `yaml:"ids"`
}

// Config is the fully resolved application configuration.
type Config struct {
	// Secrets: ENV only, never YAML.
	BotToken string `yaml:"-"`
	APIKey   string `yaml:"-"`
	// APIBaseURL: ENV API_BASE_URL wins, otherwise YAML api_base_url.
	APIBaseURL string `yaml:"api_base_url"`

	Interval    time.Duration `yaml:"interval"`
	HTTPTimeout time.Duration `yaml:"http_timeout"`
	Concurrency int           `yaml:"concurrency"`
	MaxTokens   int           `yaml:"max_tokens"`
	Streaming   bool          `yaml:"streaming"`
	SQLitePath  string        `yaml:"sqlite_path"`
	Degradation struct {
		MinSamples    int     `yaml:"min_samples"`
		LatencyFactor float64 `yaml:"latency_factor"`
		TPSShare      float64 `yaml:"tps_share"`
	} `yaml:"degradation"`
	Models []ModelCfg `yaml:"models"`

	modelIDs []string // flattened, unique
}

// ModelIDs returns the flattened, ordered list of monitored model IDs.
func (c *Config) ModelIDs() []string { return c.modelIDs }

// Load reads the YAML file and env vars and validates the result.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	cfg := &Config{
		Interval:    15 * time.Minute,
		HTTPTimeout: 120 * time.Second,
		Concurrency: 4,
		MaxTokens:   64,
		Streaming:   true,
		SQLitePath:  "opentw.db",
	}
	cfg.Degradation.MinSamples = 12
	cfg.Degradation.LatencyFactor = 3.0
	cfg.Degradation.TPSShare = 0.4
	if err := yaml.Unmarshal(raw, cfg); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}

	cfg.BotToken = os.Getenv(envBotToken)
	cfg.APIKey = os.Getenv(envAPIKey)
	if envBase := os.Getenv(envAPIBase); envBase != "" {
		cfg.APIBaseURL = envBase
	} else if cfg.APIBaseURL == "" {
		return nil, fmt.Errorf("config: %s is required (no api_base_url in config)", envAPIBase)
	}
	if cfg.BotToken == "" {
		return nil, fmt.Errorf("%s is required", envBotToken)
	}
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("%s is required", envAPIKey)
	}
	if cfg.Interval < 5*time.Minute {
		return nil, fmt.Errorf("config: interval must be >= %s", minIntervalStr)
	}
	if cfg.HTTPTimeout < 5*time.Second {
		return nil, fmt.Errorf("config: http_timeout must be >= %s", minTimeoutStr)
	}
	if cfg.Concurrency < 1 {
		return nil, fmt.Errorf("config: concurrency must be >= 1")
	}
	if cfg.MaxTokens < minTokens {
		return nil, fmt.Errorf("config: max_tokens must be >= %d", minTokens)
	}
	if cfg.Degradation.MinSamples < 2 || cfg.Degradation.LatencyFactor <= 1 || cfg.Degradation.TPSShare <= 0 || cfg.Degradation.TPSShare >= 1 {
		return nil, fmt.Errorf("config: invalid degradation settings (need min_samples>=2, latency_factor>1, 0<tps_share<1)")
	}
	if len(cfg.Models) == 0 {
		return nil, fmt.Errorf("config: at least one model is required")
	}
	seen := map[string]bool{}
	for _, m := range cfg.Models {
		for _, id := range m.IDs {
			if id == "" {
				return nil, fmt.Errorf("config: empty model id in family %s", m.Family)
			}
			if seen[id] {
				return nil, fmt.Errorf("config: duplicate model id: %s", id)
			}
			seen[id] = true
			cfg.modelIDs = append(cfg.modelIDs, id)
		}
	}
	return cfg, nil
}
