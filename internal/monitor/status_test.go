package monitor

import (
	"strings"
	"testing"

	"opentw/internal/api"
)

func mk(kind string, httpStatus int) api.RawResult {
	return api.RawResult{ErrorKind: kind, HTTPStatus: httpStatus}
}

func TestClassify(t *testing.T) {
	cases := []struct {
		name string
		res  api.RawResult
		want Status
	}{
		{"success stream", mk("", 200), StatusOperational},
		{"success no stream", mk("", 200), StatusOperational},
		{"empty generation", mk("empty", 200), StatusDegraded},
		{"http 429 upstream rate limit", mk("http", 429), StatusDegraded},
		{"http 503", mk("http", 503), StatusDown},
		{"http 500", mk("http", 500), StatusDown},
		{"http 502", mk("http", 502), StatusDown},
		{"http 404 model not found", mk("http", 404), StatusDown},
		{"http 401 auth", mk("http", 401), StatusUnknown},
		{"http 403 forbidden", mk("http", 403), StatusUnknown},
		{"http 400 incompatible request", mk("http", 400), StatusUnknown},
		{"timeout", mk("timeout", 0), StatusDown},
		{"connection refused", mk("connrefused", 0), StatusDown},
		{"invalid response", mk("invalid", 0), StatusUnknown},
	}
	for _, tc := range cases {
		if got := Classify(tc.res); got != tc.want {
			t.Errorf("%s: Classify = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestClassySkipCancelled(t *testing.T) {
	// cancelled — не результат проверки вовсе; отдельного статуса не имеет
	// (вызывающая сторона обязана отфильтровать его до записи)
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Classify panicked on cancelled: %v", r)
		}
	}()
	Classify(mk("cancelled", 0))
}

func samples(lat []int64, tps []float64) []PerfSample {
	out := make([]PerfSample, len(lat))
	for i, l := range lat {
		out[i] = PerfSample{LatencyUS: l}
		if tps != nil && i < len(tps) {
			out[i].TPS = tps[i]
			out[i].HasTPS = true
		}
	}
	return out
}

func TestDegradedByPerf(t *testing.T) {
	cfg := PerfCfg{MinSamples: 12, LatencyFactor: 3.0, TPSShare: 0.4}

	// 12 стабильных исторических образцов: latency 90–110ms, TPS 95–105
	histLat := make([]int64, 12)
	histTPS := make([]float64, 12)
	for i := range histLat {
		histLat[i] = 90_000 + int64(i)*2_000 // 90..112ms
		histTPS[i] = 95 + float64(i)*0.9
	}

	t.Run("insufficient history", func(t *testing.T) {
		s := samples(histLat[:11], nil)
		if degraded, _ := DegradedByPerf(cfg, s, PerfSample{LatencyUS: 10_000_000}); degraded {
			t.Fatal("must not degrade with too few samples")
		}
	})

	t.Run("latency spike", func(t *testing.T) {
		s := samples(histLat, nil)
		degraded, reason := DegradedByPerf(cfg, s, PerfSample{LatencyUS: 1_000_000})
		if !degraded {
			t.Fatal("latency 10x history must degrade")
		}
		if !strings.Contains(reason, "P95") {
			t.Fatalf("latency reason must cite P95: %q", reason)
		}
	})

	t.Run("small fluctuation", func(t *testing.T) {
		s := samples(histLat, nil)
		if degraded, _ := DegradedByPerf(cfg, s, PerfSample{LatencyUS: 120_000}); degraded {
			t.Fatal("1.2x fluctuation must not degrade")
		}
	})

	t.Run("tps collapse", func(t *testing.T) {
		s := samples(histLat, histTPS)
		degraded, reason := DegradedByPerf(cfg, s, PerfSample{LatencyUS: 100_000, TPS: 10, HasTPS: true})
		if !degraded {
			t.Fatal("TPS collapse must degrade")
		}
		if !strings.Contains(reason, "медианы") {
			t.Fatalf("tps reason must cite median: %q", reason)
		}
	})

	t.Run("tps mild dip", func(t *testing.T) {
		s := samples(histLat, histTPS)
		if degraded, _ := DegradedByPerf(cfg, s, PerfSample{LatencyUS: 100_000, TPS: 80, HasTPS: true}); degraded {
			t.Fatal("TPS 80 of ~100 must not degrade (share 0.4)")
		}
	})

	t.Run("current without tps ignores tps rule", func(t *testing.T) {
		s := samples(histLat, histTPS)
		if degraded, _ := DegradedByPerf(cfg, s, PerfSample{LatencyUS: 100_000}); degraded {
			t.Fatal("latency fine, no TPS data — must not degrade")
		}
	})

	t.Run("history without tps skips tps rule", func(t *testing.T) {
		s := samples(histLat, nil)
		if degraded, _ := DegradedByPerf(cfg, s, PerfSample{LatencyUS: 100_000, TPS: 1, HasTPS: true}); degraded {
			t.Fatal("no TPS history: rule cannot fire")
		}
	})
}

func msg(kind string, httpStatus int, m string) api.RawResult {
	return api.RawResult{ErrorKind: kind, HTTPStatus: httpStatus, ErrorMessage: m}
}

// Провайдер сам заявляет недоступность модели — живые формулировки live-аудита
// (kimi-k2.6 «has been offline», kimi-k3 «model "..." not available», HTTP 400)
// обязаны давать 🔴; похожие ошибки не про недоступность модели — остаются ⚪.
func TestClassifyProviderOfflineMessages(t *testing.T) {
	cases := []struct {
		name string
		res  api.RawResult
		want Status
	}{
		// живые формулировки аудита
		{"400 kimi has been offline", msg("http", 400, "The model kimi-k2.6 has been offline"), StatusDown},
		{"400 kimi not available", msg("http", 400, `model "<待填目标模型>" not available`), StatusDown},
		// близкие варианты той же семантики
		{"400 is offline", msg("http", 400, "The model gpt-x is offline"), StatusDown},
		{"400 was offline", msg("http", 400, "model was offline during maintenance"), StatusDown},
		{"422 offline wording", msg("http", 422, "The model kimi-k3 has been offline"), StatusDown},
		// похожие, но НЕ о недоступности модели
		{"400 upstream error", msg("http", 400, "Upstream error: 400"), StatusUnknown},
		{"400 incompatible param", msg("http", 400, "Invalid request: parameter 'max_tokens' not available in this mode"), StatusUnknown},
		{"400 not-available before model", msg("http", 400, "tool use is not available for this model"), StatusUnknown},
		{"400 no available accounts", msg("http", 400, "No available accounts. Please retry after 12:34 MSK"), StatusUnknown},
		{"400 request config", msg("http", 400, "Unknown request configuration"), StatusUnknown},
		{"401 stays unknown despite wording", msg("http", 401, "The model x is offline"), StatusUnknown},
		// существующие маркеры «модель не найдена» продолжают работать
		{"400 unknown provider", msg("http", 400, "Unknown provider for model 'kimi-k3'"), StatusDown},
		{"400 invalid model", msg("http", 400, "Invalid model."), StatusDown},
		{"400 no such model", msg("http", 400, "No such model: gpt-x"), StatusDown},
		{"400 model not found", msg("http", 400, "Model not found in registry"), StatusDown},
	}
	for _, tc := range cases {
		if got := Classify(tc.res); got != tc.want {
			t.Errorf("%s: Classify = %q, want %q", tc.name, got, tc.want)
		}
	}
}
