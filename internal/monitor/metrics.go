package monitor

import "sort"

// PerfSample is one successful historical measurement of a model.
type PerfSample struct {
	LatencyUS int64
	TPS       float64
	HasTPS    bool
}

// PerfCfg are the conservative degradation thresholds (from config).
type PerfCfg struct {
	MinSamples    int     // required history size
	LatencyFactor float64 // current > p95·factor → degraded
	TPSShare      float64 // current < median·share → degraded
}

// DegradedByPerf decides a substantial performance regression from the
// model's own history. Conservative: needs MinSamples, and single
// small fluctuations never trip it.
func DegradedByPerf(cfg PerfCfg, samples []PerfSample, cur PerfSample) bool {
	if len(samples) < cfg.MinSamples || cfg.MinSamples < 1 {
		return false
	}

	lat := make([]int64, 0, len(samples))
	var tpsVals []float64
	for _, s := range samples {
		if s.LatencyUS > 0 {
			lat = append(lat, s.LatencyUS)
		}
		if s.HasTPS && s.TPS > 0 {
			tpsVals = append(tpsVals, s.TPS)
		}
	}

	if cur.LatencyUS > 0 && len(lat) >= cfg.MinSamples {
		p95 := percentileInt64(lat, 0.95)
		if p95 > 0 && float64(cur.LatencyUS) > float64(p95)*cfg.LatencyFactor {
			return true
		}
	}

	if cur.HasTPS && cur.TPS > 0 && len(tpsVals) >= cfg.MinSamples {
		med := medianF64(tpsVals)
		if med > 0 && cur.TPS < med*cfg.TPSShare {
			return true
		}
	}
	return false
}

func percentileInt64(v []int64, q float64) int64 {
	if len(v) == 0 {
		return 0
	}
	sorted := append([]int64(nil), v...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(q * float64(len(sorted))) // nearest-rank
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	if idx < 0 {
		idx = 0
	}
	return sorted[idx]
}

func medianF64(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	sorted := append([]float64(nil), v...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid]
	}
	return (sorted[mid-1] + sorted[mid]) / 2
}
