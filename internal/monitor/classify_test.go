package monitor

import (
	"testing"

	"opentw/internal/api"
)

// 400 от роутера с сообщением об отсутствии модели/провайдера — это
// фактически «model not found», то есть Down, а не проблема мониторинга.
func TestClassifyModelNotFoundBodies(t *testing.T) {
	cases := []struct {
		msg  string
		want Status
	}{
		{"unknown provider for model gpt-6-astra", StatusDown},
		{"model not found: gpt-6-astra", StatusDown},
		{"The model `x` does not exist", StatusDown},
		{"no such model: x", StatusDown},
		{"invalid model identifier", StatusDown},
		{"Upstream error: 400", StatusUnknown},
		{"invalid_request_error", StatusUnknown},
		{"", StatusUnknown},
	}
	for _, tc := range cases {
		got := Classify(api.RawResult{ErrorKind: "http", HTTPStatus: 400, ErrorMessage: tc.msg})
		if got != tc.want {
			t.Errorf("400 %q → %q, want %q", tc.msg, got, tc.want)
		}
	}
}

// 429 остаётся Degraded независимо от текста.
func TestClassify429AlwaysDegraded(t *testing.T) {
	for _, msg := range []string{"Upstream rate limit", "unknown provider for model x", ""} {
		if got := Classify(api.RawResult{ErrorKind: "http", HTTPStatus: 429, ErrorMessage: msg}); got != StatusDegraded {
			t.Errorf("429 %q → %q", msg, got)
		}
	}
}
