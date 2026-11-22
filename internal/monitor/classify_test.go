package monitor

import (
	"testing"

	"github.com/moloo4ni/opentw/internal/api"
)

// A router 400 with a missing model/provider message is effectively
// "model not found" — Down, not a monitoring problem.
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

// 429 — the unavailability of one specific model (Down), regardless of text.
func TestClassify429AlwaysDown(t *testing.T) {
	for _, msg := range []string{"Upstream rate limit", "unknown provider for model x", ""} {
		if got := Classify(api.RawResult{ErrorKind: "http", HTTPStatus: 429, ErrorMessage: msg}); got != StatusDown {
			t.Errorf("429 %q → %q, want down", msg, got)
		}
	}
}
