// Package api is an isolated OpenAI-compatible adapter for the Tonworld API.
// Only what a regular user key can do: list models and one chat completion.
package api

import "time"

// RawResult carries raw facts of one model check: measurements and errors
// verbatim from the provider. Status classification lives in internal/monitor.
type RawResult struct {
	ModelID   string
	Timestamp time.Time

	HTTPStatus int // 0 when the transport failed (timeout / connection)

	LatencyUS    int64
	TTFTUS       *int64 // nil: not measured (no streaming or nothing generated)
	GenUS        *int64 // generation time after first content token
	TPS          *float64
	InputTokens  *int64
	OutputTokens *int64

	// ErrorKind: "" success, "empty" (HTTP 200 without generated content),
	// "http" (non-200 response; see HTTPStatus/ErrorMessage),
	// "timeout", "connrefused", "invalid" (unparseable response),
	// "cancelled" (monitor shutdown; skip recording).
	ErrorKind    string
	ErrorMessage string // original provider text, verbatim
	Streamed     bool
}

func ptr64(v int64) *int64      { return &v }
func ptrF64(v float64) *float64 { return &v }
