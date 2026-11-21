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

	// Content is the verbatim final answer of the model (message.content /
	// accumulated content deltas; reasoning never counted).
	Content string

	// FinishReason is the provider-reported completion reason for the choice
	// whose Content is used above. Empty when the provider omitted it or sent
	// null. It is diagnostic only and never participates in status classification.
	FinishReason string

	// Response is the verbatim diagnostic text of what the API or model
	// actually returned: the generated content when any, otherwise the
	// provider/gateway error message, otherwise the raw body of an
	// unparseable response. Empty when there is nothing to quote (e.g. a
	// transport failure). Stored as-is; never translated or summarized.
	Response string

	// ErrorKind: "" success transport-wise (HTTP 200, generated something),
	// "empty" (HTTP 200 without CONTENT — reasoning alone does not count
	// for the answer, but still tags the check as generated),
	// "http" (non-200 response; see HTTPStatus/ErrorMessage),
	// "timeout", "connrefused", "invalid" (unparseable response),
	// "cancelled" (monitor shutdown; skip recording).
	ErrorKind    string
	ErrorMessage string // original provider text, verbatim
	Streamed     bool
}

func ptr64(v int64) *int64      { return &v }
func ptrF64(v float64) *float64 { return &v }
