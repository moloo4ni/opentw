// Package monitor orchestrates model checks and determines statuses.
package monitor

import (
	"strings"

	"github.com/moloo4ni/opentw/internal/api"
	"github.com/moloo4ni/opentw/internal/status"
)

// Status aliases the shared status type for monitor-internal use.
type Status = status.Status

const (
	StatusOperational = status.Operational
	StatusDown        = status.Down
	StatusUnknown     = status.Unknown
)

// expectedReply is the only answer counted as a successful check: a reply
// to `Reply with exactly "OK"` must be exactly "OK". Case-sensitive; only
// outer whitespace may be trimmed.
const expectedReply = "OK"

// ContentOK decides whether the model fulfilled the probe task: the final
// answer content (reasoning excluded) must be exactly "OK".
func ContentOK(content string) bool {
	return strings.TrimSpace(content) == expectedReply
}

// Classify maps one raw check result to a status. A check succeeds only on
// the expected exact "OK" answer; HTTP 200 with any other (including empty)
// content is Down: the model failed the probe. Monitor-side problems (own
// key, unparseable response) are Unknown. Cancelled results are filtered by
// the caller before recording.
func Classify(res api.RawResult) Status {
	switch res.ErrorKind {
	case "":
		// HTTP 200 with generation: the verdict follows the final content.
		if ContentOK(res.Content) {
			return StatusOperational
		}
		return StatusDown
	case "empty":
		// HTTP 200 without generation: the model did not answer the probe.
		return StatusDown
	case "http":
		switch {
		case res.HTTPStatus == 429:
			// Provider rate limit for this model: the model is unavailable.
			return StatusDown
		case res.HTTPStatus == 404:
			// Model not found.
			return StatusDown
		case res.HTTPStatus >= 500:
			// A single 503 is Down, not a warning.
			return StatusDown
		case res.HTTPStatus == 401 || res.HTTPStatus == 403:
			// The monitor's own authorization problem.
			return StatusUnknown
		case looksLikeModelNotFound(res.ErrorMessage):
			// A 400 from the router: the model/provider is not served.
			return StatusDown
		case looksLikeModelOffline(res.ErrorMessage):
			// The provider explicitly states this model is offline
			// ("The model kimi-k2.6 has been offline", HTTP 400): Down,
			// not "the monitor cannot check".
			return StatusDown
		default:
			// Other 4xx: incompatible request/response shape — monitoring issue.
			return StatusUnknown
		}
	case "timeout", "connrefused":
		return StatusDown
	default:
		// "invalid" and the rest: the monitor could not verify the model.
		return StatusUnknown
	}
}

// looksLikeModelNotFound recognizes 400 responses about a missing model.
func looksLikeModelNotFound(msg string) bool {
	m := strings.ToLower(msg)
	for _, marker := range []string{
		"unknown provider for model",
		"model not found",
		"does not exist",
		"no such model",
		"invalid model",
	} {
		if strings.Contains(m, marker) {
			return true
		}
	}
	return false
}

// looksLikeModelOffline recognizes the narrow class of messages where the
// provider itself declares a model unavailable: observed live wordings are
// "The model kimi-k2.6 has been offline" and "model "..." not available"
// (HTTP 400). Other 4xx (incompatible request, configuration, quota or
// account issues) do not match and stay Unknown.
func looksLikeModelOffline(msg string) bool {
	m := strings.ToLower(msg)
	for _, marker := range []string{
		"has been offline",
		"is offline",
		"was offline",
	} {
		if strings.Contains(m, marker) {
			return true
		}
	}
	// "model … not available": the word about a specific model comes BEFORE
	// the unavailability claim. "tool use is not available for this model"
	// and "no available accounts" must not match.
	mi, ni := strings.Index(m, "model"), strings.Index(m, "not available")
	return mi >= 0 && ni > mi
}
