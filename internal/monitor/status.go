// Package monitor orchestrates model checks and determines statuses.
package monitor

import (
	"opentw/internal/api"
	"opentw/internal/status"
)

// Status aliases the shared status type for monitor-internal use.
type Status = status.Status

const (
	StatusOperational = status.Operational
	StatusDegraded    = status.Degraded
	StatusDown        = status.Down
	StatusUnknown     = status.Unknown
)

// Classify maps one raw check result to a status, per ТЗ §5.
// Cancelled results are filtered by the caller before recording.
func Classify(res api.RawResult) Status {
	switch res.ErrorKind {
	case "":
		return StatusOperational
	case "empty":
		// HTTP 200 with nothing generated.
		return StatusDegraded
	case "http":
		switch {
		case res.HTTPStatus == 429:
			// Rate limit of the upstream provider, passed through by Tonworld.
			return StatusDegraded
		case res.HTTPStatus == 404:
			// Model not found.
			return StatusDown
		case res.HTTPStatus >= 500:
			// A single 503 is Down, not a warning.
			return StatusDown
		case res.HTTPStatus == 401 || res.HTTPStatus == 403:
			// The monitor's own authorization problem.
			return StatusUnknown
		default:
			// Other 4xx: incompatible request/response shape — monitoring issue.
			return StatusUnknown
		}
	case "notfound":
		// Configured model disappeared from the API catalog.
		return StatusDown
	case "timeout", "connrefused":
		return StatusDown
	case "invalid":
		return StatusUnknown
	default:
		return StatusUnknown
	}
}
