// Package monitor orchestrates model checks and determines statuses.
package monitor

import "opentw/internal/api"

// Status is one of the four model states.
type Status string

const (
	StatusOperational Status = "operational"
	StatusDegraded    Status = "degraded"
	StatusDown        Status = "down"
	StatusUnknown     Status = "unknown"
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
	case "timeout", "connrefused":
		return StatusDown
	case "invalid":
		return StatusUnknown
	default:
		return StatusUnknown
	}
}

// Emoji returns the status icon shown in messages.
func (s Status) Emoji() string {
	switch s {
	case StatusOperational:
		return "🟢"
	case StatusDegraded:
		return "🟡"
	case StatusDown:
		return "🔴"
	default:
		return "⚪"
	}
}

// Title returns the human status name for statuses.
func (s Status) Title() string {
	switch s {
	case StatusOperational:
		return "Operational"
	case StatusDegraded:
		return "Degraded"
	case StatusDown:
		return "Down"
	default:
		return "Unknown"
	}
}
