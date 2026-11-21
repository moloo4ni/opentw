// Package status holds the model status type shared by storage, monitor
// and telegram layers (keeps the import graph acyclic).
package status

// Status is one of the three model states.
// Model unavailable for any real reason (HTTP, transport, unexpected
// content) is Down; only monitor-side problems (own auth, broken gateway
// correlation) are Unknown.
type Status string

const (
	Operational Status = "operational"
	Down        Status = "down"
	Unknown     Status = "unknown"
)

// Emoji returns the status icon shown in messages.
func (s Status) Emoji() string {
	switch s {
	case Operational:
		return "🟢"
	case Down:
		return "🔴"
	default:
		return "⚪"
	}
}

// Notifiable reports whether the state participates in event notifications
// (only 🟢/🔴 transitions are events; ⚪ is a monitoring problem).
func (s Status) Notifiable() bool {
	return s == Operational || s == Down
}
