// Package monitor orchestrates model checks and determines statuses.
package monitor

import (
	"strings"

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
		case looksLikeModelNotFound(res.ErrorMessage):
			// 400 от роутера: модель/провайдер не обслуживается.
			return StatusDown
		case looksLikeModelOffline(res.ErrorMessage):
			// Провайдер явно сообщает, что конкретная модель offline
			// («The model kimi-k2.6 has been offline», HTTP 400) — это 🔴,
			// а не «монитор не может проверить».
			return StatusDown
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

// looksLikeModelNotFound распознаёт 400-ответы об отсутствии модели.
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

// looksLikeModelOffline распознаёт узкий класс сообщений, в которых сам
// провайдер заявляет недоступность конкретной модели: живые формулировки
// аудита — «The model kimi-k2.6 has been offline» и «model "..." not
// available» (HTTP 400). Прочие 4xx (несовместимый запрос, конфигурация,
// квота/аккаунты) сюда не попадают и остаются ⚪ Unknown.
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
	// «model … not available»: слово о конкретной модели, идущее РАНЬШЕ
	// заявления о недоступности. «tool use is not available for this model»
	// и «no available accounts» под маркер не подходят.
	mi, ni := strings.Index(m, "model"), strings.Index(m, "not available")
	return mi >= 0 && ni > mi
}
