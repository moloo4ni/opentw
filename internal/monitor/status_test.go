package monitor

import (
	"testing"

	"opentw/internal/api"
)

func mk(kind string, httpStatus int) api.RawResult {
	return api.RawResult{ErrorKind: kind, HTTPStatus: httpStatus}
}

func msg(kind string, httpStatus int, m string) api.RawResult {
	return api.RawResult{ErrorKind: kind, HTTPStatus: httpStatus, ErrorMessage: m}
}

func contentOK(kind string, content string) api.RawResult {
	return api.RawResult{ErrorKind: kind, HTTPStatus: 200, Content: content, Response: content}
}

// Матрица классификации 15.1: успешна только проверка с итоговым
// содержимым «OK» (без reasoning); любой иной текст при HTTP 200 — Down;
// проблемы самого монитора — Unknown.
func TestClassify(t *testing.T) {
	cases := []struct {
		name string
		res  api.RawResult
		want Status
	}{
		{"200 + OK", contentOK("", "OK"), StatusOperational},
		{"200 + OK снаружи-пробелами", contentOK("", " OK\n"), StatusOperational},
		{"200 + OK, высокая latency", api.RawResult{ErrorKind: "", HTTPStatus: 200, Content: "OK", LatencyUS: 120_000_000}, StatusOperational},
		{"200 + китайский текст", contentOK("", "抱歉，无法回答这个问题。"), StatusDown},
		{"200 + английский текст об ошибке", contentOK("", "Sorry, I cannot do that."), StatusDown},
		{"200 + русский текст", contentOK("", "Извините, я не могу выполнить этот запрос."), StatusDown},
		{"200 + любой иной текст", contentOK("", "Hello!"), StatusDown},
		{"200 + lowercase OK", contentOK("", "ok"), StatusDown},
		{"200 + пустой ответ (empty)", mk("empty", 200), StatusDown},
		{"200 + только reasoning, без итога", api.RawResult{ErrorKind: "", HTTPStatus: 200, Content: ""}, StatusDown},
		{"http 429", mk("http", 429), StatusDown},
		{"http 404", mk("http", 404), StatusDown},
		{"http 500", mk("http", 500), StatusDown},
		{"http 503", mk("http", 503), StatusDown},
		{"timeout", mk("timeout", 0), StatusDown},
		{"connection refused", mk("connrefused", 0), StatusDown},
		{"http 401 auth", mk("http", 401), StatusUnknown},
		{"http 403 forbidden", mk("http", 403), StatusUnknown},
		{"http 400 incompatible request", mk("http", 400), StatusUnknown},
		{"invalid response", mk("invalid", 0), StatusUnknown},
	}
	for _, tc := range cases {
		if got := Classify(tc.res); got != tc.want {
			t.Errorf("%s: Classify = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// reasoning не является ответом: «мусор» в reasoning при итоговом «OK» —
// Operational; «OK» в reasoning при ином итоговом тексте — Down.
func TestClassifyReasoningIgnoredForVerdict(t *testing.T) {
	if got := Classify(contentOK("", "OK")); got != StatusOperational {
		t.Errorf("reasoning мусора не видно: итог OK → %q", got)
	}
	if got := Classify(contentOK("", "Не могу ответить")); got != StatusDown {
		t.Errorf("reasoning OK не спасает: итог не OK → %q", got)
	}
	if got := Classify(api.RawResult{ErrorKind: "", HTTPStatus: 200, Content: "OK"}); got != StatusOperational {
		t.Errorf("без reasoning: итог OK → %q", got)
	}
}

// Пользовательский контракт: отдельное служебное reasoning поле не меняет
// классификацию; итоговый content обязан быть ровно OK после TrimSpace.
// Reasoning непосредственно в content — это пользовательский текст и Down.
func TestClassifyStrictUserContent(t *testing.T) {
	cases := []struct {
		name    string
		content string
		want    Status
	}{
		{"separate reasoning and OK", "OK", StatusOperational},
		{"no reasoning and OK", "OK", StatusOperational},
		{"reasoning text as content", "Let me think through this carefully...", StatusDown},
		{"reasoning marker and OK as content", "<think>...OK", StatusDown},
		{"reasoning plus answer", "reasoning + answer", StatusDown},
		{"OK with outer spaces", " OK ", StatusOperational},
		{"OK with punctuation", "OK.", StatusDown},
		{"lowercase ok", "ok", StatusDown},
		{"empty final content", "", StatusDown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := api.RawResult{
				ErrorKind:    "",
				HTTPStatus:   200,
				Content:      tc.content,
				Response:     tc.content,
				FinishReason: "stop",
			}
			if got := Classify(res); got != tc.want {
				t.Fatalf("Classify(%q) = %q, want %q", tc.content, got, tc.want)
			}
		})
	}
}

func TestContentOK(t *testing.T) {
	for _, tc := range []struct {
		content string
		want    bool
	}{
		{"OK", true},
		{"  OK  ", true},
		{"\nOK\r\n", true},
		{"ok", false},  // регистр важен
		{"OK.", false}, // ровно «OK»
		{"", false},
		{"OK OK", false},
	} {
		if got := ContentOK(tc.content); got != tc.want {
			t.Errorf("ContentOK(%q) = %v, want %v", tc.content, got, tc.want)
		}
	}
}

func TestClassifySkipsCancelled(t *testing.T) {
	// cancelled — не результат проверки вовсе; отдельного статуса не имеет
	// (вызывающая сторона обязана отфильтровать его до записи)
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Classify panicked on cancelled: %v", r)
		}
	}()
	Classify(mk("cancelled", 0))
}

// Провайдер сам заявляет недоступность модели — живые формулировки live-аудита
// (kimi-k2.6 «has been offline», kimi-k3 «model "..." not available», HTTP 400)
// обязаны давать 🔴; похожие ошибки не про недоступность модели — остаются ⚪.
func TestClassifyProviderOfflineMessages(t *testing.T) {
	cases := []struct {
		name string
		res  api.RawResult
		want Status
	}{
		// живые формулировки аудита
		{"400 kimi has been offline", msg("http", 400, "The model kimi-k2.6 has been offline"), StatusDown},
		{"400 kimi not available", msg("http", 400, `model "<待填目标模型>" not available`), StatusDown},
		// близкие варианты той же семантики
		{"400 is offline", msg("http", 400, "The model gpt-x is offline"), StatusDown},
		{"400 was offline", msg("http", 400, "model was offline during maintenance"), StatusDown},
		{"422 offline wording", msg("http", 422, "The model kimi-k3 has been offline"), StatusDown},
		// похожие, но НЕ о недоступности модели
		{"400 upstream error", msg("http", 400, "Upstream error: 400"), StatusUnknown},
		{"400 incompatible param", msg("http", 400, "Invalid request: parameter 'max_tokens' not available in this mode"), StatusUnknown},
		{"400 not-available before model", msg("http", 400, "tool use is not available for this model"), StatusUnknown},
		{"400 no available accounts", msg("http", 400, "No available accounts. Please retry after 12:34 MSK"), StatusUnknown},
		{"400 request config", msg("http", 400, "Unknown request configuration"), StatusUnknown},
		{"401 stays unknown despite wording", msg("http", 401, "The model x is offline"), StatusUnknown},
		// существующие маркеры «модель не найдена» продолжают работать
		{"400 unknown provider", msg("http", 400, "Unknown provider for model 'kimi-k3'"), StatusDown},
		{"400 invalid model", msg("http", 400, "Invalid model."), StatusDown},
		{"400 no such model", msg("http", 400, "No such model: gpt-x"), StatusDown},
		{"400 model not found", msg("http", 400, "Model not found in registry"), StatusDown},
	}
	for _, tc := range cases {
		if got := Classify(tc.res); got != tc.want {
			t.Errorf("%s: Classify = %q, want %q", tc.name, got, tc.want)
		}
	}
}
