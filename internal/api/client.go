package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// Client is the Tonworld API adapter. BaseURL, APIKey and HTTPClient are
// injectable; zero-value fields are invalid.
type Client struct {
	BaseURL    string
	APIKey     string
	HTTPClient *http.Client
	Prompt     string // standardized test prompt, same for every model
	Stream     bool   // use streaming (needed for TTFT/TPS)
}

func (c *Client) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return http.DefaultClient
}

// minGenerationUS — минимальная длительность генерации, при которой TPS
// считается достоверным (буферизованные SSE-потоки дают ~0).
const minGenerationUS = 100_000

// maxTextRunes — предел для сохраняемого текста ответа/ошибки провайдера.
// Заметно больше любых реальных тел (max_tokens ограничивает генерацию),
// но ограничивает патологические страницы ошибок в хранилище. Обрезка
// видима («…»), сырых данных больше, чем прочитано, всё равно не бывает:
// тела ограничены maxBodyBytes.
const maxTextRunes = 16384

// maxBodyBytes — сколько байтов тела ответа читаем и сохраняем.
const maxBodyBytes = 1 << 16

func readLimited(r io.Reader, limit int64) ([]byte, error) {
	return io.ReadAll(io.LimitReader(r, limit))
}

// ListModels returns the exact model IDs visible to the key (GET /v1/models).
func (c *Client) ListModels(ctx context.Context) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/v1/models", nil)
	if err != nil {
		return nil, fmt.Errorf("api: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.APIKey)
	req.Header.Set("Accept", "application/json")
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("api: models: %w", err)
	}
	defer resp.Body.Close()
	body, err := readLimited(resp.Body, 16<<20)
	if err != nil {
		return nil, fmt.Errorf("api: models: read body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("api: models: HTTP %d: %s", resp.StatusCode, extractProviderError(body, resp.StatusCode))
	}
	var parsed struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("api: models: invalid response: %w", err)
	}
	ids := make([]string, 0, len(parsed.Data))
	for _, m := range parsed.Data {
		if m.ID != "" {
			ids = append(ids, m.ID)
		}
	}
	return ids, nil
}

// Generate performs exactly one chat completion check for the model and
// measures latency, TTFT, generation time and (with streaming + usage) TPS.
// All failures are encoded in the returned result.
func (c *Client) Generate(ctx context.Context, modelID string, maxTokens int) *RawResult {
	res := &RawResult{ModelID: modelID, Timestamp: time.Now(), Streamed: c.Stream}

	payload := map[string]any{
		"model":      modelID,
		"messages":   []map[string]string{{"role": "user", "content": c.Prompt}},
		"max_tokens": maxTokens,
	}
	var accept string
	if c.Stream {
		payload["stream"] = true
		payload["stream_options"] = map[string]any{"include_usage": true}
		accept = "text/event-stream"
	} else {
		payload["stream"] = false
		accept = "application/json"
	}
	body, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		res.ErrorKind, res.ErrorMessage = "invalid", "build request: "+err.Error()
		return res
	}
	req.Header.Set("Authorization", "Bearer "+c.APIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", accept)

	start := time.Now()
	resp, err := c.httpClient().Do(req)
	if err != nil {
		res.LatencyUS = usSince(start)
		res.ErrorKind, res.ErrorMessage = classifyTransport(ctx, err), errSummary(err)
		return res
	}
	defer resp.Body.Close()
	res.HTTPStatus = resp.StatusCode

	if resp.StatusCode != http.StatusOK {
		errBody, _ := readLimited(resp.Body, maxBodyBytes)
		res.LatencyUS = usSince(start)
		res.ErrorKind = "http"
		res.ErrorMessage = extractProviderError(errBody, resp.StatusCode)
		// диагностический ответ API — дословно (провайдерский текст либо
		// «HTTP <код>», когда тело пусто)
		res.Response = res.ErrorMessage
		return res
	}

	if c.Stream {
		c.consumeStream(ctx, resp.Body, res, start)
	} else {
		c.consumeFull(ctx, resp.Body, res)
	}
	res.LatencyUS = usSince(start)
	return res
}

// classifyTransport — единая классификация транспортных сбоев: одинаковая
// сетевая причина даёт одинаковый kind и до заголовков, и при чтении тела
// ответа. Отмена → cancelled (штатная остановка не создаёт ложное состояние
// модели), таймаут → timeout, прочий разрыв (refused/reset/DNS) → connrefused.
func classifyTransport(ctx context.Context, err error) string {
	switch {
	case errors.Is(ctx.Err(), context.Canceled), errors.Is(err, context.Canceled):
		return "cancelled"
	case isTimeout(err):
		return "timeout"
	default:
		return "connrefused"
	}
}

func (c *Client) consumeStream(ctx context.Context, body io.Reader, res *RawResult, start time.Time) {
	var firstContentAt time.Time
	out := readStream(body, func() {
		if firstContentAt.IsZero() {
			firstContentAt = time.Now()
		}
	})
	if out.err != nil {
		res.ErrorKind, res.ErrorMessage = classifyTransport(ctx, out.err), errSummary(out.err)
		return
	}
	if out.usage != nil {
		if out.usage.PromptTokens != nil {
			res.InputTokens = ptr64(*out.usage.PromptTokens)
		}
		if out.usage.CompletionTokens != nil {
			res.OutputTokens = ptr64(*out.usage.CompletionTokens)
		}
	}
	res.FinishReason = out.finishReason
	if out.dataErr != "" && !out.generated {
		// Шлюз сообщил об ошибке прямо в SSE-данных: HTTP 200 без генерации —
		// «empty» (паритет с non-stream), исходный текст провайдера
		// сохраняется дословно.
		res.ErrorKind, res.ErrorMessage = "empty", out.dataErr
		res.Response = out.dataErr
		return
	}
	res.Content = out.content
	res.Response = out.content
	if !out.sawData || !out.parsedAny {
		if msg := bodyGatewayError(out.rawPrefix); msg != "" {
			// stream:true, но SSE-данных не было: шлюз отдал JSON с ошибкой —
			// это не «ошибка монитора» и не разрыв соединения
			res.ErrorKind, res.ErrorMessage = "empty", msg
			res.Response = msg
			return
		}
		res.ErrorKind, res.ErrorMessage = "invalid", "unparseable response body"
		// исходное тело, как оно есть, — единственный достоверный след
		res.Response = truncate(strings.TrimSpace(string(bytes.ToValidUTF8(out.rawPrefix, nil))), maxTextRunes)
		return
	}
	if !out.generated {
		// HTTP 200 with nothing generated. Reasoning tokens are counted as
		// generation above, so this is a truly empty reply from the API.
		res.ErrorKind, res.ErrorMessage = "empty", "empty generation"
		return
	}
	end := time.Now()
	ttft := firstContentAt.Sub(start).Microseconds()
	if ttft < 0 {
		ttft = 0
	}
	res.TTFTUS = ptr64(ttft)
	gen := end.Sub(firstContentAt).Microseconds()
	if gen < 0 {
		gen = 0
	}
	res.GenUS = ptr64(gen)
	// Буферизованный провайдер отдаёт весь ответ одним залпом: TPS по такому
	// интервалу был бы выдумкой, поэтому требуется минимальная длительность.
	if res.OutputTokens != nil && *res.OutputTokens > 0 && gen >= minGenerationUS {
		res.TPS = ptrF64(float64(*res.OutputTokens) / (float64(gen) / 1e6))
	}
}

func (c *Client) consumeFull(ctx context.Context, body io.Reader, res *RawResult) {
	raw, err := readLimited(body, maxBodyBytes)
	if err != nil {
		// чтение тела после заголовков классифицируется транспортной причиной,
		// а не «некорректным ответом» (1.2)
		res.ErrorKind, res.ErrorMessage = classifyTransport(ctx, err), errSummary(err)
		return
	}
	var parsed nonStreamResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		res.ErrorKind, res.ErrorMessage = "invalid", "unparseable response body"
		res.Response = truncate(strings.TrimSpace(string(bytes.ToValidUTF8(raw, nil))), maxTextRunes)
		return
	}
	if parsed.Usage != nil {
		if parsed.Usage.PromptTokens != nil {
			res.InputTokens = ptr64(*parsed.Usage.PromptTokens)
		}
		if parsed.Usage.CompletionTokens != nil {
			res.OutputTokens = ptr64(*parsed.Usage.CompletionTokens)
		}
	}
	content := ""
	generated := false
	selected := -1
	// Несколько choices: классифицируется та же choice, чей content фактически
	// используется. При отсутствии content выбирается первая содержательная
	// choice (reasoning-only), чтобы finish_reason относился к ней.
	for i := range parsed.Choices {
		msg := parsed.Choices[i].Message
		if msg.Content != "" {
			content = msg.Content
			selected = i
			generated = true
			break
		}
	}
	if selected == -1 {
		for i := range parsed.Choices {
			if parsed.Choices[i].Message.ReasoningContent != "" {
				selected = i
				generated = true
				break
			}
		}
	}
	if selected == -1 {
		// Полностью пустой ответ тоже может сообщать причину завершения
		// (например, length). Значение не выдумываем, а только переносим его.
		for i := range parsed.Choices {
			if parsed.Choices[i].FinishReason != nil && *parsed.Choices[i].FinishReason != "" {
				selected = i
				break
			}
		}
	}
	if selected >= 0 && parsed.Choices[selected].FinishReason != nil {
		res.FinishReason = *parsed.Choices[selected].FinishReason
	}
	// Итоговый ответ — message.content (reasoning им не является);
	// оригинальный ответ сохраняется дословно.
	res.Content = content
	res.Response = content
	if !generated {
		res.ErrorKind = "empty"
		// некоторые шлюзы возвращают 200 с JSON-ошибкой: её дословный текст
		// и есть диагностический ответ; без него quotes не будет вовсе
		if parsed.Error != nil && parsed.Error.Message != "" {
			res.ErrorMessage = truncate(parsed.Error.Message, maxTextRunes)
			res.Response = res.ErrorMessage
		} else {
			res.ErrorMessage = "empty generation"
		}
	}
	// Without streaming there is no first-token timing: TTFT/TPS stay N/A.
}

func usSince(start time.Time) int64 {
	d := time.Since(start).Microseconds()
	if d < 0 {
		return 0
	}
	return d
}

func isTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return ne.Timeout()
	}
	return false
}

func errSummary(err error) string {
	msg := err.Error()
	// обрезка по рунам: байтовый срез мог бы ломать многобайтовые символы
	// и отправлять в Telegram невалидный UTF-8
	if r := []rune(msg); len(r) > maxTextRunes {
		return string(r[:maxTextRunes])
	}
	return msg
}
