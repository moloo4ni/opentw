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

// minGenerationUS is the shortest generation time TPS is trustworthy for
// (a fully buffered SSE stream yields ~0).
const minGenerationUS = 100_000

// maxTextRunes caps stored response/error text: far above real replies
// (max_tokens bounds generation), but keeps pathological error pages out of
// storage. Truncation is visible ("…"); raw bytes are bounded by maxBodyBytes.
const maxTextRunes = 16384

// maxBodyBytes is how many response body bytes are read and kept.
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
		// Diagnostic response, verbatim: provider text or "HTTP <code>".
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

// classifyTransport maps transport failures to one error kind, both before
// headers and while reading the body. Cancelled → "cancelled" (a normal
// shutdown must not fake a model state), timeout → "timeout", other
// disconnects (refused/reset/DNS) → "connrefused".
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
		// Gateway error inside SSE data: HTTP 200 without generation is
		// "empty" (parity with non-stream); the provider text is kept verbatim.
		res.ErrorKind, res.ErrorMessage = "empty", out.dataErr
		res.Response = out.dataErr
		return
	}
	res.Content = out.content
	res.Response = out.content
	if !out.sawData || !out.parsedAny {
		if msg := bodyGatewayError(out.rawPrefix); msg != "" {
			// stream:true with no SSE data: the gateway returned a JSON error,
			// not a monitor failure or a disconnect.
			res.ErrorKind, res.ErrorMessage = "empty", msg
			res.Response = msg
			return
		}
		res.ErrorKind, res.ErrorMessage = "invalid", "unparseable response body"
		// The raw body is the only reliable trace.
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
	// A buffered provider sends the whole reply at once; TPS over such an
	// interval would be invented, hence the minimum duration.
	if res.OutputTokens != nil && *res.OutputTokens > 0 && gen >= minGenerationUS {
		res.TPS = ptrF64(float64(*res.OutputTokens) / (float64(gen) / 1e6))
	}
}

func (c *Client) consumeFull(ctx context.Context, body io.Reader, res *RawResult) {
	raw, err := readLimited(body, maxBodyBytes)
	if err != nil {
		// A body-read failure after headers is a transport failure, not an
		// invalid response.
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
	// With several choices, classify the choice whose content is used;
	// without content pick the first choice with reasoning so its
	// finish_reason is the one carried.
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
		// A fully empty response may still report a finish reason (e.g.
		// length); pass it through as-is.
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
	// The final answer is message.content (never reasoning); kept verbatim.
	res.Content = content
	res.Response = content
	if !generated {
		res.ErrorKind = "empty"
		// Some gateways return 200 with a JSON error: its verbatim text is
		// the diagnostic answer.
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
	// Truncate by runes: a byte slice could split multibyte characters and
	// send invalid UTF-8 to Telegram.
	if r := []rune(msg); len(r) > maxTextRunes {
		return string(r[:maxTextRunes])
	}
	return msg
}
