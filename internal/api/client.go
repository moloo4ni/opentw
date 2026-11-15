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
		switch {
		case errors.Is(ctx.Err(), context.Canceled):
			res.ErrorKind, res.ErrorMessage = "cancelled", ctx.Err().Error()
		case isTimeout(err):
			res.ErrorKind, res.ErrorMessage = "timeout", errSummary(err)
		default:
			// Any other transport failure (refused, reset, DNS) is a
			// connection problem from the monitor's point of view.
			res.ErrorKind, res.ErrorMessage = "connrefused", errSummary(err)
		}
		return res
	}
	defer resp.Body.Close()
	res.HTTPStatus = resp.StatusCode

	if resp.StatusCode != http.StatusOK {
		errBody, _ := readLimited(resp.Body, 8192)
		res.LatencyUS = usSince(start)
		res.ErrorKind = "http"
		res.ErrorMessage = extractProviderError(errBody, resp.StatusCode)
		return res
	}

	if c.Stream {
		c.consumeStream(resp.Body, res, start)
	} else {
		c.consumeFull(resp.Body, res)
	}
	res.LatencyUS = usSince(start)
	return res
}

func (c *Client) consumeStream(body io.Reader, res *RawResult, start time.Time) {
	var firstContentAt time.Time
	_, u, sawData, parsedAny, generated, err := readStream(body, func() {
		if firstContentAt.IsZero() {
			firstContentAt = time.Now()
		}
	})
	if err != nil {
		res.ErrorKind, res.ErrorMessage = "invalid", errSummary(err)
		return
	}
	if u != nil {
		if u.PromptTokens != nil {
			res.InputTokens = ptr64(*u.PromptTokens)
		}
		if u.CompletionTokens != nil {
			res.OutputTokens = ptr64(*u.CompletionTokens)
		}
	}
	if !sawData || !parsedAny {
		res.ErrorKind, res.ErrorMessage = "invalid", "unparseable response body"
		return
	}
	if !generated {
		// HTTP 200 with nothing generated — Degraded, not Down. Reasoning
		// tokens already counted as generation above.
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

func (c *Client) consumeFull(body io.Reader, res *RawResult) {
	raw, err := readLimited(body, 8192)
	if err != nil {
		res.ErrorKind, res.ErrorMessage = "invalid", errSummary(err)
		return
	}
	var parsed nonStreamResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		res.ErrorKind, res.ErrorMessage = "invalid", "unparseable response body"
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
	generated := false
	if len(parsed.Choices) > 0 {
		msg := parsed.Choices[0].Message
		generated = msg.Content != "" || msg.ReasoningContent != ""
	}
	if !generated {
		res.ErrorKind, res.ErrorMessage = "empty", "empty generation"
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
	if len(msg) > 512 {
		msg = msg[:512]
	}
	return msg
}
