package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const testPrompt = "Reply with exactly OK."

func newTestClient(baseURL string, timeout time.Duration, stream bool) *Client {
	return &Client{
		BaseURL:    strings.TrimSuffix(baseURL, "/"),
		APIKey:     "test-key",
		HTTPClient: &http.Client{Timeout: timeout},
		Prompt:     testPrompt,
		Stream:     stream,
	}
}

// writeChunks writes SSE events without the [DONE] terminator.
func writeChunks(w http.ResponseWriter, events ...string) {
	for _, ev := range events {
		fmt.Fprintf(w, "data: %s\n\n", ev)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}
}

// writeSSE writes SSE events with a flush after each, so TTFT timing is observable.
func writeSSE(w http.ResponseWriter, events []string) {
	for _, ev := range events {
		fmt.Fprintf(w, "data: %s\n\n", ev)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}
	fmt.Fprint(w, "data: [DONE]\n\n")
}

func TestGenerateStreamSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("missing bearer auth")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		writeChunks(w,
			`{"id":"1","choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}]}`,
			`{"id":"1","choices":[{"index":0,"delta":{"content":"O"},"finish_reason":null}]}`,
		)
		time.Sleep(180 * time.Millisecond) // реальная генерация занимает время
		writeSSE(w, []string{
			`{"id":"1","choices":[{"index":0,"delta":{"content":"K"},"finish_reason":null}]}`,
			`{"id":"1","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":4,"total_tokens":14}}`,
			`{"id":"1","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":4,"total_tokens":14}}`,
		})
	}))
	defer srv.Close()

	c := newTestClient(srv.URL, 5*time.Second, true)
	res := c.Generate(context.Background(), "glm-5.3", 8)
	if res.HTTPStatus != 200 {
		t.Fatalf("http status = %d, kind=%q msg=%q", res.HTTPStatus, res.ErrorKind, res.ErrorMessage)
	}
	if res.ErrorKind != "" {
		t.Fatalf("unexpected error kind %q: %q", res.ErrorKind, res.ErrorMessage)
	}
	if !res.Streamed {
		t.Fatal("must be marked streamed")
	}
	if res.LatencyUS <= 0 {
		t.Fatalf("latency = %d", res.LatencyUS)
	}
	if res.TTFTUS == nil || *res.TTFTUS <= 0 {
		t.Fatalf("ttft = %v", res.TTFTUS)
	}
	if res.GenUS == nil || *res.GenUS <= 0 || *res.GenUS > res.LatencyUS {
		t.Fatalf("gen ms = %v latency = %d", res.GenUS, res.LatencyUS)
	}
	if res.OutputTokens == nil || *res.OutputTokens != 4 {
		t.Fatalf("output tokens = %v", res.OutputTokens)
	}
	if res.InputTokens == nil || *res.InputTokens != 10 {
		t.Fatalf("input tokens = %v", res.InputTokens)
	}
	if res.TPS == nil {
		t.Fatal("TPS must be computed from usage")
	}
	wantTPS := float64(4) / (float64(*res.GenUS) / 1e6)
	if *res.TPS < wantTPS*0.98 || *res.TPS > wantTPS*1.02 {
		t.Fatalf("TPS = %f, want ~%f", *res.TPS, wantTPS)
	}
}

func TestGenerateStreamNoUsage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		writeSSE(w, []string{
			`{"id":"1","choices":[{"index":0,"delta":{"content":"Hi"},"finish_reason":null}]}`,
			`{"id":"1","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
		})
	}))
	defer srv.Close()

	c := newTestClient(srv.URL, 5*time.Second, true)
	res := c.Generate(context.Background(), "glm-5.3", 8)
	if res.ErrorKind != "" {
		t.Fatalf("error kind = %q", res.ErrorKind)
	}
	if res.TPS != nil {
		t.Fatalf("TPS must be N/A without usage, got %f", *res.TPS)
	}
	if res.InputTokens != nil || res.OutputTokens != nil {
		t.Fatal("tokens must be nil without usage")
	}
	if res.TTFTUS == nil || *res.TTFTUS <= 0 {
		t.Fatal("TTFT must be measured on stream")
	}
}

func TestGenerateNoStreamSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Model    string           `json:"model"`
			Messages []map[string]any `json:"messages"`
			MaxTok   int              `json:"max_tokens"`
			Stream   bool             `json:"stream"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("bad request body: %v", err)
		}
		if req.Model != "glm-5.3" {
			t.Errorf("model = %q", req.Model)
		}
		if req.MaxTok != 8 {
			t.Errorf("max_tokens = %d", req.MaxTok)
		}
		if req.Stream {
			t.Errorf("stream must be false in request")
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":"1","object":"chat.completion","model":"glm-5.3","choices":[{"index":0,"message":{"role":"assistant","content":"OK"},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":2,"total_tokens":9}}`)
	}))
	defer srv.Close()

	c := newTestClient(srv.URL, 5*time.Second, false)
	res := c.Generate(context.Background(), "glm-5.3", 8)
	if res.ErrorKind != "" || res.HTTPStatus != 200 {
		t.Fatalf("kind=%q status=%d", res.ErrorKind, res.HTTPStatus)
	}
	if res.Streamed {
		t.Fatal("must not be marked streamed")
	}
	if res.LatencyUS <= 0 {
		t.Fatal("latency must be measured")
	}
	if res.TTFTUS != nil || res.GenUS != nil {
		t.Fatal("TTFT/generation time must be N/A without streaming")
	}
	if res.TPS != nil {
		t.Fatal("TPS must be N/A without streaming (no first-token timing)")
	}
	if res.InputTokens == nil || *res.InputTokens != 7 || res.OutputTokens == nil || *res.OutputTokens != 2 {
		t.Fatalf("tokens in=%v out=%v", res.InputTokens, res.OutputTokens)
	}
}

func TestGenerateStreamRequestShape(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("path = %q", r.URL.Path)
		}
		if r.Header.Get("Accept") != "text/event-stream" {
			t.Errorf("accept = %q", r.Header.Get("Accept"))
		}
		var req struct {
			Stream     bool           `json:"stream"`
			StreamOpts map[string]any `json:"stream_options"`
			Messages   []struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode: %v", err)
		}
		if !req.Stream {
			t.Error("stream=true expected")
		}
		if v, ok := req.StreamOpts["include_usage"].(bool); !ok || !v {
			t.Errorf("stream_options.include_usage expected, got %v", req.StreamOpts)
		}
		if len(req.Messages) != 1 || req.Messages[0].Role != "user" || req.Messages[0].Content != testPrompt {
			t.Errorf("messages = %+v", req.Messages)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		writeSSE(w, []string{`{"choices":[{"delta":{"content":"OK"},"finish_reason":"stop"}]}`})
	}))
	defer srv.Close()

	c := newTestClient(srv.URL, 5*time.Second, true)
	if res := c.Generate(context.Background(), "glm-5.3", 8); res.ErrorKind != "" {
		t.Fatalf("kind=%q msg=%q", res.ErrorKind, res.ErrorMessage)
	}
}

func TestGenerateHTTP429(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprint(w, `{"error":{"message":"Upstream rate limit","type":"rate_limit"}}`)
	}))
	defer srv.Close()

	res := newTestClient(srv.URL, 5*time.Second, true).Generate(context.Background(), "gpt-6-astra", 8)
	if res.HTTPStatus != 429 {
		t.Fatalf("status = %d", res.HTTPStatus)
	}
	if res.ErrorKind != "http" {
		t.Fatalf("kind = %q", res.ErrorKind)
	}
	if res.ErrorMessage != "Upstream rate limit" {
		t.Fatalf("message = %q", res.ErrorMessage)
	}
	if res.LatencyUS <= 0 {
		t.Fatal("latency of failed request must be recorded")
	}
}

func TestGenerateHTTP5xx(t *testing.T) {
	for _, tc := range []struct {
		code int
		body string
	}{{503, `{"error":{"message":"Service Unavailable"}}`}, {500, `{"error":{"message":"Internal Server Error"}}`}} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(tc.code)
			fmt.Fprint(w, tc.body)
		}))
		res := newTestClient(srv.URL, 5*time.Second, true).Generate(context.Background(), "glm-5.3", 8)
		if res.HTTPStatus != tc.code || res.ErrorKind != "http" {
			t.Fatalf("status=%d kind=%q", res.HTTPStatus, res.ErrorKind)
		}
		if res.ErrorMessage == "" {
			t.Fatalf("provider message must be preserved for %d", tc.code)
		}
		srv.Close()
	}
}

func TestGenerateHTTP5xxNoBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(502)
	}))
	defer srv.Close()
	res := newTestClient(srv.URL, 5*time.Second, true).Generate(context.Background(), "glm-5.3", 8)
	if res.HTTPStatus != 502 || res.ErrorKind != "http" {
		t.Fatalf("status=%d kind=%q", res.HTTPStatus, res.ErrorKind)
	}
	if res.ErrorMessage != "HTTP 502" {
		t.Fatalf("message = %q", res.ErrorMessage)
	}
}

func TestGenerateTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
	}))
	defer srv.Close()
	start := time.Now()
	res := newTestClient(srv.URL, 300*time.Millisecond, true).Generate(context.Background(), "glm-5.3", 8)
	if res.ErrorKind != "timeout" {
		t.Fatalf("kind = %q", res.ErrorKind)
	}
	if res.HTTPStatus != 0 {
		t.Fatalf("status = %d", res.HTTPStatus)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("timeout took too long: %v", elapsed)
	}
	if res.LatencyUS < 250_000 {
		t.Fatalf("latency = %d", res.LatencyUS)
	}
}

func TestGenerateConnRefused(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close() // порт закрыт

	res := newTestClient(srv.URL, 2*time.Second, true).Generate(context.Background(), "glm-5.3", 8)
	if res.ErrorKind != "connrefused" {
		t.Fatalf("kind = %q, msg = %q", res.ErrorKind, res.ErrorMessage)
	}
}

func TestGenerateModelNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
		fmt.Fprint(w, `{"error":{"message":"model not found: nope","code":"model_not_found"}}`)
	}))
	defer srv.Close()

	res := newTestClient(srv.URL, 5*time.Second, true).Generate(context.Background(), "nope", 8)
	if res.HTTPStatus != 404 || res.ErrorKind != "http" {
		t.Fatalf("status=%d kind=%q", res.HTTPStatus, res.ErrorKind)
	}
	if !strings.Contains(res.ErrorMessage, "model not found") {
		t.Fatalf("message = %q", res.ErrorMessage)
	}
}

func TestGenerateAuthError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(401)
		fmt.Fprint(w, `{"error":{"message":"Invalid API key"}}`)
	}))
	defer srv.Close()

	res := newTestClient(srv.URL, 5*time.Second, true).Generate(context.Background(), "glm-5.3", 8)
	if res.HTTPStatus != 401 || res.ErrorKind != "http" {
		t.Fatalf("status=%d kind=%q", res.HTTPStatus, res.ErrorKind)
	}
	if res.ErrorMessage != "Invalid API key" {
		t.Fatalf("message = %q", res.ErrorMessage)
	}
}

func TestGenerateInvalidResponse(t *testing.T) {
	for _, stream := range []bool{true, false} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if stream {
				w.Header().Set("Content-Type", "text/event-stream")
			} else {
				w.Header().Set("Content-Type", "application/json")
			}
			fmt.Fprint(w, "wow") // заглушка Tonworld без ключа
		}))
		res := newTestClient(srv.URL, 5*time.Second, stream).Generate(context.Background(), "glm-5.3", 8)
		if res.ErrorKind != "invalid" {
			t.Fatalf("stream=%v kind = %q, msg = %q", stream, res.ErrorKind, res.ErrorMessage)
		}
		srv.Close()
	}
}

func TestGenerateEmptyStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		writeSSE(w, []string{
			`{"id":"1","choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}]}`,
			`{"id":"1","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			`{"id":"1","choices":[],"usage":{"prompt_tokens":5,"completion_tokens":0,"total_tokens":5}}`,
		})
	}))
	defer srv.Close()

	res := newTestClient(srv.URL, 5*time.Second, true).Generate(context.Background(), "glm-5.3", 8)
	if res.HTTPStatus != 200 || res.ErrorKind != "empty" {
		t.Fatalf("status=%d kind=%q", res.HTTPStatus, res.ErrorKind)
	}
	if res.TTFTUS != nil {
		t.Fatal("TTFT must be N/A when nothing was generated")
	}
}

func TestGenerateEmptyNoStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"id":"1","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":""},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":0}}`)
	}))
	defer srv.Close()

	res := newTestClient(srv.URL, 5*time.Second, false).Generate(context.Background(), "glm-5.3", 8)
	if res.ErrorKind != "empty" {
		t.Fatalf("kind = %q", res.ErrorKind)
	}
}

func TestListModels(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			t.Errorf("path = %q", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("missing bearer")
		}
		fmt.Fprint(w, `{"object":"list","data":[{"id":"glm-5.3","object":"model"},{"id":"kimi-k3"}]}`)
	}))
	defer srv.Close()

	ids, err := newTestClient(srv.URL, 5*time.Second, true).ListModels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != "glm-5.3" || ids[1] != "kimi-k3" {
		t.Fatalf("ids = %v", ids)
	}
}

func TestListModelsErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(401)
		fmt.Fprint(w, `{"error":{"message":"Invalid API key"}}`)
	}))
	defer srv.Close()
	if _, err := newTestClient(srv.URL, 5*time.Second, true).ListModels(context.Background()); err == nil {
		t.Fatal("expected error on 401")
	}

	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "wow")
	}))
	defer srv2.Close()
	if _, err := newTestClient(srv2.URL, 5*time.Second, true).ListModels(context.Background()); err == nil {
		t.Fatal("expected error on non-JSON body")
	}
}
