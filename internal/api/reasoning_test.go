package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Реальный ответ Tonworld: reasoning-модели шлют только reasoning_content
// при небольшом max_tokens. Это НЕ пустая генерация.
func TestGenerateReasoningOnlyIsSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		writeChunks(w,
			`{"choices":[{"index":0,"delta":{"content":"","role":"assistant","reasoning_content":""},"finish_reason":null}]}`,
			`{"choices":[{"index":0,"delta":{"reasoning_content":"We"},"finish_reason":null}]}`,
		)
		time.Sleep(180 * time.Millisecond)
		writeSSE(w, []string{
			`{"choices":[{"index":0,"delta":{"reasoning_content":" need answer"},"finish_reason":null}]}`,
			`{"choices":[{"index":0,"delta":{"content":""},"finish_reason":"stop"}]}`,
			`{"choices":[],"usage":{"prompt_tokens":35,"completion_tokens":8,"total_tokens":43}}`,
		})
	}))
	defer srv.Close()

	res := newTestClient(srv.URL, 5*time.Second, true).Generate(context.Background(), "deepseek-v4.1-flash", 8)
	if res.ErrorKind != "" {
		t.Fatalf("reasoning-only generation must be a success, got kind=%q msg=%q", res.ErrorKind, res.ErrorMessage)
	}
	if res.TTFTUS == nil || *res.TTFTUS <= 0 {
		t.Fatalf("TTFT must be measured from the first reasoning token, got %v", res.TTFTUS)
	}
	if res.GenUS == nil {
		t.Fatal("generation time must be measured")
	}
	if res.OutputTokens == nil || *res.OutputTokens != 8 {
		t.Fatalf("output tokens = %v", res.OutputTokens)
	}
	if res.TPS == nil {
		t.Fatal("TPS must be computed from usage")
	}
}

// Пустая генерация: ни content, ни reasoning, completion_tokens == 0.
func TestGenerateTrulyEmptyStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		writeSSE(w, []string{
			`{"choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}]}`,
			`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			`{"choices":[],"usage":{"prompt_tokens":5,"completion_tokens":0,"total_tokens":5}}`,
		})
	}))
	defer srv.Close()

	res := newTestClient(srv.URL, 5*time.Second, true).Generate(context.Background(), "glm-5.3", 8)
	if res.ErrorKind != "empty" {
		t.Fatalf("truly empty generation must be 'empty', got %q", res.ErrorKind)
	}
}

// 200 без usage и без текста — пустая генерация, не Operational.
func TestGenerateEmptyWithoutUsage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		writeSSE(w, []string{
			`{"choices":[{"index":0,"delta":{"content":""},"finish_reason":"stop"}]}`,
		})
	}))
	defer srv.Close()

	res := newTestClient(srv.URL, 5*time.Second, true).Generate(context.Background(), "glm-5.3", 8)
	if res.ErrorKind != "empty" {
		t.Fatalf("kind = %q, want empty", res.ErrorKind)
	}
}

// Не-stream ответ с reasoning непустой, content пуст — модель работает.
func TestGenerateReasoningNoStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"choices":[{"index":0,"message":{"role":"assistant","content":"","reasoning_content":"thinking"},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":3}}`)
	}))
	defer srv.Close()

	res := newTestClient(srv.URL, 5*time.Second, false).Generate(context.Background(), "glm-5.3", 8)
	if res.ErrorKind != "" {
		t.Fatalf("reasoning-only non-stream must succeed, got %q", res.ErrorKind)
	}
	if res.TTFTUS != nil || res.TPS != nil {
		t.Fatal("non-stream must keep TTFT/TPS unavailable")
	}
}
