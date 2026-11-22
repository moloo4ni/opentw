package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// A buffered provider sends all chunks at once: the generation time is
// nearly zero and TPS comes out absurd. Such a value is not trustworthy
// and must be N/A, not millions of tokens per second.
func TestGenerateBufferedStreamHasNoTPS(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		writeSSE(w, []string{
			`{"choices":[{"index":0,"delta":{"content":"O"},"finish_reason":null}]}`,
			`{"choices":[{"index":0,"delta":{"content":"K"},"finish_reason":null}]}`,
			`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			`{"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":64,"total_tokens":74}}`,
		})
	}))
	defer srv.Close()

	res := newTestClient(srv.URL, 5*time.Second, true).Generate(context.Background(), "glm-5.3", 64)
	if res.ErrorKind != "" {
		t.Fatalf("kind = %q", res.ErrorKind)
	}
	if res.TPS != nil {
		t.Fatalf("buffered stream cannot yield a trustworthy TPS, got %.1f", *res.TPS)
	}
	if res.OutputTokens == nil || *res.OutputTokens != 64 {
		t.Fatalf("usage tokens must still be recorded, got %v", res.OutputTokens)
	}
	if res.TTFTUS == nil {
		t.Fatal("TTFT must still be measured")
	}
}

// TTFT/TPS are trustworthy when the chunks are actually spaced in time.
func TestGeneratePacedStreamHasTPS(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"O\"},\"finish_reason\":null}]}\n\n")
		w.(http.Flusher).Flush()
		time.Sleep(220 * time.Millisecond)
		fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"K\"},\"finish_reason\":\"stop\"}]}\n\n")
		w.(http.Flusher).Flush()
		fmt.Fprint(w, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":40,\"total_tokens\":50}}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer srv.Close()

	res := newTestClient(srv.URL, 5*time.Second, true).Generate(context.Background(), "glm-5.3", 64)
	if res.TPS == nil {
		t.Fatal("paced stream must yield TPS")
	}
	if *res.TPS < 100 || *res.TPS > 300 {
		t.Fatalf("TPS = %.1f, want ~180 (40 tokens over ~220ms)", *res.TPS)
	}
}
