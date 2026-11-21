package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Reasoning-only stream with a token limit: reasoning is not an answer, but
// the terminal reason and usage must remain available for diagnostics.
func TestGenerateReasoningOnlyStreamPreservesFinishReason(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		writeSSE(w, []string{
			`{"choices":[{"index":0,"delta":{"reasoning_content":"I need to answer briefly"},"finish_reason":null}]}`,
			`{"choices":[{"index":0,"delta":{"content":""},"finish_reason":"length"}]}`,
			`{"choices":[],"usage":{"prompt_tokens":32,"completion_tokens":64,"total_tokens":96}}`,
		})
	}))
	defer srv.Close()

	res := newTestClient(srv.URL, 5*time.Second, true).Generate(context.Background(), "glm-5.3-prime", 64)
	if res.ErrorKind != "" {
		t.Fatalf("reasoning-only generation has transport/generation kind %q, want empty", res.ErrorKind)
	}
	if res.Content != "" || res.Response != "" {
		t.Fatalf("reasoning must not become the user answer: content=%q response=%q", res.Content, res.Response)
	}
	if res.OutputTokens == nil || *res.OutputTokens != 64 {
		t.Fatalf("output tokens = %v, want 64", res.OutputTokens)
	}
	if res.FinishReason != "length" {
		t.Fatalf("finish reason = %q, want length", res.FinishReason)
	}
}

func TestGenerateStreamFinishReason(t *testing.T) {
	cases := []struct {
		name   string
		chunks []string
		want   string
	}{
		{
			name: "length",
			chunks: []string{
				`{"choices":[{"index":0,"delta":{"reasoning_content":"thinking"},"finish_reason":null}]}`,
				`{"choices":[{"index":0,"delta":{},"finish_reason":"length"}]}`,
				`{"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":64}}`,
			},
			want: "length",
		},
		{
			name: "stop",
			chunks: []string{
				`{"choices":[{"index":0,"delta":{"content":"OK"},"finish_reason":null}]}`,
				`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			},
			want: "stop",
		},
		{
			name: "null is absent",
			chunks: []string{
				`{"choices":[{"index":0,"delta":{"content":"OK"},"finish_reason":null}]}`,
			},
			want: "",
		},
		{
			name: "field omitted",
			chunks: []string{
				`{"choices":[{"index":0,"delta":{"content":"OK"}}]}`,
			},
			want: "",
		},
		{
			name: "usage-only after terminal cannot erase reason",
			chunks: []string{
				`{"choices":[{"index":0,"delta":{"content":"OK"},"finish_reason":"stop"}]}`,
				`{"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":2}}`,
				`{"choices":[],"finish_reason":null}`,
			},
			want: "stop",
		},
		{
			name: "selected choice supplies content and reason",
			chunks: []string{
				`{"choices":[{"index":0,"delta":{"reasoning_content":"choice zero reasoning"},"finish_reason":null},{"index":1,"delta":{"content":"OK"},"finish_reason":null}]}`,
				`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"},{"index":1,"delta":{},"finish_reason":"length"}]}`,
			},
			want: "length",
		},
		{
			name: "length without any generated text",
			chunks: []string{
				`{"choices":[{"index":0,"delta":{"content":""},"finish_reason":"length"}]}`,
				`{"choices":[],"usage":{"prompt_tokens":5,"completion_tokens":64}}`,
			},
			want: "length",
		},
		{
			name: "content choice wins over earlier terminal reason from another choice",
			chunks: []string{
				`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"},{"index":1,"delta":{"content":"OK"},"finish_reason":null}]}`,
				`{"choices":[{"index":1,"delta":{},"finish_reason":"length"}]}`,
			},
			want: "length",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				writeSSE(w, tc.chunks)
			}))
			defer srv.Close()

			res := newTestClient(srv.URL, 5*time.Second, true).Generate(context.Background(), "glm-5.3", 8)
			if res.FinishReason != tc.want {
				t.Fatalf("finish reason = %q, want %q", res.FinishReason, tc.want)
			}
			if tc.name == "selected choice supplies content and reason" && (res.Content != "OK" || res.Response != "OK") {
				t.Fatalf("content/response = %q/%q, want OK/OK from the selected choice", res.Content, res.Response)
			}
		})
	}
}

func TestGenerateNonStreamFinishReason(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{
			name: "stop",
			body: `{"choices":[{"index":0,"message":{"role":"assistant","content":"OK"},"finish_reason":"stop"}]}`,
			want: "stop",
		},
		{
			name: "null",
			body: `{"choices":[{"index":0,"message":{"role":"assistant","content":"OK"},"finish_reason":null}]}`,
			want: "",
		},
		{
			name: "field omitted",
			body: `{"choices":[{"index":0,"message":{"role":"assistant","content":"OK"}}]}`,
			want: "",
		},
		{
			name: "same choice as content",
			body: `{"choices":[{"index":0,"message":{"role":"assistant","content":"","reasoning_content":"thinking"},"finish_reason":"stop"},{"index":1,"message":{"role":"assistant","content":"OK"},"finish_reason":"length"}]}`,
			want: "length",
		},
		{
			name: "length without generated text",
			body: `{"choices":[{"index":0,"message":{"role":"assistant","content":"","reasoning_content":""},"finish_reason":"length"}]}`,
			want: "length",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, tc.body)
			}))
			defer srv.Close()

			res := newTestClient(srv.URL, 5*time.Second, false).Generate(context.Background(), "glm-5.3", 8)
			if res.FinishReason != tc.want {
				t.Fatalf("finish reason = %q, want %q", res.FinishReason, tc.want)
			}
			if tc.name == "same choice as content" && res.Content != "OK" {
				t.Fatalf("content = %q, want the nonempty choice", res.Content)
			}
		})
	}
}

// Separate reasoning_content must never replace the user-facing content.
func TestGenerateNonStreamSeparateReasoningDoesNotReplaceContent(t *testing.T) {
	cases := []struct {
		name    string
		reason  string
		content string
		want    string
	}{
		{"reasoning and OK", "Let me check the instruction", "OK", "OK"},
		{"reasoning with empty final content", "Let me check the instruction", "", ""},
		{"reasoning-like final content", "", "Let me think through this carefully...", "Let me think through this carefully..."},
		{"reasoning marker in final content", "internal", "\U0001F9E0...OK", "\U0001F9E0...OK"},
		{"OK plus surrounding text", "reason", "reasoning + answer", "reasoning + answer"},
		{"whitespace around OK", "reason", " OK ", " OK "},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintf(w, `{"choices":[{"index":0,"message":{"role":"assistant","content":%q,"reasoning_content":%q},"finish_reason":"stop"}]}`,
					tc.content, tc.reason)
			}))
			defer srv.Close()

			res := newTestClient(srv.URL, 5*time.Second, false).Generate(context.Background(), "kimi-k3", 8)
			if res.Content != tc.want || res.Response != tc.want {
				t.Fatalf("content/response = %q/%q, want %q", res.Content, res.Response, tc.want)
			}
		})
	}
}
