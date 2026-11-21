package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Реальный ответ Tonworld: reasoning-модели шлют только reasoning_content
// при небольшом max_tokens. Это НЕ пустая генерация (kind ""), но итогового
// ответа нет: Content пуст — классификация даст Down.
func TestGenerateReasoningOnlyStreamNoContent(t *testing.T) {
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
		t.Fatalf("reasoning-only generation must keep kind %q, got kind=%q msg=%q", "", res.ErrorKind, res.ErrorMessage)
	}
	if res.Content != "" || res.Response != "" {
		t.Fatalf("reasoning is not the answer: content=%q response=%q", res.Content, res.Response)
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

// reasoning никогда не попадает в Content: итоговый ответ — только
// конкатенация delta.content (даже когда reasoning осмыслен).
func TestGenerateStreamContentExcludesReasoning(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		writeSSE(w, []string{
			`{"choices":[{"index":0,"delta":{"reasoning_content":"OK"},"finish_reason":null}]}`,
			`{"choices":[{"index":0,"delta":{"reasoning_content":"thinking garbage"},"finish_reason":null}]}`,
			`{"choices":[{"index":0,"delta":{"content":"so"},"finish_reason":null}]}`,
			`{"choices":[{"index":0,"delta":{"content":"rry"},"finish_reason":"stop"}]}`,
		})
	}))
	defer srv.Close()

	res := newTestClient(srv.URL, 5*time.Second, true).Generate(context.Background(), "glm-5.3", 8)
	if res.ErrorKind != "" {
		t.Fatalf("kind = %q, msg = %q", res.ErrorKind, res.ErrorMessage)
	}
	if res.Content != "sorry" || res.Response != "sorry" {
		t.Fatalf("content must be delta.content only (reasoning excluded): %q", res.Content)
	}
}

// reasoning-мусор + итоговый «OK» (стрим): Content — ровно ответ модели.
func TestGenerateStreamReasoningGarbageFinalOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		writeSSE(w, []string{
			`{"choices":[{"index":0,"delta":{"reasoning_content":"用户要求我只回复OK"},"finish_reason":null}]}`,
			`{"choices":[{"index":0,"delta":{"content":"OK"},"finish_reason":"stop"}]}`,
		})
	}))
	defer srv.Close()

	res := newTestClient(srv.URL, 5*time.Second, true).Generate(context.Background(), "glm-5.3", 8)
	if res.ErrorKind != "" {
		t.Fatalf("kind = %q, msg = %q", res.ErrorKind, res.ErrorMessage)
	}
	if res.Content != "OK" || res.Response != "OK" {
		t.Fatalf("final answer must be exactly the content: %q", res.Content)
	}
}

// reasoning «OK» + итоговый текст об ошибке (без стрима): Content —
// message.content, reasoning не считается ответом.
func TestGenerateNoStreamReasoningOKFinalErrorText(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"choices":[{"index":0,"message":{"role":"assistant","content":"Извините, я не могу выполнить этот запрос.","reasoning_content":"OK"},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":3}}`)
	}))
	defer srv.Close()

	res := newTestClient(srv.URL, 5*time.Second, false).Generate(context.Background(), "glm-5.3", 8)
	if res.ErrorKind != "" {
		t.Fatalf("kind = %q, msg = %q", res.ErrorKind, res.ErrorMessage)
	}
	if res.Content != "Извините, я не могу выполнить этот запрос." || res.Response != res.Content {
		t.Fatalf("content must be message.content only: %q", res.Content)
	}
}

// Патологически длинный стрим: накопленный content обрезан по maxTextRunes
// рун с видимым «…» (appendCapped).
func TestGenerateStreamContentCapped(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		for i := 0; i < 5; i++ {
			fmt.Fprintf(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"%s\"},\"finish_reason\":null}]}\n\n", strings.Repeat("a", 10_000))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer srv.Close()

	res := newTestClient(srv.URL, 5*time.Second, true).Generate(context.Background(), "glm-5.3", 64)
	if res.ErrorKind != "" {
		t.Fatalf("kind = %q, msg = %q", res.ErrorKind, res.ErrorMessage)
	}
	if n := len([]rune(res.Content)); n != maxTextRunes {
		t.Fatalf("capped content rune count = %d, want %d", n, maxTextRunes)
	}
	if res.Content != strings.Repeat("a", maxTextRunes) {
		t.Fatal("content must be the capped prefix of the stream")
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

// Не-stream ответ с reasoning непустой, content пуст: генерация была
// (kind ""), итогового ответа нет — Content пуст, классификация даст Down.
func TestGenerateReasoningNoStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"choices":[{"index":0,"message":{"role":"assistant","content":"","reasoning_content":"thinking"},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":3}}`)
	}))
	defer srv.Close()

	res := newTestClient(srv.URL, 5*time.Second, false).Generate(context.Background(), "glm-5.3", 8)
	if res.ErrorKind != "" {
		t.Fatalf("reasoning-only non-stream must keep kind empty, got %q", res.ErrorKind)
	}
	if res.Content != "" || res.Response != "" {
		t.Fatalf("reasoning is not the answer: content=%q response=%q", res.Content, res.Response)
	}
	if res.TTFTUS != nil || res.TPS != nil {
		t.Fatal("non-stream must keep TTFT/TPS unavailable")
	}
}
