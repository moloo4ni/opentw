package api

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"strings"
)

// streamChunk is one SSE data event of a chat completion stream.
type streamChunk struct {
	Choices []struct {
		Index int `json:"index"`
		Delta struct {
			Role             string `json:"role"`
			Content          string `json:"content"`
			ReasoningContent string `json:"reasoning_content"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *usage `json:"usage"`
}

type usage struct {
	PromptTokens     *int64 `json:"prompt_tokens"`
	CompletionTokens *int64 `json:"completion_tokens"`
	TotalTokens      *int64 `json:"total_tokens"`
}

// readStream consumes an SSE body, recording the moment of the first
// generated token and accumulating visible text and usage. Reasoning tokens
// count as generation: reasoning models write their budget there first, and
// a visible answer may never arrive at small max_tokens.
// Individual malformed chunks are skipped: providers send keep-alive and odd
// extensions; usable state is decided by the caller from the fields.
func readStream(r io.Reader, onFirstContent func()) (text strings.Builder, u *usage, sawData, parsedAny, generated bool, err error) {
	br := bufio.NewReaderSize(r, 64*1024)
	for {
		line, readErr := br.ReadString('\n')
		trimmed := strings.TrimRight(line, "\r\n")
		if strings.HasPrefix(trimmed, ":") { // SSE comment / keep-alive
			if readErr != nil {
				break
			}
			continue
		}
		if after, ok := strings.CutPrefix(trimmed, "data:"); ok {
			payload := strings.TrimSpace(after)
			if payload == "" {
				if readErr != nil {
					break
				}
				continue
			}
			sawData = true
			if payload == "[DONE]" {
				break
			}
			var chunk streamChunk
			if json.Unmarshal([]byte(payload), &chunk) == nil {
				parsedAny = true
				if chunk.Usage != nil {
					u = chunk.Usage
				}
				for _, ch := range chunk.Choices {
					if !generated && (ch.Delta.Content != "" || ch.Delta.ReasoningContent != "") {
						generated = true
						if onFirstContent != nil {
							onFirstContent()
						}
					}
					if ch.Delta.Content != "" {
						text.WriteString(ch.Delta.Content)
					}
				}
			}
		}
		if readErr != nil {
			if readErr != io.EOF {
				err = readErr
			}
			break
		}
	}
	return text, u, sawData, parsedAny, generated, err
}

// nonStreamResponse is a full chat completion object.
type nonStreamResponse struct {
	Choices []struct {
		Message struct {
			Role             string `json:"role"`
			Content          string `json:"content"`
			ReasoningContent string `json:"reasoning_content"`
		} `json:"message"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *usage `json:"usage"`
	Error *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
		Code    string `json:"code"`
	} `json:"error"`
}

// extractProviderError pulls the human-readable provider message out of an
// error body, verbatim; falls back to the raw body, then to "HTTP <code>".
func extractProviderError(body []byte, code int) string {
	var v struct {
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
		Message string `json:"message"`
	}
	if json.Unmarshal(body, &v) == nil {
		if v.Error != nil && v.Error.Message != "" {
			return v.Error.Message
		}
		if v.Message != "" {
			return v.Message
		}
	}
	trimmed := strings.TrimSpace(string(bytes.ToValidUTF8(body, nil)))
	if trimmed == "" {
		return "HTTP " + itoa(code)
	}
	return truncate(trimmed, 512)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
