package api

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"
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
	// Error is a gateway error delivered inside SSE data (HTTP 200),
	// distinct from a transport failure or an empty generation.
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

type usage struct {
	PromptTokens     *int64 `json:"prompt_tokens"`
	CompletionTokens *int64 `json:"completion_tokens"`
	TotalTokens      *int64 `json:"total_tokens"`
}

// readStream consumes an SSE body, recording the moment of the first
// generated token and the final usage. Reasoning tokens
// count as generation: reasoning models write their budget there first, and
// a visible answer may never arrive at small max_tokens.
// Individual malformed chunks are skipped: providers send keep-alive and odd
// extensions; usable state is decided by the caller from the fields.

// streamOutcome is the result of reading an SSE stream.
type streamOutcome struct {
	usage     *usage
	sawData   bool
	parsedAny bool
	generated bool
	// content is the accumulated final answer (delta.content only; reasoning
	// is never an answer), capped at maxTextRunes with a visible "…"
	// (contentFull).
	content     string
	contentFull bool
	// finishReason is the first non-empty finish_reason of the terminal chunk
	// of the same choice the answer was accumulated from. Usage-only chunks
	// cannot clear it; a missing value stays empty.
	finishReason string
	// finishReasons holds non-empty finish_reasons by choice index; the final
	// value is taken after the stream from the choice that produced content.
	finishReasons     map[int]string
	firstFinishChoice int
	hasFinishChoice   bool
	// selectedChoice is the choice content is taken from (or the first
	// choice with any content when there is none), so multi-choice responses
	// carry the right finish_reason.
	selectedChoice   int
	hasChoice        bool
	hasContentChoice bool
	// dataErr is the error field text from SSE data (first non-empty message).
	dataErr string
	// rawPrefix is what was read before the first "data:" (≤ 8KB): with
	// stream:true a gateway may answer with plain JSON instead of SSE.
	rawPrefix []byte
	err       error
}

func readStream(r io.Reader, onFirstContent func()) streamOutcome {
	var out streamOutcome
	var raw bytes.Buffer
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
			out.sawData = true
			if payload == "[DONE]" {
				break
			}
			var chunk streamChunk
			if json.Unmarshal([]byte(payload), &chunk) == nil {
				out.parsedAny = true
				if chunk.Error != nil && chunk.Error.Message != "" && out.dataErr == "" {
					out.dataErr = chunk.Error.Message
				}
				if chunk.Usage != nil {
					out.usage = chunk.Usage
				}
				for _, ch := range chunk.Choices {
					// Store reasons by choice index first: the source of the
					// final answer may be picked later, so a global reason
					// assignment before the end of the stream is not possible.
					if ch.FinishReason != nil && *ch.FinishReason != "" {
						if out.finishReasons == nil {
							out.finishReasons = make(map[int]string)
						}
						if _, exists := out.finishReasons[ch.Index]; !exists {
							out.finishReasons[ch.Index] = *ch.FinishReason
						}
						if !out.hasFinishChoice {
							out.firstFinishChoice = ch.Index
							out.hasFinishChoice = true
						}
					}
					if ch.Delta.Content != "" {
						// The first non-empty content is the source of the final answer.
						if !out.hasContentChoice {
							out.hasContentChoice = true
							out.selectedChoice = ch.Index
							out.hasChoice = true
							out.content = ""
						}
						if out.selectedChoice != ch.Index {
							continue
						}
						if !out.generated {
							out.generated = true
							if onFirstContent != nil {
								onFirstContent()
							}
						}
						if !out.contentFull {
							out.content, out.contentFull = appendCapped(out.content, ch.Delta.Content)
						}
						continue
					}
					if ch.Delta.ReasoningContent != "" {
						if !out.hasChoice && !out.hasContentChoice {
							out.selectedChoice = ch.Index
							out.hasChoice = true
						}
						if out.selectedChoice != ch.Index {
							continue
						}
						if !out.generated {
							out.generated = true
							if onFirstContent != nil {
								onFirstContent()
							}
						}
					}
				}
			}
		} else if !out.sawData && raw.Len() < 8192 {
			// A non-data line before the first SSE event: keep it — it may be
			// a multi-line JSON error body from the gateway.
			raw.WriteString(line)
		}
		if readErr != nil {
			if readErr != io.EOF {
				out.err = readErr
			}
			break
		}
	}
	if out.hasChoice {
		if reason, ok := out.finishReasons[out.selectedChoice]; ok {
			out.finishReason = reason
		}
	} else if out.hasFinishChoice {
		// Neither content nor reasoning: take the first actual finish_reason.
		out.finishReason = out.finishReasons[out.firstFinishChoice]
	}
	out.rawPrefix = raw.Bytes()
	return out
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
			return truncate(v.Error.Message, maxTextRunes)
		}
		if v.Message != "" {
			return truncate(v.Message, maxTextRunes)
		}
	}
	trimmed := strings.TrimSpace(string(bytes.ToValidUTF8(body, nil)))
	if trimmed == "" {
		return "HTTP " + strconv.Itoa(code)
	}
	return truncate(trimmed, maxTextRunes)
}

// bodyGatewayError extracts the gateway error text from a non-SSE body
// with stream:true (not a single data: event). "" when it is not a JSON error.
func bodyGatewayError(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	var parsed nonStreamResponse
	if json.Unmarshal(raw, &parsed) != nil {
		return ""
	}
	if parsed.Error != nil && parsed.Error.Message != "" {
		return truncate(parsed.Error.Message, maxTextRunes)
	}
	return ""
}

// truncate cuts by runes: a byte slice would split multibyte characters.
func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

// appendCapped appends s to dst with a maxTextRunes rune limit: an endless
// stream must not inflate memory and storage; truncation is visible ("…").
// Returns the accumulated text and whether the cap was reached.
func appendCapped(dst, s string) (string, bool) {
	dstRunes := utf8.RuneCountInString(dst)
	if dstRunes+utf8.RuneCountInString(s) <= maxTextRunes {
		return dst + s, false
	}
	room := maxTextRunes - dstRunes
	if room > 0 {
		return truncate(dst+string([]rune(s)[:room]), maxTextRunes), true
	}
	return truncate(dst, maxTextRunes), true
}
