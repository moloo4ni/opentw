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
	// Error — ошибка шлюза, доставленная прямо в SSE-данных (HTTP 200):
	// не то же самое, что транспортный сбой или пустая генерация.
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
// streamOutcome — итог чтения SSE-потока.
type streamOutcome struct {
	usage     *usage
	sawData   bool
	parsedAny bool
	generated bool
	// content — накопленный итоговый ответ (только delta.content,
	// reasoning не считается ответом). При патологически длинном потоке
	// обрезан по maxTextRunes с видимым «…» (contentFull).
	content     string
	contentFull bool
	// finishReason — первый непустой finish_reason терминального чанка той
	// же choice, из которой накоплен итоговый ответ. Usage-only чанки не могут
	// его очистить; отсутствующее значение остаётся пустым.
	finishReason string
	// finishReasons — непустые finish_reason по index choice. Итог берётся
	// после чтения потока от той же choice, которая дала content.
	finishReasons     map[int]string
	firstFinishChoice int
	hasFinishChoice   bool
	// selectedChoice — choice, из которой берётся content (или первая
	// содержательная choice при его отсутствии); нужно для корректного
	// finish_reason в ответах с несколькими choices.
	selectedChoice   int
	hasChoice        bool
	hasContentChoice bool
	// dataErr — текст поля error из SSE-данных (первое непустое сообщение).
	dataErr string
	// rawPrefix — прочитанное до первого data: (≤ 8КБ): при stream:true
	// шлюз может ответить обычным JSON с ошибкой вместо SSE.
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
					// Сначала сохраняем reason по index choice. Источник итогового
					// ответа может быть выбран позже, поэтому глобально присваивать
					// reason до конца потока нельзя.
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
						// Первый непустой content — источник итогового ответа.
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
			// не-data строка до первого SSE-события: сохраняем — возможно,
			// это JSON-ответ шлюза (многострочный JSON собирается целиком)
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
		// Нет ни content, ни reasoning: берём первый фактический finish_reason.
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

// bodyGatewayError извлекает текст ошибки шлюза из не-SSE тела ответа при
// stream:true (нет ни одного data:-события). "" — если это не JSON с ошибкой.
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

// truncate обрезает по рунам: байтовый срез ломал бы многобайтовые символы.
func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

// appendCapped дописывает s к dst с пределом maxTextRunes рун: безграничный
// поток не раздувает память и хранилище; обрезка видима («…»). Возвращает
// накопленный текст и признак достижения предела.
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
