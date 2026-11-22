// Package telegram implements the minimal Bot API client used by OpenTW:
// long polling, messaging, pinning, chat member checks and callbacks.
package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

const maxRetries = 2 // 429: up to 2 retries with retry_after, at most 3 requests total

// ApiError is a Bot API level error (ok:false).
type ApiError struct {
	Code        int
	Description string
	RetryAfter  float64
}

func (e *ApiError) Error() string {
	return fmt.Sprintf("telegram: API error %d: %s", e.Code, e.Description)
}

func asApiError(err error, target **ApiError) bool {
	return errors.As(err, target)
}

// redactTransportError unwraps *url.Error: its text contains the method URL
// with the BOT_TOKEN. The failure cause (ue.Err — dial/timeout/cancel) is
// kept for diagnostics; ApiError/429 recognition is unaffected — those come
// from the response body, not the transport.
func redactTransportError(err error) error {
	var ue *url.Error
	if !errors.As(err, &ue) {
		return err
	}
	if ue.Err != nil {
		return ue.Err
	}
	return errors.New(ue.Op + " request failed") // a url.Error without a cause reveals no URL
}

// Client talks to the Bot API; BaseURL is injectable for tests.
type Client struct {
	Token   string
	BaseURL string
	HTTP    *http.Client
}

// NewClient creates a Bot API client.
func NewClient(token string) *Client {
	return &Client{Token: token, BaseURL: "https://api.telegram.org", HTTP: &http.Client{Timeout: 120 * time.Second}}
}

type apiResponse struct {
	OK          bool            `json:"ok"`
	Result      json.RawMessage `json:"result"`
	ErrorCode   int             `json:"error_code"`
	Description string          `json:"description"`
	Parameters  *struct {
		RetryAfter float64 `json:"retry_after"`
	} `json:"parameters"`
}

// call performs a Bot API method with one 429 retry (honouring retry_after).
func (c *Client) call(ctx context.Context, method string, payload any, out any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("telegram: encode %s: %w", method, err)
	}
	url := fmt.Sprintf("%s/bot%s/%s", c.BaseURL, c.Token, method)

	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("telegram: build %s: %w", method, redactTransportError(err))
		}
		req.Header.Set("Content-Type", "application/json")

		httpClient := c.HTTP
		if httpClient == nil {
			httpClient = http.DefaultClient
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("telegram: %s: %w", method, redactTransportError(err))
		}
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
		resp.Body.Close()

		var envelope apiResponse
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return fmt.Errorf("telegram: %s: unparseable response: %w", method, err)
		}
		if !envelope.OK {
			apiErr := &ApiError{Code: envelope.ErrorCode, Description: envelope.Description}
			if envelope.Parameters != nil {
				apiErr.RetryAfter = envelope.Parameters.RetryAfter
			}
			if apiErr.Code == 429 && attempt < maxRetries {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Duration(apiErr.RetryAfter * float64(time.Second))):
				}
				continue
			}
			return apiErr
		}
		if out != nil {
			if err := json.Unmarshal(envelope.Result, out); err != nil {
				return fmt.Errorf("telegram: %s: decode result: %w", method, err)
			}
		}
		return nil
	}
}

// GetUpdates long-polls for updates after offset.
func (c *Client) GetUpdates(ctx context.Context, offset int64, timeoutSec int) ([]Update, error) {
	var updates []Update
	err := c.call(ctx, "getUpdates", map[string]any{
		"offset":          offset,
		"timeout":         timeoutSec,
		"allowed_updates": []string{"message", "edited_message", "channel_post", "callback_query"},
	}, &updates)
	return updates, err
}

// SendMessage sends an HTML-mode message without preview.
func (c *Client) SendMessage(ctx context.Context, chatID int64, text string) (*Message, error) {
	return c.SendMessageWithKeyboard(ctx, chatID, text, nil)
}

// SendMessageWithKeyboard sends an HTML message with an optional inline keyboard.
func (c *Client) SendMessageWithKeyboard(ctx context.Context, chatID int64, text string, kb *InlineKeyboardMarkup) (*Message, error) {
	payload := map[string]any{
		"chat_id":                  chatID,
		"text":                     text,
		"parse_mode":               "HTML",
		"disable_web_page_preview": true,
	}
	if kb != nil {
		payload["reply_markup"] = kb
	}
	var msg Message
	if err := c.call(ctx, "sendMessage", payload, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

// EditMessageText replaces the text (and, optionally, keyboard) of a message.
func (c *Client) EditMessageText(ctx context.Context, chatID, messageID int64, text string, kb *InlineKeyboardMarkup) error {
	payload := map[string]any{
		"chat_id":                  chatID,
		"message_id":               messageID,
		"text":                     text,
		"parse_mode":               "HTML",
		"disable_web_page_preview": true,
	}
	if kb != nil {
		payload["reply_markup"] = kb
	}
	return c.call(ctx, "editMessageText", payload, nil)
}

// SendRichMessage sends a Bot API rich message (html, no entity detection).
func (c *Client) SendRichMessage(ctx context.Context, chatID int64, html string) (*Message, error) {
	payload := map[string]any{
		"chat_id": chatID,
		"rich_message": map[string]any{
			"html":                  html,
			"skip_entity_detection": true,
		},
	}
	var msg Message
	if err := c.call(ctx, "sendRichMessage", payload, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

// EditRichMessageText replaces the content of a message via rich_message
// (no parse_mode: the panel edit uses rich markup).
func (c *Client) EditRichMessageText(ctx context.Context, chatID, messageID int64, html string) error {
	payload := map[string]any{
		"chat_id":    chatID,
		"message_id": messageID,
		"rich_message": map[string]any{
			"html":                  html,
			"skip_entity_detection": true,
		},
	}
	return c.call(ctx, "editMessageText", payload, nil)
}

// PinChatMessage pins a message.
func (c *Client) PinChatMessage(ctx context.Context, chatID, messageID int64) error {
	return c.call(ctx, "pinChatMessage", map[string]any{"chat_id": chatID, "message_id": messageID, "disable_notification": true}, nil)
}

// UnpinChatMessage unpins a specific message.
func (c *Client) UnpinChatMessage(ctx context.Context, chatID, messageID int64) error {
	return c.call(ctx, "unpinChatMessage", map[string]any{"chat_id": chatID, "message_id": messageID}, nil)
}

// DeleteMessage deletes a message (in private chats and groups the bot may
// delete its own messages; in channels it needs the admin right).
func (c *Client) DeleteMessage(ctx context.Context, chatID, messageID int64) error {
	return c.call(ctx, "deleteMessage", map[string]any{"chat_id": chatID, "message_id": messageID}, nil)
}

// GetChatMember fetches the membership of a user in a chat.
func (c *Client) GetChatMember(ctx context.Context, chatID, userID int64) (*ChatMember, error) {
	var member ChatMember
	if err := c.call(ctx, "getChatMember", map[string]any{"chat_id": chatID, "user_id": userID}, &member); err != nil {
		return nil, err
	}
	return &member, nil
}

// AnswerCallbackQuery answers a callback press (typically a toast).
func (c *Client) AnswerCallbackQuery(ctx context.Context, callbackID, text string) error {
	return c.call(ctx, "answerCallbackQuery", map[string]any{"callback_query_id": callbackID, "text": text}, nil)
}

// SetMyCommands publishes the command list.
func (c *Client) SetMyCommands(ctx context.Context, cmds []BotCommand) error {
	return c.call(ctx, "setMyCommands", map[string]any{"commands": cmds}, nil)
}
