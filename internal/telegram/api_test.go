package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// mockTG is a scriptable Telegram Bot API server.
type mockTG struct {
	mu      sync.Mutex
	calls   []string                                    // method paths
	bodies  map[string][]byte                           // last body per method
	handler func(method string, body []byte) (any, int) // custom response, status
	token   string
	url     string
	msgID   int64
}

func (m *mockTG) nextMessageID() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.msgID++
	return m.msgID
}

func newMockTG(t *testing.T) *mockTG {
	t.Helper()
	m := &mockTG{bodies: map[string][]byte{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/bot")
		method := path[strings.LastIndex(path, "/")+1:]
		token := strings.TrimSuffix(path, "/"+method)
		m.mu.Lock()
		m.calls = append(m.calls, method)
		body := make([]byte, r.ContentLength)
		r.Body.Read(body)
		m.bodies[method] = body
		m.token = token
		m.mu.Unlock()
		var payload any
		status := http.StatusOK
		if m.handler != nil {
			payload, status = m.handler(method, body)
		} else if method == "sendMessage" {
			payload = map[string]any{"ok": true, "result": map[string]any{"message_id": m.nextMessageID()}}
		} else {
			payload = map[string]any{"ok": true, "result": true}
		}
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(payload)
	}))
	t.Cleanup(srv.Close)
	m.url = srv.URL
	return m
}

func (m *mockTG) callCount(method string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, c := range m.calls {
		if c == method {
			n++
		}
	}
	return n
}

func (m *mockTG) lastBody(method string) map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]any{}
	json.Unmarshal(m.bodies[method], &out)
	return out
}

func newTestBot(m *mockTG) *Client {
	return &Client{Token: "TOKEN", BaseURL: m.url, HTTP: &http.Client{Timeout: 5 * time.Second}}
}

func TestSendMessageOK(t *testing.T) {
	m := newMockTG(t)
	m.handler = func(method string, body []byte) (any, int) {
		return map[string]any{"ok": true, "result": map[string]any{"message_id": 42, "chat": map[string]any{"id": 100}}}, http.StatusOK
	}
	c := newTestBot(m)
	ctx := context.Background()
	msg, err := c.SendMessage(ctx, 100, "hello <b>world</b>")
	if err != nil {
		t.Fatal(err)
	}
	if msg.MessageID != 42 {
		t.Fatalf("message id = %d", msg.MessageID)
	}
	body := m.lastBody("sendMessage")
	if body["chat_id"].(float64) != 100 || body["text"] != "hello <b>world</b>" {
		t.Fatalf("body = %v", body)
	}
	if body["parse_mode"] != "HTML" {
		t.Fatalf("parse mode = %v", body["parse_mode"])
	}
	if dp, ok := body["disable_web_page_preview"].(bool); !ok || !dp {
		t.Fatalf("preview not disabled: %v", body)
	}
	if !strings.HasPrefix(m.token, "TOKEN") {
		t.Fatalf("token path = %q", m.token)
	}
}

func TestSendMessageWithKeyboard(t *testing.T) {
	m := newMockTG(t)
	m.handler = func(method string, body []byte) (any, int) {
		return map[string]any{"ok": true, "result": map[string]any{"message_id": 7}}, http.StatusOK
	}
	c := newTestBot(m)
	kb := InlineKeyboardMarkup{InlineKeyboard: [][]InlineButton{{{Text: "✓ glm-5.3", CallbackData: "sub:glm-5.3"}}}}
	if _, err := c.SendMessageWithKeyboard(context.Background(), 1, "text", &kb); err != nil {
		t.Fatal(err)
	}
	body := m.lastBody("sendMessage")
	markup, _ := json.Marshal(body["reply_markup"])
	if !strings.Contains(string(markup), "sub:glm-5.3") {
		t.Fatalf("markup = %s", markup)
	}
}

func TestSend429RetriesOnce(t *testing.T) {
	m := newMockTG(t)
	attempts := 0
	m.handler = func(method string, body []byte) (any, int) {
		if method == "sendMessage" {
			attempts++
			if attempts == 1 {
				return map[string]any{"ok": false, "error_code": 429, "description": "Too Many Requests: retry after 0", "parameters": map[string]any{"retry_after": 0}}, http.StatusTooManyRequests
			}
		}
		return map[string]any{"ok": true, "result": map[string]any{"message_id": 7}}, http.StatusOK
	}
	c := newTestBot(m)
	msg, err := c.SendMessage(context.Background(), 1, "x")
	if err != nil {
		t.Fatal(err)
	}
	if msg.MessageID != 7 {
		t.Fatalf("message id = %d", msg.MessageID)
	}
	if m.callCount("sendMessage") != 2 {
		t.Fatalf("retries = %d", m.callCount("sendMessage"))
	}
}

func TestApiError(t *testing.T) {
	m := newMockTG(t)
	m.handler = func(method string, body []byte) (any, int) {
		return map[string]any{"ok": false, "error_code": 400, "description": "Bad Request: message to edit not found"}, http.StatusBadRequest
	}
	c := newTestBot(m)
	err := c.EditMessageText(context.Background(), 1, 5, "x", nil)
	var apiErr *ApiError
	if err == nil {
		t.Fatal("expected error")
	}
	if ok := asApiError(err, &apiErr); !ok {
		t.Fatalf("want ApiError, got %v", err)
	}
	if apiErr.Code != 400 || !strings.Contains(apiErr.Description, "message to edit not found") {
		t.Fatalf("api error = %+v", apiErr)
	}
}

func TestApiError429SurfacesAfterRetries(t *testing.T) {
	m := newMockTG(t)
	m.handler = func(method string, body []byte) (any, int) {
		return map[string]any{"ok": false, "error_code": 429, "description": "Too Many Requests", "parameters": map[string]any{"retry_after": 0}}, http.StatusTooManyRequests
	}
	c := newTestBot(m)
	_, err := c.SendMessage(context.Background(), 1, "x")
	var apiErr *ApiError
	if !asApiError(err, &apiErr) || apiErr.Code != 429 {
		t.Fatalf("err = %v", err)
	}
	if m.callCount("sendMessage") != maxRetries+1 {
		t.Fatalf("attempts = %d", m.callCount("sendMessage"))
	}
}

func TestGetUpdatesParses(t *testing.T) {
	m := newMockTG(t)
	m.handler = func(method string, body []byte) (any, int) {
		return map[string]any{"ok": true, "result": []map[string]any{
			{"update_id": 10, "message": map[string]any{"message_id": 1, "text": "/status", "chat": map[string]any{"id": 100, "type": "private"}, "from": map[string]any{"id": 55, "is_bot": false}}},
			{"update_id": 11, "callback_query": map[string]any{"id": "cb1", "from": map[string]any{"id": 55, "is_bot": false}, "data": "sub:glm-5.3", "message": map[string]any{"message_id": 2, "chat": map[string]any{"id": 100, "type": "private"}}}},
		}}, http.StatusOK
	}
	c := newTestBot(m)
	updates, err := c.GetUpdates(context.Background(), 9, 25)
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 2 {
		t.Fatalf("updates = %d", len(updates))
	}
	if updates[0].Message == nil || updates[0].Message.Text != "/status" || updates[0].Message.Chat.ID != 100 {
		t.Fatalf("message update = %+v", updates[0].Message)
	}
	if updates[1].CallbackQuery == nil || updates[1].CallbackQuery.Data != "sub:glm-5.3" || updates[1].CallbackQuery.From.ID != 55 {
		t.Fatalf("callback update = %+v", updates[1].CallbackQuery)
	}
	body := m.lastBody("getUpdates")
	if body["offset"].(float64) != 9 {
		t.Fatalf("offset = %v", body["offset"])
	}
}

func TestGetChatMemberAdmin(t *testing.T) {
	m := newMockTG(t)
	for _, tc := range []struct {
		status string
		admin  bool
	}{{"creator", true}, {"administrator", true}, {"member", false}, {"left", false}} {
		st := tc.status
		m.handler = func(method string, body []byte) (any, int) {
			return map[string]any{"ok": true, "result": map[string]any{"status": st, "user": map[string]any{"id": 55}}}, http.StatusOK
		}
		c := newTestBot(m)
		member, err := c.GetChatMember(context.Background(), -100, 55)
		if err != nil {
			t.Fatal(err)
		}
		if member.Status != st {
			t.Fatalf("status = %q", member.Status)
		}
		if member.IsAdmin() != tc.admin {
			t.Fatalf("IsAdmin(%q) = %v", st, member.IsAdmin())
		}
	}
}

func TestPinUnpinAnswerCommands(t *testing.T) {
	m := newMockTG(t)
	c := newTestBot(m)
	ctx := context.Background()
	if err := c.PinChatMessage(ctx, 100, 42); err != nil {
		t.Fatal(err)
	}
	if b := m.lastBody("pinChatMessage"); b["chat_id"].(float64) != 100 || b["message_id"].(float64) != 42 {
		t.Fatalf("pin body = %v", b)
	}
	if err := c.UnpinChatMessage(ctx, 100, 42); err != nil {
		t.Fatal(err)
	}
	if b := m.lastBody("unpinChatMessage"); b["message_id"].(float64) != 42 {
		t.Fatalf("unpin body = %v", b)
	}
	if err := c.AnswerCallbackQuery(ctx, "cb1", "saved"); err != nil {
		t.Fatal(err)
	}
	if b := m.lastBody("answerCallbackQuery"); b["callback_query_id"] != "cb1" || b["text"] != "saved" {
		t.Fatalf("answer body = %v", b)
	}
	cmds := []BotCommand{{Command: "status", Description: "статус"}}
	if err := c.SetMyCommands(ctx, cmds); err != nil {
		t.Fatal(err)
	}
	if b := m.lastBody("setMyCommands"); fmt.Sprint(b["commands"]) == "" {
		t.Fatal("commands missing")
	}
}

func TestNetworkError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close()
	c := &Client{Token: "T", BaseURL: srv.URL, HTTP: &http.Client{Timeout: time.Second}}
	if err := c.PinChatMessage(context.Background(), 1, 1); err == nil {
		t.Fatal("network error expected")
	}
}

func TestDeleteMessage(t *testing.T) {
	m := newMockTG(t)
	c := newTestBot(m)
	if err := c.DeleteMessage(context.Background(), 100, 55); err != nil {
		t.Fatal(err)
	}
	body := m.lastBody("deleteMessage")
	if body["chat_id"].(float64) != 100 || body["message_id"].(float64) != 55 {
		t.Fatalf("delete body = %v", body)
	}
}

// 1.1: транспортные ошибки http-клиента несут URL с BOT_TOKEN
// (Post "https://api.telegram.org/bot<токен>/метод": ...) — текст
// возвращаемой ошибки и всех её обёрток не должен его раскрывать.
func TestTransportErrorDoesNotLeakToken(t *testing.T) {
	const token = "123456:AAE-SECRET-token-XYZ"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close() // порт закрыт: соединение отклонено

	c := &Client{Token: token, BaseURL: srv.URL, HTTP: &http.Client{Timeout: time.Second}}
	err := c.PinChatMessage(context.Background(), 1, 1)
	if err == nil {
		t.Fatal("network error expected")
	}
	assertNoTokenInChain(t, err, token)
}

func TestTransportTimeoutDoesNotLeakToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body) // тело прочитано: разрыв коннекта клиентом отменит ctx
		<-r.Context().Done()        // зависаем до клиентского таймаута
	}))
	defer srv.Close()

	const token = "123456:AAE-SECRET-token-XYZ"
	c := &Client{Token: token, BaseURL: srv.URL, HTTP: &http.Client{Timeout: 250 * time.Millisecond}}
	_, err := c.SendMessage(context.Background(), 1, "hi")
	if err == nil {
		t.Fatal("timeout error expected")
	}
	assertNoTokenInChain(t, err, token)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout cause must stay diagnosable: %v", err)
	}
	srv.CloseClientConnections() // зависший коннект не должен тормозить Close
}

func TestTransportCancelledDoesNotLeakToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	defer srv.Close()

	const token = "123456:AAE-SECRET-token-XYZ"
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	c := &Client{Token: token, BaseURL: srv.URL, HTTP: &http.Client{Timeout: 5 * time.Second}}
	_, err := c.SendMessage(ctx, 1, "hi")
	if err == nil {
		t.Fatal("cancelled error expected")
	}
	assertNoTokenInChain(t, err, token)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel cause must stay diagnosable: %v", err)
	}
	srv.CloseClientConnections()
}

// assertNoTokenInChain проверяет текст ошибки и каждой её обёртки.
func assertNoTokenInChain(t *testing.T, err error, token string) {
	t.Helper()
	for e := err; e != nil; e = errors.Unwrap(e) {
		msg := e.Error()
		if strings.Contains(msg, token) {
			t.Fatalf("BOT_TOKEN leaked in error: %q", msg)
		}
		if strings.Contains(msg, "/bot") {
			t.Fatalf("full bot URL (masked token, /bot path) leaked in error: %q", msg)
		}
	}
}
