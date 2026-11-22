package telegram

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"opentw/internal/status"
	"opentw/internal/storage"
)

// BotCfg configures the bot.
type BotCfg struct {
	API      *Client
	Store    *storage.Store
	Models   []storage.Model // ordered models (family, id)
	Logger   *slog.Logger
	Interval time.Duration // monitoring interval, for "next check" lines
}

// Bot handles Telegram commands and maintains status panels.
type Bot struct {
	api    *Client
	store  *storage.Store
	models []storage.Model
	log    *slog.Logger
	tick   time.Duration

	lastText map[int64]string // per-chat cache of the last panel text
	pinWarn  map[int64]bool   // one-time warning about a missing pin permission

	drafts map[int64]map[string]bool // subscription drafts until "Save"
	mu     sync.Mutex

	wake        chan struct{} // nudges the delivery worker after a cycle
	floodUntil  atomic.Int64  // unix seconds of the batch pause after a 429 (the bot's flood limit is global)
	panelDirty  atomic.Bool   // a cycle changed state — panels need a refresh
	lastCleanup atomic.Int64  // unix time of the last outbox cleanup
}

// wakeDelivery wakes the delivery worker (non-blocking).
func (b *Bot) wakeDelivery() {
	select {
	case b.wake <- struct{}{}:
	default:
	}
}

// NewBot creates the bot.
func NewBot(cfg BotCfg) *Bot {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Bot{
		api:      cfg.API,
		store:    cfg.Store,
		models:   cfg.Models,
		log:      log,
		tick:     cfg.Interval,
		lastText: map[int64]string{},
		pinWarn:  map[int64]bool{},
		drafts:   map[int64]map[string]bool{},
		wake:     make(chan struct{}, 1),
	}
}

// Run runs long polling until ctx is cancelled.
func (b *Bot) Run(ctx context.Context) error {
	if err := b.api.SetMyCommands(ctx, []BotCommand{
		{Command: "start", Description: "Показать статус моделей"},
		{Command: "status", Description: "Общий статус всех моделей"},
		{Command: "subscribe", Description: "Настройка уведомлений"},
		{Command: "help", Description: "Справка"},
	}); err != nil {
		b.log.Warn("telegram: setMyCommands failed", "err", err)
	}
	offset, err := b.store.BotOffset() // previously confirmed updates are not re-delivered
	if err != nil {
		b.log.Error("telegram: read offset", "err", err)
	}
	for {
		updates, err := b.api.GetUpdates(ctx, offset, 25)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if fatal := fatalTelegramErr(err); fatal != nil {
				return fatal
			}
			b.log.Error("telegram: getUpdates", "err", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(3 * time.Second):
			}
			continue
		}
		for _, u := range updates {
			offset = u.UpdateID + 1
			func() {
				defer func() {
					if r := recover(); r != nil {
						b.log.Error("telegram: update handler panic", "recover", r)
					}
				}()
				if err := b.HandleUpdate(ctx, &u); err != nil {
					b.log.Error("telegram: handle update", "err", err)
				}
			}()
			// the update is confirmed only after handling: a crash in this window
			// yields a (idempotent) repeat, not a lost command
			if err := b.store.SetBotOffset(offset); err != nil {
				b.log.Warn("telegram: persist offset", "offset", offset, "err", err)
			}
		}
	}
}

// HandleUpdate routes one update.
func (b *Bot) HandleUpdate(ctx context.Context, u *Update) error {
	switch {
	case u.Message != nil:
		return b.handleMessage(ctx, u.Message)
	case u.ChannelPost != nil:
		return b.handleMessage(ctx, u.ChannelPost)
	case u.CallbackQuery != nil:
		return b.handleCallback(ctx, u.CallbackQuery)
	case u.EditedMessage != nil:
		return nil
	}
	return nil
}

func (b *Bot) handleMessage(ctx context.Context, msg *Message) error {
	text := strings.TrimSpace(msg.Text)
	cmd, arg := splitCommand(text)
	switch cmd {
	case "/start", "/status":
		return b.handleStatusCommand(ctx, msg.Chat, arg)
	case "/subscribe":
		return b.handleSubscribeCommand(ctx, msg.Chat, fromID(msg))
	case "/help":
		_, err := b.api.SendMessage(ctx, msg.Chat.ID, RenderHelp(b.interval()))
		return err
	case "":
		return nil
	default:
		return nil // unknown commands are silently ignored (no extra commands exist)
	}
}

func splitCommand(text string) (string, string) {
	fields := strings.Fields(text)
	if len(fields) == 0 || !strings.HasPrefix(fields[0], "/") {
		return "", ""
	}
	cmd := strings.SplitN(fields[0], "@", 2)[0]
	arg := strings.TrimSpace(strings.TrimPrefix(text, fields[0]))
	return cmd, arg
}

func fromID(msg *Message) int64 {
	if msg.From != nil {
		return msg.From.ID
	}
	if msg.Chat.Type == "private" {
		return msg.Chat.ID
	}
	return 0 // anonymous channel post; callbacks from real users are checked separately
}

// handleStatusCommand: /status without an argument shows the panel;
// /status model-id shows the model card.
func (b *Bot) handleStatusCommand(ctx context.Context, chat Chat, modelID string) error {
	if modelID == "" {
		return b.ensurePanel(ctx, chat.ID)
	}
	return b.handleModelDetail(ctx, chat.ID, modelID)
}

func (b *Bot) handleModelDetail(ctx context.Context, chatID int64, modelID string) error {
	m, err := b.store.Model(modelID)
	if err != nil {
		return err
	}
	if m == nil {
		// input softening: a unique prefix completes to the exact ID
		if id := b.uniquePrefix(modelID); id != "" {
			modelID = id
			m, err = b.store.Model(modelID)
			if err != nil {
				return err
			}
		}
	}
	if m == nil {
		if cands := b.prefixCandidates(modelID); len(cands) > 0 {
			// ambiguous prefix: suggest the nearest matches
			_, err := b.api.SendMessage(ctx, chatID, "Уточните модель: "+codeIDs(cands))
			return err
		}
		// the argument is external input: truncate before escaping, or a
		// long paste inflates the reply past the Telegram limit
		_, err := b.api.SendMessage(ctx, chatID, "Модель не найдена: "+EscapeHTML(trunc(modelID, 64))+"\nТочный model-id указан в панели /status.")
		return err
	}
	checks, err := b.store.RecentChecks(modelID, time.Now().Add(-24*time.Hour))
	if err != nil {
		return err
	}
	last := storage.Check{}
	if len(checks) > 0 {
		last = checks[len(checks)-1]
	} else if m.LastCheckAt != nil {
		last = storage.Check{ModelID: modelID, At: *m.LastCheckAt, Status: m.LastStatus, ErrorType: m.ErrorType, ErrorMessage: m.ErrorMessage}
	}
	return b.sendModelCard(ctx, chatID, modelID, m.LastStatus, last, checks)
}

// sendModelCard sends the model's rich card; the tail of the "Response"
// section that does not fit the limit goes as follow-up rich messages,
// without silent truncation. Runes are counted ESCAPED: special characters
// grow up to 6× after EscapeHTML, the limit applies to the final HTML.
func (b *Bot) sendModelCard(ctx context.Context, chatID int64, modelID string, st status.Status, last storage.Check, checks []storage.Check) error {
	card, followup := renderModelCard(modelID, st, last, checks)
	if _, err := b.api.SendRichMessage(ctx, chatID, card); err != nil {
		return err
	}
	rest := []rune(followup)
	const limit = maxRichMessage - 8 // headroom for the <p>…</p> wrapper
	i := 0
	for i < len(rest) {
		cost := 0
		j := i
		for j < len(rest) {
			c := escapedRuneLen(rest[j])
			if cost+c > limit {
				break // a single rune (max 6) never exceeds the limit
			}
			cost += c
			j++
		}
		if _, err := b.api.SendRichMessage(ctx, chatID, "<p>"+EscapeHTML(string(rest[i:j]))+"</p>"); err != nil {
			return err
		}
		i = j
	}
	return nil
}

// uniquePrefix returns the exact ID when the prefix matches exactly one model.
func (b *Bot) uniquePrefix(arg string) string {
	c := b.prefixCandidates(arg)
	if len(c) == 1 {
		return c[0]
	}
	return ""
}

// prefixCandidates returns model IDs starting with arg (up to 5, for hints).
func (b *Bot) prefixCandidates(arg string) []string {
	if arg == "" {
		return nil
	}
	var out []string
	for _, m := range b.models {
		if strings.HasPrefix(m.ID, arg) {
			out = append(out, m.ID)
			if len(out) >= 5 {
				break
			}
		}
	}
	return out
}

func codeIDs(ids []string) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = codeID(id)
	}
	return strings.Join(parts, ", ")
}

// handleStatusCallback — error buttons in panels and notifications:
// answerCallbackQuery immediately, the card goes as a NEW message to the
// same chat, the panel is not edited, subscriptions and drafts untouched.
// A model in Down/Unknown is NOT a stale button: unavailability does not
// invalidate the callback.
func (b *Bot) handleStatusCallback(ctx context.Context, cb *CallbackQuery) error {
	if cb.Message == nil {
		// neither chat_id nor message_id — nowhere to send the card
		return b.api.AnswerCallbackQuery(ctx, cb.ID, "Не удалось открыть карточку модели.")
	}
	id, ok := resolveCallback(strings.TrimPrefix(cb.Data, "st:"), modelIDs(b.models))
	if !ok {
		// model removed from config: the button lives in an old message
		return b.api.AnswerCallbackQuery(ctx, cb.ID, "Модель больше не отслеживается.")
	}
	// the loading indicator is dropped before the heavy work
	if err := b.api.AnswerCallbackQuery(ctx, cb.ID, ""); err != nil {
		b.log.Warn("status callback: answer", "err", err)
	}
	if err := b.handleModelDetail(ctx, cb.Message.Chat.ID, id); err != nil {
		b.log.Error("status callback: model detail", "model", id, "err", err)
	}
	return nil
}

// fatalTelegramErr returns a permanent error for configuration problems that
// retrying cannot fix: wrong bot token (401), forbidden (403), not found (404).
func fatalTelegramErr(err error) error {
	var apiErr *ApiError
	if !errors.As(err, &apiErr) {
		return nil
	}
	switch apiErr.Code {
	case 401, 403, 404:
		return fmt.Errorf("telegram: fatal API error %d: %s (check BOT_TOKEN)", apiErr.Code, apiErr.Description)
	}
	return nil
}
