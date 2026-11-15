package telegram

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"opentw/internal/storage"
)

// BotCfg配置 бота.
type BotCfg struct {
	API    *Client
	Store  *storage.Store
	Models []storage.Model // упорядоченные модели (family, id)
	Logger *slog.Logger
}

// Bot обрабатывает команды Telegram и ведёт статусные панели.
type Bot struct {
	api    *Client
	store  *storage.Store
	models []storage.Model
	log    *slog.Logger

	lastText map[int64]string // кэш последнего текста панели по чату
	pinWarn  map[int64]bool   // однократное предупреждение о запрете закрепления
	tick     time.Duration    // интервал мониторинга для «Следующая проверка»

	drafts map[int64]map[string]bool // черновики подписок до «Сохранить»
	mu     sync.Mutex
}

// NewBot создаёт бота.
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
		lastText: map[int64]string{},
		pinWarn:  map[int64]bool{},
		drafts:   map[int64]map[string]bool{},
	}
}

// Run запускает long polling до отмены ctx.
func (b *Bot) Run(ctx context.Context) error {
	if err := b.api.SetMyCommands(ctx, []BotCommand{
		{Command: "status", Description: "Общий статус всех моделей"},
		{Command: "subscribe", Description: "Настройка уведомлений"},
		{Command: "help", Description: "Справка"},
	}); err != nil {
		b.log.Warn("telegram: setMyCommands failed", "err", err)
	}
	offset := int64(0)
	for {
		updates, err := b.api.GetUpdates(ctx, offset, 25)
		if err != nil {
			if ctx.Err() != nil {
				return nil
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
		}
	}
}

// HandleUpdate路由 одного апдейта.
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
		_, err := b.api.SendMessage(ctx, msg.Chat.ID, RenderHelp())
		return err
	case "":
		return nil
	default:
		return nil // неизвестные команды молча игнорируются (лишних команд нет)
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
	return 0 // анонимный канал-пост; коллбэки от реальных пользователей проверяются отдельно
}

// handleStatusCommand: /status (без аргумента — панель) или /status model-id.
func (b *Bot) handleStatusCommand(ctx context.Context, chat Chat, modelID string) error {
	if modelID == "" {
		return b.ensurePanel(ctx, chat.ID, true)
	}
	return b.handleModelDetail(ctx, chat.ID, modelID)
}

func (b *Bot) handleModelDetail(ctx context.Context, chatID int64, modelID string) error {
	m, err := b.store.Model(modelID)
	if err != nil {
		return err
	}
	if m == nil {
		_, err := b.api.SendMessage(ctx, chatID, "Модель не найдена: "+EscapeHTML(modelID)+"\nТочный model-id указан в панели /status.")
		return err
	}
	checks, err := b.store.RecentChecks(modelID, time.Now().Add(-24*time.Hour))
	if err != nil {
		return err
	}
	last := storage.Check{}
	if len(checks) > 0 {
		last = checks[len(checks)-1]
	} else if sm, err := b.store.Model(modelID); err == nil && sm != nil && sm.LastCheckAt != nil {
		last = storage.Check{ModelID: modelID, At: *sm.LastCheckAt, Status: sm.LastStatus, ErrorType: sm.ErrorType, ErrorMessage: sm.ErrorMessage}
	}
	text := RenderModelDetail(modelID, m.LastStatus, last, checks)
	_, err = b.api.SendMessage(ctx, chatID, text)
	return err
}
