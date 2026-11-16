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

	"opentw/internal/storage"
)

// BotCfg configures the bot.
type BotCfg struct {
	API      *Client
	Store    *storage.Store
	Models   []storage.Model // упорядоченные модели (family, id)
	Logger   *slog.Logger
	Interval time.Duration // интервал мониторинга для «Следующая проверка»
}

// Bot обрабатывает команды Telegram и ведёт статусные панели.
type Bot struct {
	api    *Client
	store  *storage.Store
	models []storage.Model
	log    *slog.Logger
	tick   time.Duration

	lastText map[int64]string // кэш последнего текста панели по чату
	pinWarn  map[int64]bool   // однократное предупреждение о запрете закрепления

	drafts map[int64]map[string]bool // черновики подписок до «Сохранить»
	mu     sync.Mutex

	wake         chan struct{} // букс deliveries-воркера после цикла
	floodUntil   atomic.Int64  // unix-сек паузы рассылки после 429 (флуд-лимит бота глобальный)
	panelDirty   atomic.Bool   // цикл изменил состояние — панели надо обновить
	lastCleanup  atomic.Int64  // unix-время последней очистки outbox
	backlogStale time.Duration // порог дайджеста в тестах; 0 = 2×interval
}

// wakeDelivery будит воркер доставки (неблокирующе).
func (b *Bot) wakeDelivery() {
	select {
	case b.wake <- struct{}{}:
	default:
	}
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
		tick:     cfg.Interval,
		lastText: map[int64]string{},
		pinWarn:  map[int64]bool{},
		drafts:   map[int64]map[string]bool{},
		wake:     make(chan struct{}, 1),
	}
}

// Run запускает long polling до отмены ctx.
func (b *Bot) Run(ctx context.Context) error {
	if err := b.api.SetMyCommands(ctx, []BotCommand{
		{Command: "start", Description: "Показать статус моделей"},
		{Command: "status", Description: "Общий статус всех моделей"},
		{Command: "subscribe", Description: "Настройка уведомлений"},
		{Command: "help", Description: "Справка"},
	}); err != nil {
		b.log.Warn("telegram: setMyCommands failed", "err", err)
	}
	offset, err := b.store.BotOffset() // подтверждённые ранее апдейты не доставляются повторно
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
			// подтверждаем апдейт только после обработки: крах в этом окне
			// даст повтор (идемпотентный), а не потерю команды
			if err := b.store.SetBotOffset(offset); err != nil {
				b.log.Warn("telegram: persist offset", "offset", offset, "err", err)
			}
		}
	}
}

// HandleUpdate маршрутизирует один апдейт.
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
		// смягчение ввода: уникальный префикс достаивает до точного ID
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
			// неоднозначный префикс: подсказываем ближайшие
			_, err := b.api.SendMessage(ctx, chatID, "Уточните модель: "+codeIDs(cands))
			return err
		}
		// аргумент — внешние данные: ограничиваем до экранирования,
		// иначе длинный ввод раздувает ответ за лимит Telegram
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
	text := RenderModelDetail(modelID, m.LastStatus, last, checks)
	_, err = b.api.SendMessage(ctx, chatID, text)
	return err
}

// uniquePrefix — точное совпадение по префиксу, если оно единственное.
func (b *Bot) uniquePrefix(arg string) string {
	c := b.prefixCandidates(arg)
	if len(c) == 1 {
		return c[0]
	}
	return ""
}

// prefixCandidates — ID моделей, начинающиеся с arg (до 5 шт. для подсказки).
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
