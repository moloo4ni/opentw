package telegram

import (
	"context"
	"errors"
	"strings"
	"time"

	"opentw/internal/monitor"
	"opentw/internal/storage"
)

var (
	errNotModified  = "message is not modified"
	errEditNotFound = "message to edit not found"
	errChatNotFound = "chat not found"
	errKicked       = "bot was kicked"
)

// panelText собирает текущий текст панели (порядок моделей — из конфига).
func (b *Bot) panelText(checks map[string]storage.Check) string {
	models := b.models
	updated := time.Now()
	latest := time.Time{}
	for _, c := range checks {
		if c.At.After(latest) {
			latest = c.At
		}
	}
	if !latest.IsZero() {
		updated = latest
	}
	return RenderStatusPanel(models, checks, updated, updated.Add(b.interval()))
}

func (b *Bot) interval() time.Duration {
	if b.tick > 0 {
		return b.tick
	}
	return 15 * time.Minute
}

// ensurePanel: /status из чата — всегда создаёт новую панель по правилам ТЗ §7:
// новое сообщение закрепляется, прежнее открепляется после успешного закрепления
// нового, привязка обновляется только после успешной отправки.
func (b *Bot) ensurePanel(ctx context.Context, chatID int64, _ bool) error {
	checks, err := b.store.LastChecks()
	if err != nil {
		b.log.Error("panel: last checks", "err", err)
		checks = nil
	}
	return b.postPanel(ctx, chatID, checks)
}

func (b *Bot) postPanel(ctx context.Context, chatID int64, checks map[string]storage.Check) error {
	text := b.panelText(checks)
	old, err := b.store.StatusMessage(chatID)
	if err != nil {
		return err
	}

	msg, err := b.api.SendMessage(ctx, chatID, text)
	if err != nil {
		b.log.Error("panel: send", "chat", chatID, "err", err)
		return err
	}
	b.cachePanel(chatID, text)

	pinErr := b.api.PinChatMessage(ctx, chatID, msg.MessageID)
	switch {
	case pinErr == nil:
		b.clearPinWarn(chatID)
		if old != nil {
			if err := b.api.UnpinChatMessage(ctx, chatID, old.MessageID); err != nil {
				b.log.Info("panel: old unpin failed", "chat", chatID, "err", err)
			}
		}
	case pinErr != nil && old != nil:
		// прежнее закрепление не трогали — оно остаётся в силе; попытка
		// восстановить его на случай, если Telegram его уже сбросил
		if err := b.api.PinChatMessage(ctx, chatID, old.MessageID); err != nil {
			b.log.Info("panel: old re-pin failed", "chat", chatID, "err", err)
		}
		b.warnPinOnce(ctx, chatID, pinErr)
	default:
		b.warnPinOnce(ctx, chatID, pinErr)
	}

	if err := b.store.SetStatusMessage(chatID, msg.MessageID); err != nil {
		return err
	}
	return nil
}

func (b *Bot) warnPinOnce(ctx context.Context, chatID int64, pinErr error) {
	b.mu.Lock()
	warned := b.pinWarn[chatID]
	b.pinWarn[chatID] = true
	b.mu.Unlock()
	if warned {
		return
	}
	msg := "Не удалось закрепить статусную панель: бот должен быть администратором чата с правом закрепления сообщений. Панель будет обновляться без закрепления."
	if _, err := b.api.SendMessage(ctx, chatID, msg); err != nil {
		b.log.Warn("panel: pin warn send", "chat", chatID, "err", err)
	}
	b.log.Warn("panel: pin failed", "chat", chatID, "err", pinErr)
}

func (b *Bot) clearPinWarn(chatID int64) {
	b.mu.Lock()
	delete(b.pinWarn, chatID)
	b.mu.Unlock()
}

func (b *Bot) cachePanel(chatID int64, text string) {
	b.mu.Lock()
	b.lastText[chatID] = text
	b.mu.Unlock()
}

func (b *Bot) cachedPanel(chatID int64) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lastText[chatID]
}

// OnCycle вызывается монитором после каждого цикла: обновляет все панели и
// рассылает события (Task 9).
func (b *Bot) OnCycle(ctx context.Context, res monitor.CycleResult) {
	text := RenderStatusPanel(b.models, res.Checks, res.CycleAt, res.NextAt)

	chats, err := b.store.StatusChats()
	if err != nil {
		b.log.Error("panel: status chats", "err", err)
	}
	for _, chatID := range chats {
		b.syncPanel(ctx, chatID, text)
	}
	b.notifyTransitions(ctx, res)
}

// syncPanel редактирует закреплённую панель чата; при удалении сообщения —
// создаёт новую панель.
func (b *Bot) syncPanel(ctx context.Context, chatID int64, text string) {
	if b.cachedPanel(chatID) == text {
		return // текст не изменился — лишнее редактирование не нужно
	}
	sm, err := b.store.StatusMessage(chatID)
	if err != nil || sm == nil {
		return
	}
	err = b.api.EditMessageText(ctx, chatID, sm.MessageID, text, nil)
	if err == nil {
		b.cachePanel(chatID, text)
		return
	}
	var apiErr *ApiError
	if errors.As(err, &apiErr) && apiErr.Code == 400 {
		desc := strings.ToLower(apiErr.Description)
		switch {
		case strings.Contains(desc, errNotModified):
			b.cachePanel(chatID, text)
			return
		case strings.Contains(desc, errEditNotFound) || strings.Contains(desc, "message to delete not found"):
			// панель удалена: пересоздаём и закрепляем
			b.log.Info("panel: deleted, recreating", "chat", chatID)
			if postErr := b.postPanelCycle(ctx, chatID, text); postErr != nil {
				b.log.Error("panel: recreate failed", "chat", chatID, "err", postErr)
			}
			return
		case strings.Contains(desc, errChatNotFound) || strings.Contains(desc, errKicked) || strings.Contains(desc, "bot is not a member"):
			// бот удалён из чата — забываем привязку, чтобы не спамить
			if delErr := b.store.DeleteStatusMessage(chatID); delErr != nil {
				b.log.Error("panel: drop binding failed", "chat", chatID, "err", delErr)
			}
			return
		}
	}
	b.log.Error("panel: edit failed", "chat", chatID, "err", err)
}

// postPanelCycle пересоздаёт панель во время цикла (сообщение удалено).
func (b *Bot) postPanelCycle(ctx context.Context, chatID int64, text string) error {
	msg, err := b.api.SendMessage(ctx, chatID, text)
	if err != nil {
		return err
	}
	b.cachePanel(chatID, text)
	if pinErr := b.api.PinChatMessage(ctx, chatID, msg.MessageID); pinErr != nil {
		b.warnPinOnce(ctx, chatID, pinErr)
	}
	return b.store.SetStatusMessage(chatID, msg.MessageID)
}
