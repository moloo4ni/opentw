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

// chatUnavailable recognizes Telegram errors meaning the bot can no longer
// interact with the chat: blocked, kicked from a group/channel, chat deleted
// or user deactivated. Temporary failures (network, 429, 5xx, 401 — the
// token's own problem) do not match.
func chatUnavailable(err error) bool {
	var apiErr *ApiError
	if !errors.As(err, &apiErr) {
		return false
	}
	desc := strings.ToLower(apiErr.Description)
	switch apiErr.Code {
	case 403:
		return strings.Contains(desc, "blocked") ||
			strings.Contains(desc, "kicked") ||
			strings.Contains(desc, "not a member") ||
			strings.Contains(desc, "deactivated")
	case 400:
		return strings.Contains(desc, errChatNotFound) ||
			strings.Contains(desc, errKicked) ||
			strings.Contains(desc, "bot is not a member")
	}
	return false
}

// panelText builds the current panel text (model order follows the config).
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
	return RenderStatusPanel(models, checks, updated, monitor.NextTick(b.interval(), updated))
}

func (b *Bot) interval() time.Duration {
	if b.tick > 0 {
		return b.tick
	}
	return 15 * time.Minute
}

// ensurePanel: a chat's /status always creates a fresh panel: the new
// message is pinned, the old one is unpinned after the new pin succeeds,
// and the binding is updated only after a successful send.
func (b *Bot) ensurePanel(ctx context.Context, chatID int64) error {
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

	msg, err := b.api.SendRichMessage(ctx, chatID, text)
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
		// the old pin was not touched — it stays in force; try to restore it
		// in case Telegram already dropped it
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

// OnCycle is called by the monitor at the end of a cycle: it prepares the
// notification events (no DB writes — the runner commits them atomically
// with the states) and flags the panels dirty — the delivery worker (network
// calls) handles them. An error means events are unavailable → the whole
// cycle is not committed.
func (b *Bot) OnCycle(_ context.Context, res monitor.CycleResult) ([]storage.OutboxEvent, error) {
	events, err := b.prepareTransitions(res)
	if err != nil {
		return nil, err
	}
	if len(res.Checks) > 0 || len(res.Transitions) > 0 {
		b.panelDirty.Store(true)
	}
	b.wakeDelivery()
	return events, nil
}

// syncPanel edits the chat's pinned panel; when the message is gone it
// creates a fresh panel.
func (b *Bot) syncPanel(ctx context.Context, chatID int64, text string) {
	if b.cachedPanel(chatID) == text {
		return // text unchanged — no needless edit
	}
	sm, err := b.store.StatusMessage(chatID)
	if err != nil || sm == nil {
		return
	}
	err = b.api.EditRichMessageText(ctx, chatID, sm.MessageID, text)
	if err == nil {
		b.cachePanel(chatID, text)
		return
	}
	var apiErr *ApiError
	if errors.As(err, &apiErr) {
		desc := strings.ToLower(apiErr.Description)
		switch {
		case apiErr.Code == 400 && strings.Contains(desc, errNotModified):
			b.cachePanel(chatID, text)
			return
		case apiErr.Code == 400 && (strings.Contains(desc, errEditNotFound) || strings.Contains(desc, "message to delete not found")):
			// panel deleted: recreate and pin
			b.log.Info("panel: deleted, recreating", "chat", chatID)
			if postErr := b.postPanelCycle(ctx, chatID, text); postErr != nil {
				b.log.Error("panel: recreate failed", "chat", chatID, "err", postErr)
			}
			return
		case chatUnavailable(err):
			// the bot was removed from the chat — forget the binding to avoid noise
			if delErr := b.store.DeleteStatusMessage(chatID); delErr != nil {
				b.log.Error("panel: drop binding failed", "chat", chatID, "err", delErr)
			}
			return
		}
	}
	b.log.Error("panel: edit failed", "chat", chatID, "err", err)
}

// postPanelCycle recreates the panel during a cycle (message deleted).
func (b *Bot) postPanelCycle(ctx context.Context, chatID int64, text string) error {
	msg, err := b.api.SendRichMessage(ctx, chatID, text)
	if err != nil {
		return err
	}
	b.cachePanel(chatID, text)
	if pinErr := b.api.PinChatMessage(ctx, chatID, msg.MessageID); pinErr != nil {
		b.warnPinOnce(ctx, chatID, pinErr)
	}
	return b.store.SetStatusMessage(chatID, msg.MessageID)
}
