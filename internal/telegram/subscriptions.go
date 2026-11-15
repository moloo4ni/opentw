package telegram

import (
	"context"
	"strconv"
	"strings"
)

// handleSubscribeCommand открывает панель настройки подписок (admin-only в группах/каналах).
func (b *Bot) handleSubscribeCommand(ctx context.Context, chat Chat, userID int64) error {
	ok, err := b.userIsAdmin(ctx, chat, userID)
	if err != nil {
		return err
	}
	if !ok {
		_, err := b.api.SendMessage(ctx, chat.ID, "Изменять уведомления в этом чате могут только администраторы.")
		return err
	}
	subs, err := b.store.Subscriptions(chat.ID)
	if err != nil {
		return err
	}
	draft := map[string]bool{}
	for _, id := range subs {
		draft[id] = true
	}
	b.mu.Lock()
	b.drafts[chat.ID] = draft
	b.mu.Unlock()

	models, err := b.store.Models()
	if err != nil {
		return err
	}
	text, kb := RenderSubscribePanel(models, draft)
	_, err = b.api.SendMessageWithKeyboard(ctx, chat.ID, text, kb)
	return err
}

// userIsAdmin: личные чаты — всегда владелец; группы/каналы — getChatMember.
func (b *Bot) userIsAdmin(ctx context.Context, chat Chat, userID int64) (bool, error) {
	if chat.Type == "private" {
		return true, nil
	}
	if userID == 0 {
		return false, nil // анонимный пост канала — не доверяем
	}
	member, err := b.api.GetChatMember(ctx, chat.ID, userID)
	if err != nil {
		b.log.Warn("subscribe: getChatMember failed", "chat", chat.ID, "user", userID, "err", err)
		return false, err
	}
	return member.IsAdmin(), nil
}

// handleCallback обрабатывает нажатия inline-кнопок панели подписок.
func (b *Bot) handleCallback(ctx context.Context, cb *CallbackQuery) error {
	chat := Chat{ID: 0}
	if cb.Message != nil {
		chat = cb.Message.Chat
	}
	switch {
	case cb.Data == "save":
		return b.handleSubscribeSave(ctx, chat, cb)
	case strings.HasPrefix(cb.Data, "s:"):
		idx, err := strconv.Atoi(strings.TrimPrefix(cb.Data, "s:"))
		if err != nil || idx < 0 || idx >= len(b.models) {
			return b.api.AnswerCallbackQuery(ctx, cb.ID, "Кнопка устарела, откройте /subscribe заново")
		}
		return b.handleSubscribeToggle(ctx, chat, cb, b.models[idx].ID)
	default:
		return b.api.AnswerCallbackQuery(ctx, cb.ID, "")
	}
}

func (b *Bot) ensureDraft(chatID int64) map[string]bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	d, ok := b.drafts[chatID]
	if !ok {
		d = map[string]bool{}
		b.drafts[chatID] = d
	}
	return d
}

func (b *Bot) handleSubscribeToggle(ctx context.Context, chat Chat, cb *CallbackQuery, modelID string) error {
	admin, err := b.userIsAdmin(ctx, chat, cb.From.ID)
	if err != nil {
		return err
	}
	if !admin {
		err := b.api.AnswerCallbackQuery(ctx, cb.ID, "Только администраторы могут менять уведомления")
		if err != nil {
			return err
		}
		return nil
	}
	draft := b.ensureDraft(chat.ID)
	draft[modelID] = !draft[modelID]

	models, err := b.store.Models()
	if err != nil {
		return err
	}
	text, kb := RenderSubscribePanel(models, draft)
	if err := b.api.EditMessageText(ctx, chat.ID, cb.Message.MessageID, text, kb); err != nil {
		b.log.Error("subscribe: toggle edit", "chat", chat.ID, "err", err)
	}
	return b.api.AnswerCallbackQuery(ctx, cb.ID, "")
}

func (b *Bot) handleSubscribeSave(ctx context.Context, chat Chat, cb *CallbackQuery) error {
	admin, err := b.userIsAdmin(ctx, chat, cb.From.ID)
	if err != nil {
		return err
	}
	if !admin {
		err := b.api.AnswerCallbackQuery(ctx, cb.ID, "Только администраторы могут менять уведомления")
		if err != nil {
			return err
		}
		return nil
	}
	b.mu.Lock()
	draft := b.drafts[chat.ID]
	delete(b.drafts, chat.ID)
	b.mu.Unlock()
	if draft == nil {
		draft = map[string]bool{}
	}

	saved, err := b.store.Subscriptions(chat.ID)
	if err != nil {
		return err
	}
	current := map[string]bool{}
	for _, id := range saved {
		current[id] = true
	}
	for _, m := range b.models {
		want := draft[m.ID]
		has := current[m.ID]
		switch {
		case want && !has:
			if err := b.store.SetSubscription(chat.ID, m.ID, true); err != nil {
				return err
			}
		case !want && has:
			if err := b.store.SetSubscription(chat.ID, m.ID, false); err != nil {
				return err
			}
		}
	}
	count := 0
	for _, m := range b.models {
		if draft[m.ID] {
			count++
		}
	}
	if err := b.api.AnswerCallbackQuery(ctx, cb.ID, "Сохранено"); err != nil {
		return err
	}
	return b.api.EditMessageText(ctx, chat.ID, cb.Message.MessageID, RenderSubscribedSaved(count), nil)
}
