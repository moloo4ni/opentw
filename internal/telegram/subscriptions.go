package telegram

import (
	"context"
	"strconv"
	"strings"
)

// handleSubscribeCommand открывает корневой экран настройки подписок
// (admin-only в группах; в каналах права проверяются на каждом callback).
func (b *Bot) handleSubscribeCommand(ctx context.Context, chat Chat, userID int64) error {
	// Пост /subscribe в канале пишет администратор канала (только они могут
	// публиковать); автора Bot API не сообщает. Панель публикуется, а права
	// каждого нажатия кнопок проверяются отдельным getChatMember по реальному
	// пользователю из callback. В группах автор известен и проверяется сразу.
	anonymousChannel := chat.Type == "channel" && userID == 0
	if !anonymousChannel {
		ok, err := b.userIsAdmin(ctx, chat, userID)
		if err != nil {
			return err
		}
		if !ok {
			_, err := b.api.SendMessage(ctx, chat.ID, "Изменять уведомления в этом чате могут только администраторы.")
			return err
		}
	}
	draft := b.loadDraft(ctx, chat.ID)
	text, kb := RenderSubscribeRoot(b.models, draft)
	_, err := b.api.SendMessageWithKeyboard(ctx, chat.ID, text, kb)
	return err
}

// loadDraft инициализирует черновик подписок чата из сохранённого состояния.
func (b *Bot) loadDraft(ctx context.Context, chatID int64) map[string]bool {
	subs, err := b.store.Subscriptions(chatID)
	if err != nil {
		b.log.Error("subscribe: subscriptions", "chat", chatID, "err", err)
	}
	draft := map[string]bool{}
	for _, id := range subs {
		draft[id] = true
	}
	b.mu.Lock()
	b.drafts[chatID] = draft
	b.mu.Unlock()
	return draft
}

func (b *Bot) draftOf(chatID int64) map[string]bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	d, ok := b.drafts[chatID]
	if !ok {
		d = map[string]bool{}
		b.drafts[chatID] = d
	}
	return d
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

// handleCallback обрабатывает нажатия inline-кнопок панели подписок:
// семейства, возврат, переключение моделей, сохранение и отмена.
func (b *Bot) handleCallback(ctx context.Context, cb *CallbackQuery) error {
	chat := Chat{ID: 0}
	if cb.Message != nil {
		chat = cb.Message.Chat
	}
	switch {
	case cb.Data == "save":
		return b.handleSubscribeSave(ctx, chat, cb)
	case cb.Data == "cancel":
		return b.handleSubscribeCancel(ctx, chat, cb)
	case cb.Data == "back":
		return b.showRoot(ctx, chat, cb)
	case strings.HasPrefix(cb.Data, "f:"):
		idx, err := strconv.Atoi(strings.TrimPrefix(cb.Data, "f:"))
		if err != nil {
			return b.api.AnswerCallbackQuery(ctx, cb.ID, "")
		}
		return b.showFamily(ctx, chat, cb, idx)
	case strings.HasPrefix(cb.Data, "s:"):
		idx, err := strconv.Atoi(strings.TrimPrefix(cb.Data, "s:"))
		if err != nil || idx < 0 || idx >= len(b.models) {
			return b.api.AnswerCallbackQuery(ctx, cb.ID, "Кнопка устарела, откройте /subscribe заново")
		}
		return b.handleSubscribeToggle(ctx, chat, cb, idx)
	default:
		return b.api.AnswerCallbackQuery(ctx, cb.ID, "")
	}
}

// adminGate проверяет права нажавшего; в группах и каналах — только админы.
func (b *Bot) adminGate(ctx context.Context, chat Chat, cb *CallbackQuery) (bool, error) {
	admin, err := b.userIsAdmin(ctx, chat, cb.From.ID)
	if err != nil {
		return false, err
	}
	if !admin {
		if err := b.api.AnswerCallbackQuery(ctx, cb.ID, "Только администраторы могут менять уведомления"); err != nil {
			return false, err
		}
		return false, nil
	}
	return true, nil
}

// showRoot редактирует сообщение до корневого экрана.
func (b *Bot) showRoot(ctx context.Context, chat Chat, cb *CallbackQuery) error {
	if ok, err := b.adminGate(ctx, chat, cb); err != nil {
		return err
	} else if !ok {
		return nil
	}
	draft := b.draftOf(chat.ID)
	text, kb := RenderSubscribeRoot(b.models, draft)
	if err := b.api.EditMessageText(ctx, chat.ID, cb.Message.MessageID, text, kb); err != nil {
		b.log.Error("subscribe: root edit", "chat", chat.ID, "err", err)
	}
	return b.api.AnswerCallbackQuery(ctx, cb.ID, "")
}

// showFamily редактирует сообщение до экрана семейства с индексом famIdx.
func (b *Bot) showFamily(ctx context.Context, chat Chat, cb *CallbackQuery, famIdx int) error {
	if ok, err := b.adminGate(ctx, chat, cb); err != nil {
		return err
	} else if !ok {
		return nil
	}
	families := FamiliesOf(b.models)
	if famIdx < 0 || famIdx >= len(families) {
		return b.api.AnswerCallbackQuery(ctx, cb.ID, "Кнопка устарела, откройте /subscribe заново")
	}
	draft := b.draftOf(chat.ID)
	text, kb := RenderSubscribeFamily(families[famIdx], b.models, draft)
	if err := b.api.EditMessageText(ctx, chat.ID, cb.Message.MessageID, text, kb); err != nil {
		b.log.Error("subscribe: family edit", "chat", chat.ID, "err", err)
	}
	return b.api.AnswerCallbackQuery(ctx, cb.ID, "")
}

// handleSubscribeToggle переключает модель в черновике и показывает экран
// её семейства.
func (b *Bot) handleSubscribeToggle(ctx context.Context, chat Chat, cb *CallbackQuery, idx int) error {
	if ok, err := b.adminGate(ctx, chat, cb); err != nil {
		return err
	} else if !ok {
		return nil
	}
	modelID := b.models[idx].ID
	draft := b.draftOf(chat.ID)
	draft[modelID] = !draft[modelID]

	text, kb := RenderSubscribeFamily(b.models[idx].Family, b.models, draft)
	if err := b.api.EditMessageText(ctx, chat.ID, cb.Message.MessageID, text, kb); err != nil {
		b.log.Error("subscribe: toggle edit", "chat", chat.ID, "err", err)
	}
	return b.api.AnswerCallbackQuery(ctx, cb.ID, "")
}

// handleSubscribeCancel удаляет сообщение с меню; изменения не сохраняются.
// Если удалить нельзя (например, нет права в канале) — клавиатура убирается
// с текстовым подтверждением.
func (b *Bot) handleSubscribeCancel(ctx context.Context, chat Chat, cb *CallbackQuery) error {
	if ok, err := b.adminGate(ctx, chat, cb); err != nil {
		return err
	} else if !ok {
		return nil
	}
	b.mu.Lock()
	delete(b.drafts, chat.ID)
	b.mu.Unlock()
	if err := b.api.AnswerCallbackQuery(ctx, cb.ID, "Отменено"); err != nil {
		return err
	}
	if err := b.api.DeleteMessage(ctx, chat.ID, cb.Message.MessageID); err != nil {
		b.log.Warn("subscribe: delete failed, hiding keyboard", "chat", chat.ID, "err", err)
		return b.api.EditMessageText(ctx, chat.ID, cb.Message.MessageID,
			"Настройка уведомлений закрыта. Изменения не сохранены.", nil)
	}
	return nil
}

func (b *Bot) handleSubscribeSave(ctx context.Context, chat Chat, cb *CallbackQuery) error {
	if ok, err := b.adminGate(ctx, chat, cb); err != nil {
		return err
	} else if !ok {
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
