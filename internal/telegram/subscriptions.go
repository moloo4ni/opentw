package telegram

import (
	"context"
	"errors"
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
	text, kb := RenderSubscribeRoot(b.models, draft, draftCopy(draft))
	_, err := b.api.SendMessageWithKeyboard(ctx, chat.ID, text, kb)
	return err
}

// draftCopy делает независимую копию черновика (для нового меню дифф = 0).
func draftCopy(d map[string]bool) map[string]bool {
	c := make(map[string]bool, len(d))
	for k, v := range d {
		c[k] = v
	}
	return c
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

// draftOf возвращает черновик чата; если меню не открывали в этом процессе
// (рестарт между открытием и «Сохранить»), черновик гидратируется из
// сохранённых подписок — иначе пустой черновик стёр бы их (P0).
//
// Инвариант: черновики читаются и пишутся только горутиной bot.Run —
// колбэки обрабатываются последовательно в цикле polling, — поэтому
// мутации вне b.mu безопасны.
func (b *Bot) draftOf(chatID int64) map[string]bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if d, ok := b.drafts[chatID]; ok {
		return d
	}
	subs, err := b.store.Subscriptions(chatID)
	if err != nil {
		b.log.Error("subscribe: subscriptions", "chat", chatID, "err", err)
	}
	d := map[string]bool{}
	for _, id := range subs {
		d[id] = true
	}
	b.drafts[chatID] = d
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
// Кнопки несут идентификаторы моделей/семейств (не индексы): устаревшие
// клавиатуры после смены конфигурации вежливо отвергаются.
func (b *Bot) handleCallback(ctx context.Context, cb *CallbackQuery) error {
	if cb.Message == nil {
		// колбэк без сообщения (отозванная/старая клавиатура): ни chat_id, ни
		// message_id — обработка испортила бы данные нулевым chat_id (7.2);
		// отвечаем тостом, чтобы у пользователя не висел спиннер
		return b.api.AnswerCallbackQuery(ctx, cb.ID, "Кнопка устарела, откройте /subscribe заново")
	}
	chat := cb.Message.Chat
	switch {
	case cb.Data == "save":
		return b.handleSubscribeSave(ctx, chat, cb)
	case cb.Data == "cancel":
		return b.handleSubscribeCancel(ctx, chat, cb)
	case cb.Data == "back":
		return b.showRoot(ctx, chat, cb)
	case cb.Data == "all":
		return b.handleMassToggle(ctx, chat, cb, "", true)
	case cb.Data == "none":
		return b.handleMassToggle(ctx, chat, cb, "", false)
	case strings.HasPrefix(cb.Data, "fa:"):
		fam, ok := resolveCallback(strings.TrimPrefix(cb.Data, "fa:"), FamiliesOf(b.models))
		if !ok {
			return b.api.AnswerCallbackQuery(ctx, cb.ID, "Кнопка устарела, откройте /subscribe заново")
		}
		return b.handleMassToggle(ctx, chat, cb, fam, true)
	case strings.HasPrefix(cb.Data, "fn:"):
		fam, ok := resolveCallback(strings.TrimPrefix(cb.Data, "fn:"), FamiliesOf(b.models))
		if !ok {
			return b.api.AnswerCallbackQuery(ctx, cb.ID, "Кнопка устарела, откройте /subscribe заново")
		}
		return b.handleMassToggle(ctx, chat, cb, fam, false)
	case strings.HasPrefix(cb.Data, "f:"):
		fam, ok := resolveCallback(strings.TrimPrefix(cb.Data, "f:"), FamiliesOf(b.models))
		if !ok {
			return b.api.AnswerCallbackQuery(ctx, cb.ID, "Кнопка устарела, откройте /subscribe заново")
		}
		return b.showFamily(ctx, chat, cb, fam)
	case strings.HasPrefix(cb.Data, "s:"):
		id, ok := resolveCallback(strings.TrimPrefix(cb.Data, "s:"), modelIDs(b.models))
		if !ok {
			return b.api.AnswerCallbackQuery(ctx, cb.ID, "Кнопка устарела, откройте /subscribe заново")
		}
		return b.handleSubscribeToggle(ctx, chat, cb, id)
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
	saved := b.savedMap(chat.ID)
	text, kb := RenderSubscribeRoot(b.models, draft, saved)
	return b.editMenu(ctx, chat, cb, text, kb)
}

// savedMap — сохранённые подписки чата для диффа черновика.
func (b *Bot) savedMap(chatID int64) map[string]bool {
	subs, err := b.store.Subscriptions(chatID)
	if err != nil {
		b.log.Error("subscribe: subscriptions", "chat", chatID, "err", err)
	}
	saved := map[string]bool{}
	for _, id := range subs {
		saved[id] = true
	}
	return saved
}

// editMenu перерисовывает сообщение меню; «message is not modified» —
// штатный случай (двойной клик по кнопке), а не ошибка; при настоящей
// неудаче даёт понятный тост (7.2).
func (b *Bot) editMenu(ctx context.Context, chat Chat, cb *CallbackQuery, text string, kb *InlineKeyboardMarkup) error {
	err := b.api.EditMessageText(ctx, chat.ID, cb.Message.MessageID, text, kb)
	var apiErr *ApiError
	if err == nil || (errors.As(err, &apiErr) && apiErr.Code == 400 && strings.Contains(strings.ToLower(apiErr.Description), errNotModified)) {
		return b.api.AnswerCallbackQuery(ctx, cb.ID, "")
	}
	b.log.Error("subscribe: menu edit", "chat", chat.ID, "err", err)
	return b.api.AnswerCallbackQuery(ctx, cb.ID, "Меню недоступно, откройте /subscribe заново")
}

// showFamily редактирует сообщение до экрана семейства.
func (b *Bot) showFamily(ctx context.Context, chat Chat, cb *CallbackQuery, family string) error {
	if ok, err := b.adminGate(ctx, chat, cb); err != nil {
		return err
	} else if !ok {
		return nil
	}
	draft := b.draftOf(chat.ID)
	saved := b.savedMap(chat.ID)
	text, kb := RenderSubscribeFamily(family, b.models, draft, saved)
	return b.editMenu(ctx, chat, cb, text, kb)
}

// handleSubscribeToggle переключает модель в черновике и показывает экран
// её семейства.
func (b *Bot) handleSubscribeToggle(ctx context.Context, chat Chat, cb *CallbackQuery, modelID string) error {
	if ok, err := b.adminGate(ctx, chat, cb); err != nil {
		return err
	} else if !ok {
		return nil
	}
	var family string
	for _, m := range b.models {
		if m.ID == modelID {
			family = m.Family
			break
		}
	}
	draft := b.draftOf(chat.ID)
	draft[modelID] = !draft[modelID]

	saved := b.savedMap(chat.ID)
	text, kb := RenderSubscribeFamily(family, b.models, draft, saved)
	return b.editMenu(ctx, chat, cb, text, kb)
}

// handleMassToggle включает/выключает в черновике все модели (family=""
// означает все семейства) и перерисовывает соответствующий экран.
func (b *Bot) handleMassToggle(ctx context.Context, chat Chat, cb *CallbackQuery, family string, on bool) error {
	if ok, err := b.adminGate(ctx, chat, cb); err != nil {
		return err
	} else if !ok {
		return nil
	}
	draft := b.draftOf(chat.ID)
	for _, m := range b.models {
		if family == "" || m.Family == family {
			draft[m.ID] = on
		}
	}
	saved := b.savedMap(chat.ID)
	if family == "" {
		text, kb := RenderSubscribeRoot(b.models, draft, saved)
		return b.editMenu(ctx, chat, cb, text, kb)
	}
	text, kb := RenderSubscribeFamily(family, b.models, draft, saved)
	return b.editMenu(ctx, chat, cb, text, kb)
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
	draft := b.draftOf(chat.ID) // гидратированный черновик: save без меню не стирает
	keep := make([]string, 0, len(b.models))
	for _, m := range b.models {
		if draft[m.ID] {
			keep = append(keep, m.ID)
		}
	}
	if err := b.store.ReplaceSubscriptions(chat.ID, keep); err != nil {
		b.log.Error("subscribe: save", "chat", chat.ID, "err", err)
		return b.api.AnswerCallbackQuery(ctx, cb.ID, "Не удалось сохранить, попробуйте ещё раз")
	}
	b.mu.Lock()
	delete(b.drafts, chat.ID)
	b.mu.Unlock()
	if err := b.api.AnswerCallbackQuery(ctx, cb.ID, "Сохранено"); err != nil {
		return err
	}
	return b.api.EditMessageText(ctx, chat.ID, cb.Message.MessageID, RenderSubscribedSaved(len(keep)), nil)
}
