package telegram

import (
	"context"
	"errors"
	"strings"
)

// handleSubscribeCommand opens the subscription setup root screen
// (admin-only in groups; in channels each callback is permission-checked).
func (b *Bot) handleSubscribeCommand(ctx context.Context, chat Chat, userID int64) error {
	// A channel /subscribe post is written by a channel admin (only they can
	// publish); the Bot API does not report the author. The panel is
	// published, and every button press is checked with a separate
	// getChatMember against the real user from the callback. In groups the
	// author is known and checked immediately.
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

// draftCopy makes an independent copy of the draft (a fresh menu has diff 0).
func draftCopy(d map[string]bool) map[string]bool {
	c := make(map[string]bool, len(d))
	for k, v := range d {
		c[k] = v
	}
	return c
}

// loadDraft initializes the chat's subscription draft from the saved state.
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

// draftOf returns the chat's draft; if the menu was not opened in this
// process (a restart between opening and "Save"), the draft is hydrated
// from the saved subscriptions — an empty draft would otherwise wipe them.
//
// Invariant: drafts are read and written only by the bot.Run goroutine —
// callbacks are handled sequentially in the polling loop — so mutations
// outside b.mu are safe.
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

// userIsAdmin: private chats — always the owner; groups/channels — getChatMember.
func (b *Bot) userIsAdmin(ctx context.Context, chat Chat, userID int64) (bool, error) {
	if chat.Type == "private" {
		return true, nil
	}
	if userID == 0 {
		return false, nil // an anonymous channel post — do not trust
	}
	member, err := b.api.GetChatMember(ctx, chat.ID, userID)
	if err != nil {
		b.log.Warn("subscribe: getChatMember failed", "chat", chat.ID, "user", userID, "err", err)
		return false, err
	}
	return member.IsAdmin(), nil
}

// handleCallback handles presses of the subscribe menu's inline buttons:
// families, back, model toggles, save and cancel. Buttons carry
// model/family identifiers (not indices): stale keyboards after a config
// change are rejected politely.
func (b *Bot) handleCallback(ctx context.Context, cb *CallbackQuery) error {
	if strings.HasPrefix(cb.Data, "st:") {
		// panel/notification error buttons come before the subscribe menu:
		// they never collide with f:/fa:/fn:/s:/save/cancel/back/all/none and
		// always answer themselves
		return b.handleStatusCallback(ctx, cb)
	}
	if cb.Message == nil {
		// a callback without a message (revoked/stale keyboard): neither
		// chat_id nor message_id — handling would corrupt data with a zero
		// chat_id; answer with a toast so the user is not left with a spinner
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

// adminGate checks the presser's rights; in groups and channels — admins only.
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

// showRoot edits the message back to the root screen.
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

// savedMap is the chat's saved subscriptions, for the draft diff.
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

// editMenu redraws the menu message; "message is not modified" is normal
// (a double click), not an error; a real failure yields a clear toast.
func (b *Bot) editMenu(ctx context.Context, chat Chat, cb *CallbackQuery, text string, kb *InlineKeyboardMarkup) error {
	err := b.api.EditMessageText(ctx, chat.ID, cb.Message.MessageID, text, kb)
	var apiErr *ApiError
	if err == nil || (errors.As(err, &apiErr) && apiErr.Code == 400 && strings.Contains(strings.ToLower(apiErr.Description), errNotModified)) {
		return b.api.AnswerCallbackQuery(ctx, cb.ID, "")
	}
	b.log.Error("subscribe: menu edit", "chat", chat.ID, "err", err)
	return b.api.AnswerCallbackQuery(ctx, cb.ID, "Меню недоступно, откройте /subscribe заново")
}

// showFamily edits the message to the family screen.
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

// handleSubscribeToggle flips one model in the draft and shows its family
// screen.
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

// handleMassToggle turns all models on/off in the draft (family="" means
// every family) and redraws the matching screen.
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

// handleSubscribeCancel deletes the menu message; changes are not saved.
// When deletion is impossible (e.g. no channel right) the keyboard is
// removed with a text confirmation.
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
	draft := b.draftOf(chat.ID) // hydrated draft: save without a menu wipes nothing
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
