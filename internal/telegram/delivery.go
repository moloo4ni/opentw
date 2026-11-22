package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"opentw/internal/status"
	"opentw/internal/storage"
)

const (
	outboxBatch      = 50 // rows per worker pass
	deliveryInterval = 30 * time.Second
	outboxRetention  = 7 * 24 * time.Hour // delivered rows are kept a week
	outboxTTL        = 24 * time.Hour     // undelivered rows older than a day are dropped
	cleanupEvery     = time.Hour
	floodFloor       = time.Second // minimum batch pause after a 429 without retry_after
	backoffBase      = 30 * time.Second
	backoffCap       = 15 * time.Minute
)

// RunDelivery is the delivery worker: panels and notifications leave
// asynchronously from the monitoring cycle and survive Telegram outages
// and restarts.
func (b *Bot) RunDelivery(ctx context.Context) error {
	ticker := time.NewTicker(deliveryInterval)
	defer ticker.Stop()
	b.deliveryTick(ctx) // immediate attempt: startup may find unsent rows
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-b.wake:
		case <-ticker.C:
		}
		b.deliveryTick(ctx)
	}
}

// deliveryTick is one pass: notifications (news first), then panels,
// then periodic outbox cleanup.
func (b *Bot) deliveryTick(ctx context.Context) {
	b.processOutbox(ctx)
	b.syncPanelsOnce(ctx)
	b.cleanupOutboxOnce()
}

// processOutbox delivers accumulated events by reconciling freshness.
// Per-chat FIFO is preserved: PendingOutboxDue returns only each chat's due
// head row; processing collapses the whole chat into one message built from
// the CURRENT states. A 429 pauses the whole batch: the bot's flood limit is
// global. Temporary errors back off progressively. Send-done-commit-failed
// yields at-least-once: the row may re-deliver — a documented trade-off, the
// queue never freezes.
func (b *Bot) processOutbox(ctx context.Context) {
	if until := b.floodUntil.Load(); until != 0 && time.Now().Unix() < until {
		return
	}
	failedChat := map[int64]bool{} // a failed chat is not overtaken by newer rows
	budget := outboxBatch
	// after the head is delivered the chat's next row becomes its head and
	// leaves within the same tick (progress re-queries the queue); the pass
	// limit guards against an eternal loop on a broken MarkOutboxSent
	for pass := 0; pass < outboxBatch; pass++ {
		rows, err := b.store.PendingOutboxDue(outboxBatch)
		if err != nil {
			b.log.Error("delivery: pending outbox", "err", err)
			return
		}
		if len(rows) == 0 {
			return
		}
		progressed := false
		seen := map[int64]bool{} // one chat — one collapse per pass
		for _, row := range rows {
			if ctx.Err() != nil { // shutdown inflates no attempts and makes no noise
				return
			}
			if failedChat[row.ChatID] || seen[row.ChatID] {
				continue
			}
			seen[row.ChatID] = true
			switch b.deliverChat(ctx, row.ChatID) {
			case deliverDone:
				progressed = true
			case deliverFlood:
				return
			case deliverFail:
				failedChat[row.ChatID] = true
			}
			budget--
			if budget <= 0 {
				return
			}
		}
		if !progressed {
			return
		}
	}
}

// deliverState is the outcome of one chat's collapse.
type deliverState int

const (
	deliverDone deliverState = iota
	deliverFlood
	deliverFail
)

// deliverChat handles all of a chat's pending rows with one freshness
// reconciliation: rows are grouped by model and decided by the CURRENT
// state (models.last_status) and chat_notified. cur Unknown → silence;
// cur == notified → silence; otherwise one line for cur. Lines follow the
// config order, in a single rich message, without headers.
//
// A row is marked sent ONLY when all its models are observed definitively
// (or no longer observed: unsubscribed, removed from config). Rows whose
// model is Unknown stay pending: an uncertain check is not a delivery and
// must not destroy the evidence of an undelivered change. Rows of one model
// are resolved together (the fact follows the model's current status — a
// recovery cannot overtake an undelivered fall); other models' rows do not
// wait for an uncertain one (per-model isolation within a chat).
func (b *Bot) deliverChat(ctx context.Context, chatID int64) deliverState {
	rows, err := b.store.PendingOutboxChat(chatID)
	if err != nil {
		b.log.Error("delivery: backlog rows", "chat", chatID, "err", err)
		return deliverFail
	}
	if len(rows) == 0 {
		return deliverDone
	}
	// Exact IDs of the rows read: ApplyCycle may add a new event to the chat
	// during delivery — it was not part of the decision and must stay pending.
	type pendingRow struct {
		row    storage.OutboxRow
		models map[string]bool
	}
	decoded := make([]pendingRow, 0, len(rows))
	modelsPending := map[string]bool{}
	for _, row := range rows {
		var p deliveryPayload
		if err := json.Unmarshal(row.Payload, &p); err != nil {
			b.log.Error("delivery: decode payload", "chat", row.ChatID, "err", err)
			if err := b.store.MarkOutboxSent(row.ID); err != nil {
				b.log.Error("delivery: drop broken row", "err", err)
			}
			continue
		}
		pr := pendingRow{row: row, models: map[string]bool{}}
		for _, tr := range p.Trs {
			pr.models[tr.ModelID] = true
			modelsPending[tr.ModelID] = true
		}
		decoded = append(decoded, pr)
	}
	if len(decoded) == 0 {
		return deliverDone
	}

	models, err := b.store.Models()
	if err != nil {
		b.log.Error("delivery: models", "chat", chatID, "err", err)
		return b.deferHead(chatID, decoded[0].row)
	}
	cur := map[string]status.Status{}
	for _, m := range models {
		cur[m.ID] = m.LastStatus
	}
	lastChecks, err := b.store.LastChecks()
	if err != nil {
		b.log.Error("delivery: last checks", "chat", chatID, "err", err)
		return b.deferHead(chatID, decoded[0].row)
	}
	// the subscription may be gone between the event and delivery: drop the row
	subs, err := b.store.Subscriptions(chatID)
	if err != nil {
		b.log.Error("delivery: subscriptions", "chat", chatID, "err", err)
		return b.deferHead(chatID, decoded[0].row)
	}
	subscribed := map[string]bool{}
	for _, id := range subs {
		subscribed[id] = true
	}

	var lines []string
	notifiedSet := map[string]status.Status{}
	for _, m := range b.models { // line order follows the config order
		if !modelsPending[m.ID] || !subscribed[m.ID] {
			continue
		}
		st, exists := cur[m.ID]
		if !exists {
			continue // model removed from config: nothing to observe
		}
		if st != status.Operational && st != status.Down {
			continue // cur Unknown (or stale): silence, chat_notified untouched
		}
		notified, ok, err := b.store.ChatNotifiedState(chatID, m.ID)
		if err != nil {
			b.log.Error("delivery: chat notified state", "chat", chatID, "model", m.ID, "err", err)
			return b.deferHead(chatID, decoded[0].row)
		}
		if ok && notified == st {
			continue // the user already knows: no lines, mark sent
		}
		if st == status.Down {
			c := lastChecks[m.ID]
			if _, ok := lastChecks[m.ID]; !ok {
				// a model without a check row (theoretical): sign from the model
				c = storage.Check{ModelID: m.ID, Status: st, ErrorType: m.ErrorType, ErrorMessage: m.ErrorMessage}
			}
			lines = append(lines, downLine(m.ID, c))
		} else {
			lines = append(lines, recoveredLine(m.ID))
		}
		notifiedSet[m.ID] = st
	}

	// A row is resolved (marked sent) only when ALL its models are observed
	// definitively — an Unknown model keeps its rows pending: uncertainty
	// must not swallow the only evidence of an undelivered change. The fact
	// follows the model's current status, so one model's rows resolve together.
	var ids []int64
	var stuckHead *storage.OutboxRow
	for i := range decoded {
		resolved := true
		for mid := range decoded[i].models {
			if !subscribed[mid] {
				continue // unsubscribed: drop the row
			}
			st, exists := cur[mid]
			if !exists {
				continue // model left the config: nothing to observe
			}
			if st != status.Operational && st != status.Down {
				resolved = false // cur Unknown: evidence of an undelivered change
			}
		}
		if resolved {
			ids = append(ids, decoded[i].row.ID)
		} else if stuckHead == nil {
			stuckHead = &decoded[i].row
		}
	}

	if len(lines) > 0 {
		if _, err := b.api.SendRichMessage(ctx, chatID, strings.Join(lines, "")); err != nil {
			if chatUnavailable(err) {
				b.dropDeadChat(chatID)
				return deliverDone
			}
			b.log.Error("delivery: send", "chat", chatID, "err", err)
			if after, flood := b.pauseAfter(err); flood {
				_ = b.store.DeferOutboxAttempt(decoded[0].row.ID, after)
				return deliverFlood
			}
			// Temporary error — growing backoff (30s/1m/2m… up to 15m):
			// rows live until delivered or TTL; there is no "after N attempts" drop
			return b.deferHead(chatID, decoded[0].row)
		}
	}
	// The order is mandatory: first Telegram confirmed the send (above),
	// then ONE commit records the notified states and the row marks.
	// chat_notified is not written before Telegram answers — otherwise a
	// delivery failure would count the event as sent and lose it. A commit
	// failure does not acknowledge delivery: the rows stay pending
	// (at-least-once — a rare duplicate instead of a loss), the pause is a
	// backoff without hammering the DB.
	if len(ids) > 0 || len(notifiedSet) > 0 {
		if err := b.store.CommitDelivery(chatID, notifiedSet, ids); err != nil {
			b.log.Error("delivery: commit", "chat", chatID, "err", err)
			return b.deferHead(chatID, decoded[0].row)
		}
	}
	if stuckHead != nil {
		// The Unknown-state evidence stays pending; the head gets a short
		// pause WITHOUT an attempt counter (does not distort the backoff of
		// real failures, keeps the chat out of batch slots and stops re-running
		// the collapse every tick).
		if err := b.store.DeferOutboxRetry(stuckHead.ID, deliveryInterval); err != nil {
			b.log.Error("delivery: defer stuck row", "chat", chatID, "err", err)
		}
		if len(ids) == 0 && len(notifiedSet) == 0 {
			return deliverFail // nothing to commit: leave the chat alone for this tick
		}
	}
	return deliverDone
}

// deferHead defers the chat's head row and reports failure.
func (b *Bot) deferHead(chatID int64, head storage.OutboxRow) deliverState {
	if err := b.store.DeferOutboxAttempt(head.ID, deferBackoff(head.Attempts)); err != nil {
		b.log.Error("delivery: defer attempt", "chat", chatID, "err", err)
	}
	return deliverFail
}

// deferBackoff is the growing pause between retries of a temporary delivery
// error (not 429: that has its own retry_after): 30s, 1m, 2m… up to 15m.
// attempts is the row's number of failed attempts so far.
func deferBackoff(attempts int) time.Duration {
	d := backoffBase << attempts
	if d <= 0 || d > backoffCap { // shift overflow — cap
		d = backoffCap
	}
	return d
}

// pauseAfter pauses the whole batch until retry_after on a 429 (the bot's
// flood limit is global). ok=false for other errors.
func (b *Bot) pauseAfter(err error) (time.Duration, bool) {
	var apiErr *ApiError
	if !errors.As(err, &apiErr) || apiErr.Code != 429 {
		return 0, false
	}
	after := time.Duration(apiErr.RetryAfter * float64(time.Second))
	if after < floodFloor {
		after = floodFloor
	}
	b.floodUntil.Store(time.Now().Add(after).Unix() + 1)
	return after, true
}

// dropDeadChat removes a chat the bot can no longer write to (blocked,
// kicked, deleted): its subscriptions, queue, panel binding and notified
// states are dead. The log is mandatory — otherwise the owner never learns
// where the notifications went. In-memory caches too.
func (b *Bot) dropDeadChat(chatID int64) {
	if err := b.store.DeleteSubscriptions(chatID); err != nil {
		b.log.Error("delivery: drop dead chat subscriptions", "chat", chatID, "err", err)
	}
	n, err := b.store.DeleteOutboxChat(chatID)
	if err != nil {
		b.log.Error("delivery: drop dead chat rows", "chat", chatID, "err", err)
		return
	}
	if err := b.store.DeleteStatusMessage(chatID); err != nil {
		b.log.Error("delivery: drop dead chat panel binding", "chat", chatID, "err", err)
	}
	if err := b.store.DeleteChatNotifiedState(chatID); err != nil {
		b.log.Error("delivery: drop dead chat notified state", "chat", chatID, "err", err)
	}
	b.mu.Lock()
	delete(b.lastText, chatID)
	delete(b.pinWarn, chatID)
	delete(b.drafts, chatID)
	b.mu.Unlock()
	b.log.Info("delivery: chat unavailable, dropped subscriptions and queued events", "chat", chatID, "rows", n)
}

// syncPanelsOnce refreshes the panels of all bound chats when the cycle
// flagged them dirty; the text is built from store.LastChecks() — an
// incomplete cycle does not erase known state.
func (b *Bot) syncPanelsOnce(ctx context.Context) {
	if !b.panelDirty.Swap(false) {
		return
	}
	checks, err := b.store.LastChecks()
	if err != nil {
		b.log.Error("panel: last checks", "err", err)
		return
	}
	text := b.panelText(checks)
	chats, err := b.store.StatusChats()
	if err != nil {
		b.log.Error("panel: status chats", "err", err)
		return
	}
	for _, chatID := range chats {
		b.syncPanel(ctx, chatID, text)
	}
}

func (b *Bot) cleanupOutboxOnce() {
	now := time.Now().Unix()
	if last := b.lastCleanup.Load(); last != 0 && now-last < int64(cleanupEvery/time.Second) {
		return
	}
	b.lastCleanup.Store(now)
	if _, err := b.store.DeleteOldOutbox(outboxRetention); err != nil {
		b.log.Error("delivery: outbox cleanup", "err", err)
	}
	// TTL is a safety valve; everything dropped is logged: silent event
	// loss is unacceptable
	if n, err := b.store.DropStaleOutbox(outboxTTL); err != nil {
		b.log.Error("delivery: outbox ttl", "err", err)
	} else if n > 0 {
		b.log.Warn("delivery: dropped undeliverable events older than TTL", "rows", n)
	}
}
