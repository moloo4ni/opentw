package telegram

import (
	"encoding/json"
	"fmt"

	"github.com/moloo4ni/opentw/internal/monitor"
	"github.com/moloo4ni/opentw/internal/storage"
)

// deliveryPayload is the content of an outbox row: the cycle's events for one
// chat. Delivery builds the line labels from the model's CURRENT state; the
// payload is only needed for grouping by model.
type deliveryPayload struct {
	Trs []monitor.Transition `json:"trs"`
}

// prepareTransitions groups the cycle's transitions by subscribed chat and
// prepares one outbox row per chat: one cycle's events arrive as one message.
// It performs no writes — the runner commits them via Store.ApplyCycle
// atomically with the model states. A subscriber read failure fails the whole
// cycle: the next cycle reopens the same transitions, nothing is lost.
func (b *Bot) prepareTransitions(res monitor.CycleResult) ([]storage.OutboxEvent, error) {
	var events []storage.OutboxEvent
	covered := map[int64]map[string]bool{} // (chat, model) pairs already covered by a cycle event
	if len(res.Transitions) > 0 {
		perChat := map[int64][]monitor.Transition{}
		for _, tr := range res.Transitions {
			chats, err := b.store.Subscribers(tr.ModelID)
			if err != nil {
				return nil, fmt.Errorf("notify: subscribers: %w", err)
			}
			for _, chatID := range chats {
				perChat[chatID] = append(perChat[chatID], tr)
				if covered[chatID] == nil {
					covered[chatID] = map[string]bool{}
				}
				covered[chatID][tr.ModelID] = true
			}
		}
		dedupeKey := fmt.Sprintf("%d", res.CycleAt.UnixNano())
		events = make([]storage.OutboxEvent, 0, len(perChat))
		for chatID, trs := range perChat {
			payload, err := json.Marshal(deliveryPayload{Trs: trs})
			if err != nil {
				return nil, fmt.Errorf("notify: encode: %w", err)
			}
			kind := "grouped"
			if len(trs) == 1 {
				kind = "single"
			}
			events = append(events, storage.OutboxEvent{ChatID: chatID, Kind: kind, DedupeKey: dedupeKey, Payload: payload})
		}
	}
	// Reconciliation fallback: a notification about the current definitive
	// state must not depend on the survival of the original outbox row — TTL
	// expiry, swallowing on a state change or suppression of a repeated event
	// by a live incident must not leave the subscriber in the dark. A
	// chat_notified divergence from the settled state warrants delivery by
	// itself: the pair gets a reconciliation row. The runner commits it
	// atomically with the cycle's states (ApplyCycle), so "row without state"
	// is impossible. The dedupe key carries the epoch (chat_notified
	// updated_at): while the notified state is unchanged there is one row, not
	// a new one per cycle; after delivery the epoch is new — the key does not
	// collide with already sent rows; TTL cleanup frees the key. The
	// reconciliation reads states committed by PAST cycles (before the current
	// commit): a fresh divergence arms on the next cycle — self-healing with
	// a delay ≤ Interval. A pair with a live undelivered evidence row in the
	// queue is NOT duplicated by reconciliation: the delivery collapse still
	// yields one line for the current state, a duplicate would only lengthen
	// the queue.
	stale, err := b.store.StaleNotifyPairs()
	if err != nil {
		return nil, fmt.Errorf("notify: stale pairs: %w", err)
	}
	if len(stale) > 0 {
		// models whose evidence is already queued for the chat
		pendingOf := map[int64]map[string]bool{}
		for _, p := range stale {
			if _, ok := pendingOf[p.ChatID]; ok {
				continue
			}
			rows, err := b.store.PendingOutboxChat(p.ChatID)
			if err != nil {
				return nil, fmt.Errorf("notify: pending rows: %w", err)
			}
			queued := map[string]bool{}
			for _, row := range rows {
				var p deliveryPayload
				if json.Unmarshal(row.Payload, &p) == nil {
					for _, tr := range p.Trs {
						queued[tr.ModelID] = true
					}
				}
			}
			pendingOf[p.ChatID] = queued
		}
		for _, p := range stale {
			if covered[p.ChatID] != nil && covered[p.ChatID][p.ModelID] {
				continue // this cycle's event already carries the pair's current state
			}
			if pendingOf[p.ChatID][p.ModelID] {
				continue // undelivered evidence is still alive in the queue
			}
			payload, err := json.Marshal(deliveryPayload{Trs: []monitor.Transition{{
				ModelID: p.ModelID, From: p.Notified, To: p.Current,
			}}})
			if err != nil {
				return nil, fmt.Errorf("notify: encode reconcile: %w", err)
			}
			key := "recon:" + p.ModelID + ":" + string(p.Current) + ":" + p.NotifiedAt
			events = append(events, storage.OutboxEvent{
				ChatID: p.ChatID, Kind: "reconcile", DedupeKey: key, Payload: payload,
			})
		}
	}
	return events, nil
}

// downLine is a model-fall notification line: the HTML is shared with the
// panel via downModelLine; the button carries the LAST check's label;
// the delivery worker passes the current check from the DB.
func downLine(modelID string, c storage.Check) string {
	return "<p>" + downModelLine(modelID, shortErrorLabel(c)) + "</p>"
}

// recoveredLine is a recovery line: no button.
func recoveredLine(modelID string) string {
	return "<p>🟢 " + codeID(modelID) + " - Восстановилась</p>"
}
