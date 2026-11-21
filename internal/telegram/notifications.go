package telegram

import (
	"encoding/json"
	"fmt"

	"opentw/internal/monitor"
	"opentw/internal/storage"
)

// deliveryPayload — содержимое строки outbox: события цикла, адресованные
// одному чату. Строки-этикетки (label) доставки строит по ТЕКУЩЕМУ состоянию
// модели, payload нужен только для группировки по моделям.
type deliveryPayload struct {
	Trs []monitor.Transition `json:"trs"`
}

// prepareTransitions группирует переходы цикла по подписанным чатам и готовит
// по одной outbox-строке на чат: события одного цикла приходят одним
// сообщением. Записи не пишет — их фиксирует раннер через Store.ApplyCycle
// атомарно с состояниями моделей. Ошибка чтения подписчиков — ошибка
// всего цикла: следующий цикл переоткроет те же переходы, потери нет.
func (b *Bot) prepareTransitions(res monitor.CycleResult) ([]storage.OutboxEvent, error) {
	var events []storage.OutboxEvent
	covered := map[int64]map[string]bool{} // (чат, модель), уже покрытые событием цикла
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
	// Сверочный резерв (OPW-01): уведомление об актуальном достоверном
	// состоянии не может зависеть от выживания исходной outbox-строки —
	// истечение TTL, поглощение при смене состояния или подавление
	// повторного события живым инцидентом не должны оставлять подписчика
	// в неведении. Расхождение chat_notified с зафиксированным состоянием
	// само по себе требует доставки: паре добавляется сверочная строка.
	// Коммитит её раннер атомарно с состояниями цикла (ApplyCycle), поэтому
	// «строка есть — состояния нет» невозможно. Дедуп-ключ несёт эпоху
	// (updated_at chat_notified): пока уведомлённое состояние не менялось,
	// строка одна и не плодится каждый цикл; после доставки эпоха новая —
	// ключ не конфликтует с уже отправленными строками; TTL-удаление
	// освобождает ключ. Сверка читает зафиксированные состояния ПРОШЛЫХ
	// циклов (до коммита текущего): свежее расхождение вооружается
	// следующим циклом — самовосстановление с задержкой ≤ Interval.
	// Пара с живым недоставленным свидетельством в очереди сверкой
	// НЕ дублируется: свёртка доставки всё равно даёт одну строку по
	// текущему состоянию, дубль лишь удлинял бы очередь.
	stale, err := b.store.StaleNotifyPairs()
	if err != nil {
		return nil, fmt.Errorf("notify: stale pairs: %w", err)
	}
	if len(stale) > 0 {
		// модели, чьё свидетельство уже ждёт в очереди чата
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
				continue // событие этого цикла уже несёт актуальное состояние пары
			}
			if pendingOf[p.ChatID][p.ModelID] {
				continue // недоставленное свидетельство ещё живо в очереди
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

// downLine — строка падения модели в уведомлении: общий с панелью HTML
// строит downModelLine, кнопка — с подписью из ПОСЛЕДНЕЙ проверки;
// доставщик передаёт актуальный check из БД.
func downLine(modelID string, c storage.Check) string {
	return "<p>" + downModelLine(modelID, shortErrorLabel(c)) + "</p>"
}

// recoveredLine — строка восстановления: без кнопки.
func recoveredLine(modelID string) string {
	return "<p>🟢 " + codeID(modelID) + " - Восстановилась</p>"
}
