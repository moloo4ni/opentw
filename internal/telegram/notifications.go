package telegram

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"opentw/internal/monitor"
	"opentw/internal/status"
	"opentw/internal/storage"
)

// deliveryPayload — содержимое одной строки outbox: события цикла, адресованные
// одному чату, плюс контекст массового сбоя для заголовка сводки.
type deliveryPayload struct {
	Trs         []monitor.Transition `json:"trs"`
	Total       int                  `json:"total"`         // всего моделей в конфигурации
	DownInCycle int                  `json:"down_in_cycle"` // переходов в Down глобально за цикл
}

// prepareTransitions группирует переходы цикла по подписанным чатам и готовит
// по одной outbox-строке на чат: события одного цикла приходят одним
// сообщением. Записи не пишет — их фиксирует раннер через Store.ApplyCycle
// атомарно с состояниями моделей (P0-1). Ошибка чтения подписчиков — ошибка
// всего цикла: следующий цикл переоткроет те же переходы, потери нет.
func (b *Bot) prepareTransitions(res monitor.CycleResult) ([]storage.OutboxEvent, error) {
	if len(res.Transitions) == 0 {
		return nil, nil
	}
	downInCycle := 0
	for _, tr := range res.Transitions {
		if tr.To == status.Down {
			downInCycle++
		}
	}
	perChat := map[int64][]monitor.Transition{}
	for _, tr := range res.Transitions {
		chats, err := b.store.Subscribers(tr.ModelID)
		if err != nil {
			return nil, fmt.Errorf("notify: subscribers: %w", err)
		}
		for _, chatID := range chats {
			perChat[chatID] = append(perChat[chatID], tr)
		}
	}
	dedupeKey := fmt.Sprintf("%d", res.CycleAt.UnixNano())
	events := make([]storage.OutboxEvent, 0, len(perChat))
	for chatID, trs := range perChat {
		payload, err := json.Marshal(deliveryPayload{Trs: trs, Total: len(b.models), DownInCycle: downInCycle})
		if err != nil {
			return nil, fmt.Errorf("notify: encode: %w", err)
		}
		kind := "grouped"
		if len(trs) == 1 {
			kind = "single"
		}
		events = append(events, storage.OutboxEvent{ChatID: chatID, Kind: kind, DedupeKey: dedupeKey, Payload: payload})
	}
	return events, nil
}

// humanDuration — длительность инцидента в человекочитаемом виде.
func humanDuration(d time.Duration) string {
	sec := int64(d.Seconds())
	if sec < 0 {
		sec = 0
	}
	switch {
	case sec == 0:
		return "0с"
	case sec < 60:
		return fmt.Sprintf("%dс", sec)
	case sec < 3600:
		return fmt.Sprintf("%dм %02dс", sec/60, sec%60)
	default:
		return fmt.Sprintf("%dч %02dм", sec/3600, (sec%3600)/60)
	}
}

// RenderNotification форматирует одиночное событийное уведомление: русский
// строчный глагол в заголовке, исходные данные провайдера в теле, время и длительность.
func RenderNotification(tr monitor.Transition) string {
	var b strings.Builder
	switch tr.To {
	case status.Operational:
		b.WriteString(tr.To.Emoji() + " " + codeID(tr.ModelID) + " - восстановилась\n\n")
		b.WriteString("HTTP 200")
		if tr.Check.LatencyUS != nil {
			b.WriteString("\nLatency: " + fmtSecs(*tr.Check.LatencyUS))
		}
		if tr.Check.TPS != nil && *tr.Check.TPS > 0 {
			b.WriteString(fmt.Sprintf("\nTPS: %.1f", *tr.Check.TPS))
		}
	case status.Degraded:
		if tr.From == status.Down {
			b.WriteString(tr.To.Emoji() + " " + codeID(tr.ModelID) + " - частично восстановилась\n\n")
		} else {
			b.WriteString(tr.To.Emoji() + " " + codeID(tr.ModelID) + " - работает нестабильно\n\n")
		}
		b.WriteString(notifyErrorBody(tr.Check))
	default: // Down
		b.WriteString(tr.To.Emoji() + " " + codeID(tr.ModelID) + " - недоступна\n\n")
		b.WriteString(notifyErrorBody(tr.Check))
		if tr.From == status.Unknown {
			// Модель вернулась из слепого окна монитора уже упавшей (D2):
			// длительность неизвестна — честно сообщаем, не выдумываем.
			b.WriteString("\n\nТочное время начала недоступности неизвестно: монитор не мог проверять модель.")
		}
	}
	b.WriteString("\n\nПроверено: " + fmtClockMSK(tr.Check.At) + " MSK")
	if (tr.To == status.Operational || (tr.To == status.Degraded && tr.From == status.Down)) &&
		tr.Incident != nil && tr.Incident.Duration > 0 {
		b.WriteString("\nДлительность сбоя: " + humanDuration(tr.Incident.Duration))
	}
	return b.String()
}

// notifyErrorBody — исходные данные ошибки: HTTP-код и текст провайдера.
func notifyErrorBody(c storage.Check) string {
	var b strings.Builder
	switch c.ErrorType {
	case "perf":
		b.WriteString("Деградация производительности")
		if c.ErrorMessage != "" {
			b.WriteString("\n" + EscapeHTML(oneLine(c.ErrorMessage)))
		}
	default:
		b.WriteString(providerErrorBody(c))
	}
	return b.String()
}

// providerErrorBody — код + исходный текст провайдера (кап 500 рун — лимит 4096).
func providerErrorBody(c storage.Check) string {
	switch c.ErrorType {
	case "http":
		line := fmt.Sprintf("HTTP %d", c.HTTPStatus)
		if c.ErrorMessage != "" {
			line += "\n" + EscapeHTML(trunc(oneLine(c.ErrorMessage), 500))
		}
		return line
	case "empty":
		line := "HTTP 200"
		if c.ErrorMessage != "" && c.ErrorMessage != "empty generation" {
			line += "\n" + EscapeHTML(trunc(oneLine(c.ErrorMessage), 500))
		}
		return line
	case "timeout":
		return "Timeout"
	case "connrefused":
		return "Connection refused"
	case "invalid":
		return "Некорректный ответ API"
	default:
		if c.ErrorMessage != "" {
			return EscapeHTML(trunc(oneLine(c.ErrorMessage), 500))
		}
		return "ошибка монитора"
	}
}
