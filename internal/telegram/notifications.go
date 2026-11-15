package telegram

import (
	"context"
	"fmt"
	"strings"
	"time"

	"opentw/internal/monitor"
	"opentw/internal/status"
	"opentw/internal/storage"
)

// notifyTransitions рассылает события только чатам, подписанным на конкретную модель.
func (b *Bot) notifyTransitions(ctx context.Context, res monitor.CycleResult) {
	for _, tr := range res.Transitions {
		chats, err := b.store.Subscribers(tr.ModelID)
		if err != nil {
			b.log.Error("notify: subscribers", "model", tr.ModelID, "err", err)
			continue
		}
		if len(chats) == 0 {
			continue
		}
		text := RenderNotification(tr)
		for _, chatID := range chats {
			if _, err := b.api.SendMessage(ctx, chatID, text); err != nil {
				b.log.Error("notify: send", "chat", chatID, "model", tr.ModelID, "err", err)
			}
		}
	}
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

// RenderNotification форматирует событийное уведомление: русский строчный
// глагол в заголовке, исходные данные провайдера в теле, время и длительность.
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
	}
	b.WriteString("\n\nВремя: " + fmtClockMSK(tr.Check.At) + " MSK")
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
	case "http":
		line := fmt.Sprintf("HTTP %d", c.HTTPStatus)
		if c.ErrorMessage != "" {
			line += "\n" + EscapeHTML(oneLine(c.ErrorMessage))
		}
		b.WriteString(line)
	case "notfound":
		b.WriteString("нет модели")
	case "empty":
		b.WriteString("HTTP 200\nпустая генерация")
	case "timeout":
		b.WriteString("Timeout")
	case "connrefused":
		b.WriteString("Connection refused")
	case "invalid":
		b.WriteString("Некорректный ответ API")
	case "perf":
		b.WriteString("Деградация производительности")
	default:
		if c.ErrorMessage != "" {
			b.WriteString(EscapeHTML(oneLine(c.ErrorMessage)))
		}
	}
	return b.String()
}
