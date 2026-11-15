package telegram

import (
	"context"
	"fmt"
	"strings"

	"opentw/internal/monitor"
	"opentw/internal/status"
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
func humanDuration(d interface{ Seconds() float64 }) string {
	sec := int64(d.Seconds())
	switch {
	case sec < 0:
		sec = 0
		fallthrough
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

// RenderNotification форматирует событийное уведомление по ТЗ §10.
func RenderNotification(tr monitor.Transition) string {
	var b strings.Builder
	emoji := tr.To.Emoji()
	state := strings.ToUpper(string(tr.To))
	if tr.To == status.Operational {
		// русская строка для восстановления
		b.WriteString(emoji + " " + escape(tr.ModelID) + " — OPERATIONAL\n\n")
	} else {
		b.WriteString(emoji + " " + escape(tr.ModelID) + " — " + state + "\n\n")
	}

	switch tr.To {
	case status.Operational:
		b.WriteString("HTTP 200\n")
		if tr.Check.LatencyUS != nil {
			b.WriteString("Latency: " + fmtSecs(*tr.Check.LatencyUS) + "\n")
		}
		if tr.Check.TPS != nil && *tr.Check.TPS > 0 {
			b.WriteString(fmt.Sprintf("TPS: %.1f\n", *tr.Check.TPS))
		}
		speedline := ""
		if tr.Incident != nil && tr.Incident.Duration > 0 {
			speedline = humanDuration(tr.Incident.Duration)
			b.WriteString("\nВосстановлена: " + fmtClockMSK(tr.Check.At) + " MSK\n")
			b.WriteString("Длительность сбоя: " + speedline)
		} else {
			b.WriteString("\nВосстановлена: " + fmtClockMSK(tr.Check.At) + " MSK")
		}
		if tr.From == status.Degraded {
			// полная компоновка уже согласована выше
			_ = speedline
		}
	case status.Degraded:
		if tr.From == status.Down {
			// частичное восстановление: Down → Degraded
			b.WriteString(detailErrorLine(tr.Check) + "\n")
			if tr.Incident != nil && tr.Incident.Duration > 0 {
				b.WriteString("\nЧастично восстановлена: " + fmtClockMSK(tr.Check.At) + " MSK\n")
				b.WriteString("Длительность сбоя: " + humanDuration(tr.Incident.Duration))
			} else {
				b.WriteString("\nЧастично восстановлена: " + fmtClockMSK(tr.Check.At) + " MSK")
			}
		} else {
			b.WriteString(detailErrorLine(tr.Check) + "\n")
			if tr.Incident != nil && tr.Incident.Duration > 0 {
				b.WriteString("\nДлительность: " + humanDuration(tr.Incident.Duration))
			}
			b.WriteString("\nВремя: " + fmtClockMSK(tr.Check.At) + " MSK")
		}
	default: // Down
		b.WriteString(detailErrorLine(tr.Check) + "\n")
		b.WriteString("\nВремя: " + fmtClockMSK(tr.Check.At) + " MSK")
	}
	return b.String()
}

func escape(s string) string { return EscapeHTML(s) }
