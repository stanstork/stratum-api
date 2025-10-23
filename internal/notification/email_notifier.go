package notification

import (
	"context"
	"fmt"
	"net/smtp"
	"strings"

	"github.com/rs/zerolog"
	"github.com/stanstork/stratum-api/internal/config"
	"github.com/stanstork/stratum-api/internal/models"
)

type EmailNotifier struct {
	host       string
	port       int
	username   string
	password   string
	from       string
	recipients []string
	logger     zerolog.Logger
}

func NewEmailNotifier(cfg config.EmailConfig, logger zerolog.Logger) (*EmailNotifier, error) {
	recipients := sanitizeRecipients(cfg.AlertRecipients)
	host := strings.TrimSpace(cfg.SMTPHost)
	from := strings.TrimSpace(cfg.From)
	if host == "" {
		return nil, fmt.Errorf("smtp_host is required for email notifier")
	}
	if from == "" {
		return nil, fmt.Errorf("from is required for email notifier")
	}
	port := cfg.SMTPPort
	if port == 0 {
		port = 587
	}

	return &EmailNotifier{
		host:       host,
		port:       port,
		username:   strings.TrimSpace(cfg.Username),
		password:   cfg.Password,
		from:       from,
		recipients: recipients,
		logger:     logger.With().Str("notifier", "email").Logger(),
	}, nil
}

func (n *EmailNotifier) Notify(_ context.Context, notif models.Notification) error {
	if len(n.recipients) == 0 {
		return nil
	}

	subject := fmt.Sprintf("[Stratum] %s", strings.TrimSpace(notif.Title))
	if subject == "[Stratum] " {
		subject = "[Stratum] Notification"
	}

	body := strings.Builder{}
	body.WriteString(strings.TrimSpace(notif.Message))
	body.WriteString("\n\n")
	body.WriteString(fmt.Sprintf("Event: %s\n", notif.EventType))
	body.WriteString(fmt.Sprintf("Severity: %s\n", notif.Severity))
	body.WriteString(fmt.Sprintf("Created: %s\n", notif.CreatedAt.Format("2006-01-02 15:04:05 MST")))
	if len(notif.Metadata) > 0 {
		body.WriteString(fmt.Sprintf("Metadata: %s\n", string(notif.Metadata)))
	}

	headers := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=\"UTF-8\"\r\n\r\n",
		n.from, strings.Join(n.recipients, ","), subject)

	message := []byte(headers + body.String())
	addr := fmt.Sprintf("%s:%d", n.host, n.port)

	var auth smtp.Auth
	if n.username != "" {
		auth = smtp.PlainAuth("", n.username, n.password, n.host)
	}

	err := smtp.SendMail(addr, auth, n.from, n.recipients, message)
	if err != nil {
		return err
	}

	n.logger.Info().
		Str("notification_id", notif.ID).
		Str("event_type", string(notif.EventType)).
		Strs("recipients", n.recipients).
		Msg("email notification sent")
	return nil
}

func (n *EmailNotifier) String() string {
	return "EmailNotifier"
}
