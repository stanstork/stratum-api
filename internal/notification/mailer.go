package notification

import (
	"fmt"
	"net/smtp"
	"strings"

	"github.com/stanstork/stratum-api/internal/config"
)

// InviteMailer is responsible for delivering tenant invite emails.
type InviteMailer interface {
	SendInvite(recipientEmail, tenantName, inviteURL string) error
}

// SMTPInviteMailer sends invite emails using an SMTP server.
type SMTPInviteMailer struct {
	host     string
	port     int
	username string
	password string
	from     string
}

// NewSMTPInviteMailer constructs a new SMTPInviteMailer from config.
func NewSMTPInviteMailer(cfg config.EmailConfig) (*SMTPInviteMailer, error) {
	if strings.TrimSpace(cfg.SMTPHost) == "" {
		return nil, fmt.Errorf("smtp_host is required")
	}
	if cfg.SMTPPort == 0 {
		cfg.SMTPPort = 587
	}
	if strings.TrimSpace(cfg.From) == "" {
		return nil, fmt.Errorf("email from address is required")
	}

	return &SMTPInviteMailer{
		host:     cfg.SMTPHost,
		port:     cfg.SMTPPort,
		username: cfg.Username,
		password: cfg.Password,
		from:     cfg.From,
	}, nil
}

// SendInvite dispatches an invitation email to a prospective user.
func (m *SMTPInviteMailer) SendInvite(recipientEmail, tenantName, inviteURL string) error {
	headers := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=\"UTF-8\"\r\n\r\n",
		m.from, recipientEmail, fmt.Sprintf("You have been invited to join %s", tenantName))

	body := strings.Builder{}
	body.WriteString("Hello,\n\n")
	body.WriteString(fmt.Sprintf("You've been invited to join the %s workspace on Stratum.\n", tenantName))
	body.WriteString("Click the link below to accept the invitation and create your account:\n\n")
	body.WriteString(inviteURL + "\n\n")
	body.WriteString("This invite is valid for a limited time. If you did not expect this email, you can ignore it.\n\n")
	body.WriteString("Thanks,\nThe Stratum Team\n")

	message := []byte(headers + body.String())

	addr := fmt.Sprintf("%s:%d", m.host, m.port)

	var auth smtp.Auth
	if strings.TrimSpace(m.username) != "" {
		auth = smtp.PlainAuth("", m.username, m.password, m.host)
	}

	return smtp.SendMail(addr, auth, m.from, []string{recipientEmail}, message)
}
