package notification

import (
	"context"
	"strings"

	"github.com/rs/zerolog"
	"github.com/stanstork/stratum-api/internal/models"
)

type Notifier interface {
	Notify(ctx context.Context, notification models.Notification) error
}

func sanitizeRecipients(recipients []string) []string {
	var cleaned []string
	for _, recipient := range iterStrings(recipients) {
		if recipient != "" {
			cleaned = append(cleaned, recipient)
		}
	}
	return cleaned
}

func iterStrings(values []string) []string {
	if len(values) == 0 {
		return values
	}
	result := make([]string, len(values))
	for i, v := range values {
		result[i] = strings.TrimSpace(v)
	}
	return result
}

func logNotifyError(logger zerolog.Logger, err error, channel string, notif models.Notification) {
	if err == nil {
		return
	}
	logger.Warn().
		Err(err).
		Str("notification_id", notif.ID).
		Str("event_type", string(notif.EventType)).
		Str("channel", channel).
		Msg("failed to deliver notification")
}
