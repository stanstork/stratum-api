package notification

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"
	"github.com/stanstork/stratum-api/internal/config"
	"github.com/stanstork/stratum-api/internal/models"
)

type FirebaseNotifier struct {
	enabled   bool
	projectID string
	topic     string
	logger    zerolog.Logger
}

func NewFirebaseNotifier(cfg config.FirebaseConfig, logger zerolog.Logger) *FirebaseNotifier {
	enabled := cfg.Enabled && cfg.ProjectID != "" && cfg.Topic != ""
	return &FirebaseNotifier{
		enabled:   enabled,
		projectID: cfg.ProjectID,
		topic:     cfg.Topic,
		logger:    logger.With().Str("notifier", "firebase").Logger(),
	}
}

func (n *FirebaseNotifier) Notify(_ context.Context, notif models.Notification) error {
	if !n.enabled {
		return nil
	}
	n.logger.Info().
		Str("notification_id", notif.ID).
		Str("event_type", string(notif.EventType)).
		Str("topic", n.topic).
		Msg("firebase notification dispatched (mock)")
	return nil
}

func (n *FirebaseNotifier) String() string {
	if !n.enabled {
		return "FirebaseNotifier(disabled)"
	}
	return fmt.Sprintf("FirebaseNotifier(project=%s, topic=%s)", n.projectID, n.topic)
}
