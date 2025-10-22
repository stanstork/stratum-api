package notification

import (
	"context"
	"fmt"
	"strings"

	"github.com/rs/zerolog"
	"github.com/stanstork/stratum-api/internal/models"
	"github.com/stanstork/stratum-api/internal/repository"
)

type Event struct {
	TenantID string
	Event    models.NotificationEvent
	Severity models.NotificationSeverity
	Title    string
	Message  string
	Metadata map[string]interface{}
}

type Service interface {
	Publish(ctx context.Context, evt Event) (models.Notification, error)
	NotifyValidationComplete(ctx context.Context, tenantID, jobDefID, jobName string) error
	NotifyExecutionStarted(ctx context.Context, tenantID, jobDefID, executionID, jobName string) error
	NotifyExecutionSucceeded(ctx context.Context, tenantID, jobDefID, executionID, jobName string, recordsProcessed, bytesTransferred int64) error
	NotifyExecutionFailed(ctx context.Context, tenantID, jobDefID, executionID, jobName, reason string) error
	ListRecent(ctx context.Context, tenantID string, limit int) ([]models.Notification, error)
	MarkRead(ctx context.Context, tenantID, notificationID string) (models.Notification, error)
}

type service struct {
	repo      repository.NotificationRepository
	logger    zerolog.Logger
	notifiers []Notifier
}

func NewService(repo repository.NotificationRepository, logger zerolog.Logger, notifiers ...Notifier) Service {
	active := make([]Notifier, 0, len(notifiers))
	for _, notifier := range notifiers {
		if notifier != nil {
			active = append(active, notifier)
		}
	}
	return &service{
		repo:      repo,
		logger:    logger.With().Str("component", "notification_service").Logger(),
		notifiers: active,
	}
}

func (s *service) Publish(ctx context.Context, evt Event) (models.Notification, error) {
	if evt.Event == "" {
		return models.Notification{}, fmt.Errorf("event type is required")
	}
	if evt.Severity == "" {
		evt.Severity = models.NotificationSeverityInfo
	}
	title := strings.TrimSpace(evt.Title)
	message := strings.TrimSpace(evt.Message)
	if title == "" {
		title = string(evt.Event)
	}
	params := repository.CreateNotificationParams{
		Event:    evt.Event,
		Severity: evt.Severity,
		Title:    title,
		Message:  message,
		Metadata: evt.Metadata,
	}
	if tid := strings.TrimSpace(evt.TenantID); tid != "" {
		params.TenantID = &tid
	}

	notif, err := s.repo.Create(ctx, params)
	if err != nil {
		s.logger.Error().Err(err).Str("event_type", string(evt.Event)).Msg("failed to persist notification")
		return models.Notification{}, err
	}
	for _, notifier := range s.notifiers {
		if err := notifier.Notify(ctx, notif); err != nil {
			logNotifyError(s.logger, err, notifierChannelName(notifier), notif)
		}
	}
	return notif, nil
}

func (s *service) NotifyValidationComplete(ctx context.Context, tenantID, jobDefID, jobName string) error {
	if strings.TrimSpace(tenantID) == "" {
		return fmt.Errorf("tenant id is required for validation notifications")
	}
	_, err := s.Publish(ctx, Event{
		TenantID: tenantID,
		Event:    models.NotificationEventValidationComplete,
		Severity: models.NotificationSeverityInfo,
		Title:    "Validation complete",
		Message:  fmt.Sprintf("Job definition %q is ready.", jobName),
		Metadata: map[string]interface{}{
			"job_definition_id": jobDefID,
			"job_definition":    jobName,
		},
	})
	return err
}

func (s *service) NotifyExecutionStarted(ctx context.Context, tenantID, jobDefID, executionID, jobName string) error {
	if strings.TrimSpace(tenantID) == "" {
		return fmt.Errorf("tenant id is required for execution notifications")
	}
	name := fallbackName(jobName, jobDefID)
	_, err := s.Publish(ctx, Event{
		TenantID: tenantID,
		Event:    models.NotificationEventExecutionStarted,
		Severity: models.NotificationSeverityInfo,
		Title:    fmt.Sprintf("Execution started: %s", name),
		Message:  fmt.Sprintf("Job %s execution %s has started.", name, executionID),
		Metadata: map[string]interface{}{
			"job_definition_id": jobDefID,
			"job_definition":    name,
			"execution_id":      executionID,
		},
	})
	return err
}

func (s *service) NotifyExecutionSucceeded(ctx context.Context, tenantID, jobDefID, executionID, jobName string, recordsProcessed, bytesTransferred int64) error {
	if strings.TrimSpace(tenantID) == "" {
		return fmt.Errorf("tenant id is required for execution notifications")
	}
	name := fallbackName(jobName, jobDefID)
	metadata := map[string]interface{}{
		"job_definition_id": jobDefID,
		"job_definition":    name,
		"execution_id":      executionID,
	}
	if recordsProcessed > 0 {
		metadata["records_processed"] = recordsProcessed
	}
	if bytesTransferred > 0 {
		metadata["bytes_transferred"] = bytesTransferred
	}
	_, err := s.Publish(ctx, Event{
		TenantID: tenantID,
		Event:    models.NotificationEventExecutionSucceeded,
		Severity: models.NotificationSeverityInfo,
		Title:    fmt.Sprintf("Execution succeeded: %s", name),
		Message:  fmt.Sprintf("Job %s execution %s completed successfully.", name, executionID),
		Metadata: metadata,
	})
	return err
}

func (s *service) NotifyExecutionFailed(ctx context.Context, tenantID, jobDefID, executionID, jobName, reason string) error {
	if strings.TrimSpace(tenantID) == "" {
		return fmt.Errorf("tenant id is required for execution notifications")
	}
	name := fallbackName(jobName, jobDefID)
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "Unknown error"
	}
	_, err := s.Publish(ctx, Event{
		TenantID: tenantID,
		Event:    models.NotificationEventExecutionFailed,
		Severity: models.NotificationSeverityError,
		Title:    fmt.Sprintf("Execution failed: %s", name),
		Message:  fmt.Sprintf("Job %s execution %s failed: %s", name, executionID, reason),
		Metadata: map[string]interface{}{
			"job_definition_id": jobDefID,
			"job_definition":    name,
			"execution_id":      executionID,
			"reason":            reason,
		},
	})
	return err
}

func (s *service) ListRecent(ctx context.Context, tenantID string, limit int) ([]models.Notification, error) {
	return s.repo.ListRecent(ctx, tenantID, limit)
}

func (s *service) MarkRead(ctx context.Context, tenantID, notificationID string) (models.Notification, error) {
	return s.repo.MarkRead(ctx, tenantID, notificationID)
}

func fallbackName(name, fallback string) string {
	if trimmed := strings.TrimSpace(name); trimmed != "" {
		return trimmed
	}
	return fallback
}

func notifierChannelName(n Notifier) string {
	type named interface {
		String() string
	}
	if v, ok := n.(named); ok {
		return v.String()
	}
	return fmt.Sprintf("%T", n)
}
