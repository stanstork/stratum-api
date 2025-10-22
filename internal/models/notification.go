package models

import (
	"encoding/json"
	"time"
)

type NotificationSeverity string

const (
	NotificationSeverityInfo    NotificationSeverity = "info"
	NotificationSeverityWarning NotificationSeverity = "warning"
	NotificationSeverityError   NotificationSeverity = "error"
)

type NotificationEvent string

const (
	NotificationEventExecutionStarted   NotificationEvent = "execution_started"
	NotificationEventExecutionSucceeded NotificationEvent = "execution_succeeded"
	NotificationEventExecutionFailed    NotificationEvent = "execution_failed"
	NotificationEventValidationComplete NotificationEvent = "validation_complete"
)

type Notification struct {
	ID        string               `json:"id" db:"id"`
	TenantID  *string              `json:"tenant_id,omitempty" db:"tenant_id"`
	EventType NotificationEvent    `json:"event_type" db:"event_type"`
	Severity  NotificationSeverity `json:"severity" db:"severity"`
	Title     string               `json:"title" db:"title"`
	Message   string               `json:"message" db:"message"`
	Metadata  json.RawMessage      `json:"metadata,omitempty" db:"metadata"`
	CreatedAt time.Time            `json:"created_at" db:"created_at"`
	ReadAt    *time.Time           `json:"read_at,omitempty" db:"read_at"`
}
