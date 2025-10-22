package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/stanstork/stratum-api/internal/models"
)

type NotificationRepository interface {
	Create(ctx context.Context, params CreateNotificationParams) (models.Notification, error)
	ListRecent(ctx context.Context, tenantID string, limit int) ([]models.Notification, error)
	MarkRead(ctx context.Context, tenantID, notificationID string) (models.Notification, error)
}

type notificationRepository struct {
	db *sql.DB
}

type CreateNotificationParams struct {
	TenantID *string
	Event    models.NotificationEvent
	Severity models.NotificationSeverity
	Title    string
	Message  string
	Metadata map[string]interface{}
}

func NewNotificationRepository(db *sql.DB) NotificationRepository {
	return &notificationRepository{db: db}
}

func (r *notificationRepository) Create(ctx context.Context, params CreateNotificationParams) (models.Notification, error) {
	const query = `
		INSERT INTO tenant.notifications (tenant_id, event_type, severity, title, message, metadata)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, tenant_id, event_type, severity, title, message, metadata, created_at, read_at
	`

	var tenantID interface{}
	if params.TenantID != nil && strings.TrimSpace(*params.TenantID) != "" {
		tenantID = strings.TrimSpace(*params.TenantID)
	}

	var metadata interface{}
	if len(params.Metadata) > 0 {
		bytes, err := json.Marshal(params.Metadata)
		if err != nil {
			return models.Notification{}, fmt.Errorf("marshal metadata: %w", err)
		}
		metadata = bytes
	}

	row := r.db.QueryRowContext(ctx, query, tenantID, params.Event, params.Severity, params.Title, params.Message, metadata)
	return scanNotification(row)
}

func (r *notificationRepository) ListRecent(ctx context.Context, tenantID string, limit int) ([]models.Notification, error) {
	if limit <= 0 || limit > 100 {
		limit = 25
	}

	const query = `
		SELECT id, tenant_id, event_type, severity, title, message, metadata, created_at, read_at
		FROM tenant.notifications
		WHERE tenant_id IS NULL OR tenant_id = $1
		ORDER BY created_at DESC
		LIMIT $2
	`

	rows, err := r.db.QueryContext(ctx, query, strings.TrimSpace(tenantID), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var notifications []models.Notification
	for rows.Next() {
		notif, err := scanNotification(rows)
		if err != nil {
			return nil, err
		}
		notifications = append(notifications, notif)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return notifications, nil
}

func (r *notificationRepository) MarkRead(ctx context.Context, tenantID, notificationID string) (models.Notification, error) {
	const query = `
		UPDATE tenant.notifications
		SET read_at = NOW()
		WHERE id = $1 AND (tenant_id IS NULL OR tenant_id = $2)
		RETURNING id, tenant_id, event_type, severity, title, message, metadata, created_at, read_at
	`
	row := r.db.QueryRowContext(ctx, query, strings.TrimSpace(notificationID), strings.TrimSpace(tenantID))
	return scanNotification(row)
}

func scanNotification(scanner interface {
	Scan(dest ...interface{}) error
}) (models.Notification, error) {
	var (
		notif       models.Notification
		tenantID    sql.NullString
		metadataRaw []byte
		readAt      sql.NullTime
	)

	if err := scanner.Scan(
		&notif.ID,
		&tenantID,
		&notif.EventType,
		&notif.Severity,
		&notif.Title,
		&notif.Message,
		&metadataRaw,
		&notif.CreatedAt,
		&readAt,
	); err != nil {
		return models.Notification{}, err
	}

	if tenantID.Valid {
		val := tenantID.String
		notif.TenantID = &val
	}
	if len(metadataRaw) > 0 {
		notif.Metadata = metadataRaw
	}
	if readAt.Valid {
		t := readAt.Time
		notif.ReadAt = &t
	}

	return notif, nil
}
