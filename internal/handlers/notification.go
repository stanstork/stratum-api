package handlers

import (
	"database/sql"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/rs/zerolog"
	"github.com/stanstork/stratum-api/internal/authz"
	"github.com/stanstork/stratum-api/internal/notification"
)

type NotificationHandler struct {
	service notification.Service
	logger  zerolog.Logger
}

func NewNotificationHandler(service notification.Service, logger zerolog.Logger) *NotificationHandler {
	return &NotificationHandler{
		service: service,
		logger:  logger.With().Str("handler", "notification").Logger(),
	}
}

func (h *NotificationHandler) List(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := authz.TenantIDFromRequest(r)
	if !ok {
		http.Error(w, "Missing tenant context", http.StatusUnauthorized)
		return
	}

	limit := 25
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	notifications, err := h.service.ListRecent(r.Context(), tenantID, limit)
	if err != nil {
		h.logger.Error().Err(err).Msg("failed to list notifications")
		http.Error(w, "Failed to list notifications", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"notifications": notifications,
	})
}

func (h *NotificationHandler) MarkRead(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := authz.TenantIDFromRequest(r)
	if !ok {
		http.Error(w, "Missing tenant context", http.StatusUnauthorized)
		return
	}

	notifID := strings.TrimSpace(mux.Vars(r)["notificationID"])
	if notifID == "" {
		http.Error(w, "Notification ID is required", http.StatusBadRequest)
		return
	}

	notif, err := h.service.MarkRead(r.Context(), tenantID, notifID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "Notification not found", http.StatusNotFound)
			return
		}
		h.logger.Error().Err(err).Str("notification_id", notifID).Msg("failed to mark notification as read")
		http.Error(w, "Failed to update notification", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, notif)
}
