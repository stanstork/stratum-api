package authz

import (
	"context"
	"net/http"

	"github.com/stanstork/stratum-api/internal/models"
)

type contextKey string

const (
	tenantIDKey  contextKey = "tenant_id"
	userIDKey    contextKey = "user_id"
	userRolesKey contextKey = "user_roles"
)

// WithIdentity stores tenant, user, and role information on the context.
func WithIdentity(ctx context.Context, tenantID, userID string, roles []models.UserRole) context.Context {
	if tenantID != "" {
		ctx = context.WithValue(ctx, tenantIDKey, tenantID)
	}
	if userID != "" {
		ctx = context.WithValue(ctx, userIDKey, userID)
	}
	normalized := models.EnsureDefaultRole(models.NormalizeRoles(roles))
	ctx = context.WithValue(ctx, userRolesKey, normalized)
	return ctx
}

func TenantIDFromRequest(r *http.Request) (string, bool) {
	tid, ok := r.Context().Value(tenantIDKey).(string)
	if !ok || tid == "" {
		return "", false
	}
	return tid, true
}

func UserIDFromRequest(r *http.Request) (string, bool) {
	uid, ok := r.Context().Value(userIDKey).(string)
	if !ok || uid == "" {
		return "", false
	}
	return uid, true
}

func RolesFromRequest(r *http.Request) ([]models.UserRole, bool) {
	roles, ok := r.Context().Value(userRolesKey).([]models.UserRole)
	if !ok || !models.IsValidRoleList(roles) {
		return nil, false
	}
	return roles, true
}
