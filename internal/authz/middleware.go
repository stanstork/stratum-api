package authz

import (
	"net/http"

	"github.com/stanstork/stratum-api/internal/models"
)

// RequireRole returns a middleware that ensures the requester has at least the required role tier.
func RequireRole(required models.UserRole) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			roles, ok := RolesFromRequest(r)
			if !ok || !models.HasAtLeast(roles, required) {
				http.Error(w, "insufficient permissions", http.StatusForbidden)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// RequireRoleHandler applies the role middleware inline when registering routes.
func RequireRoleHandler(required models.UserRole, next http.Handler) http.Handler {
	return RequireRole(required)(next)
}
