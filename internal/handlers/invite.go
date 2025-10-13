package handlers

import (
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/stanstork/stratum-api/internal/authz"
	"github.com/stanstork/stratum-api/internal/models"
	"github.com/stanstork/stratum-api/internal/notification"
	"github.com/stanstork/stratum-api/internal/repository"
)

const defaultInviteTTL = 7 * 24 * time.Hour

type InviteHandler struct {
	inviteRepo repository.InviteRepository
	tenantRepo repository.TenantRepository
	userRepo   repository.UserRepository
	tokenTTL   time.Duration
	mailer     notification.InviteMailer
	urlTpl     string
}

type inviteRequest struct {
	Email          string   `json:"email"`
	Roles          []string `json:"roles"`
	ExpiresInHours *int     `json:"expires_in_hours"`
}

func NewInviteHandler(
	inviteRepo repository.InviteRepository,
	tenantRepo repository.TenantRepository,
	userRepo repository.UserRepository,
	mailer notification.InviteMailer,
	inviteURLTemplate string,
) *InviteHandler {
	if inviteURLTemplate == "" {
		inviteURLTemplate = "https://app.stratum.dev/invite/accept?token=%s"
	}
	return &InviteHandler{
		inviteRepo: inviteRepo,
		tenantRepo: tenantRepo,
		userRepo:   userRepo,
		tokenTTL:   defaultInviteTTL,
		mailer:     mailer,
		urlTpl:     inviteURLTemplate,
	}
}

func (h *InviteHandler) CreateInvite(w http.ResponseWriter, r *http.Request) {
	requesterRoles, _ := authz.RolesFromRequest(r)
	isSuperAdmin := models.HasAtLeast(requesterRoles, models.RoleSuperAdmin)

	tenantID := mux.Vars(r)["tenantID"]
	if tenantID == "" {
		http.Error(w, "tenant id is required", http.StatusBadRequest)
		return
	}

	if !isSuperAdmin {
		if tid, ok := authz.TenantIDFromRequest(r); !ok || tid != tenantID {
			http.Error(w, "insufficient permissions for tenant", http.StatusForbidden)
			return
		}
	}

	tenant, err := h.tenantRepo.GetTenantByID(tenantID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "tenant not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to load tenant: "+err.Error(), http.StatusInternalServerError)
		return
	}

	var payload inviteRequest
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid request payload", http.StatusBadRequest)
		return
	}

	var createdBy *string
	if uid, ok := authz.UserIDFromRequest(r); ok {
		createdBy = &uid
	}

	h.processInviteCreation(w, tenant, payload, createdBy)
}

func (h *InviteHandler) CreateCurrentTenantInvite(w http.ResponseWriter, r *http.Request) {
	requesterRoles, _ := authz.RolesFromRequest(r)
	if !models.HasAtLeast(requesterRoles, models.RoleAdmin) {
		http.Error(w, "insufficient permissions", http.StatusForbidden)
		return
	}

	tenantID, ok := authz.TenantIDFromRequest(r)
	if !ok || tenantID == "" {
		http.Error(w, "tenant context missing", http.StatusForbidden)
		return
	}

	tenant, err := h.tenantRepo.GetTenantByID(tenantID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "tenant not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to load tenant: "+err.Error(), http.StatusInternalServerError)
		return
	}

	var payload inviteRequest
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid request payload", http.StatusBadRequest)
		return
	}

	var createdBy *string
	if uid, ok := authz.UserIDFromRequest(r); ok {
		createdBy = &uid
	}

	h.processInviteCreation(w, tenant, payload, createdBy)
}

func (h *InviteHandler) processInviteCreation(w http.ResponseWriter, tenant models.Tenant, payload inviteRequest, createdBy *string) {
	email := strings.TrimSpace(strings.ToLower(payload.Email))
	if email == "" {
		http.Error(w, "email is required", http.StatusBadRequest)
		return
	}

	roles := make([]models.UserRole, 0, len(payload.Roles))
	if len(payload.Roles) == 0 {
		roles = append(roles, models.RoleViewer)
	} else {
		for _, roleStr := range payload.Roles {
			role := models.UserRole(strings.ToLower(strings.TrimSpace(roleStr)))
			roles = append(roles, role)
		}
	}
	roles = models.NormalizeRoles(roles)
	if !models.IsValidRoleList(roles) {
		http.Error(w, "invalid roles", http.StatusBadRequest)
		return
	}

	ttl := h.tokenTTL
	if payload.ExpiresInHours != nil {
		dur := *payload.ExpiresInHours
		if dur <= 0 || dur > 24*30 {
			http.Error(w, "expires_in_hours must be between 1 and 720", http.StatusBadRequest)
			return
		}
		ttl = time.Duration(dur) * time.Hour
	}

	expiresAt := time.Now().Add(ttl)
	token, err := generateInviteToken()
	if err != nil {
		http.Error(w, "failed to generate invite token", http.StatusInternalServerError)
		return
	}
	tokenHash := hashInviteToken(token)

	invite, err := h.inviteRepo.CreateInvite(models.Invite{
		TenantID:  tenant.ID,
		Email:     email,
		Roles:     roles,
		TokenHash: tokenHash,
		ExpiresAt: expiresAt,
		CreatedBy: createdBy,
	})
	if err != nil {
		http.Error(w, "failed to create invite: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if h.mailer == nil {
		http.Error(w, "email sender not configured", http.StatusInternalServerError)
		return
	}

	inviteURL := fmt.Sprintf(h.urlTpl, token)
	if err := h.mailer.SendInvite(invite.Email, tenant.Name, inviteURL); err != nil {
		http.Error(w, "failed to send invite email: "+err.Error(), http.StatusInternalServerError)
		return
	}

	response := struct {
		ID        string            `json:"id"`
		TenantID  string            `json:"tenant_id"`
		Email     string            `json:"email"`
		Roles     []models.UserRole `json:"roles"`
		Token     string            `json:"token"`
		ExpiresAt time.Time         `json:"expires_at"`
	}{
		ID:        invite.ID,
		TenantID:  invite.TenantID,
		Email:     invite.Email,
		Roles:     invite.Roles,
		Token:     token,
		ExpiresAt: invite.ExpiresAt,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func (h *InviteHandler) PreviewInvite(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimSpace(mux.Vars(r)["token"])
	if token == "" {
		http.Error(w, "token is required", http.StatusBadRequest)
		return
	}

	invite, err := h.inviteRepo.GetInviteByTokenHash(hashInviteToken(token))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "invite not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to load invite: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if invite.IsUsed() {
		http.Error(w, "invite already accepted", http.StatusConflict)
		return
	}
	if invite.IsExpired(time.Now()) {
		http.Error(w, "invite expired", http.StatusGone)
		return
	}

	tenant, err := h.tenantRepo.GetTenantByID(invite.TenantID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "tenant not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to load tenant: "+err.Error(), http.StatusInternalServerError)
		return
	}

	response := struct {
		Email      string            `json:"email"`
		TenantID   string            `json:"tenant_id"`
		TenantName string            `json:"tenant_name"`
		Roles      []models.UserRole `json:"roles"`
		ExpiresAt  time.Time         `json:"expires_at"`
	}{
		Email:      invite.Email,
		TenantID:   invite.TenantID,
		TenantName: tenant.Name,
		Roles:      invite.Roles,
		ExpiresAt:  invite.ExpiresAt,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (h *InviteHandler) AcceptInvite(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimSpace(mux.Vars(r)["token"])
	if token == "" {
		http.Error(w, "token is required", http.StatusBadRequest)
		return
	}

	var payload struct {
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid request payload", http.StatusBadRequest)
		return
	}

	invite, err := h.inviteRepo.GetInviteByTokenHash(hashInviteToken(token))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "invite not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to load invite: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if invite.IsUsed() {
		http.Error(w, "invite already accepted", http.StatusConflict)
		return
	}
	if invite.IsExpired(time.Now()) {
		http.Error(w, "invite expired", http.StatusGone)
		return
	}

	existingUser, err := h.userRepo.GetUserByEmail(invite.Email)
	switch {
	case err == nil:
		if existingUser.TenantID != invite.TenantID {
			http.Error(w, "user already belongs to a different tenant", http.StatusConflict)
			return
		}
		if !existingUser.IsActive {
			http.Error(w, "user is inactive", http.StatusConflict)
			return
		}
		mergedRoles := mergeRoles(existingUser.Roles, invite.Roles)
		if _, err := h.userRepo.UpdateUserRoles(existingUser.ID, mergedRoles); err != nil {
			http.Error(w, "failed to update user roles: "+err.Error(), http.StatusInternalServerError)
			return
		}
	case errors.Is(err, sql.ErrNoRows):
		password := strings.TrimSpace(payload.Password)
		if password == "" {
			http.Error(w, "password is required", http.StatusBadRequest)
			return
		}
		if _, err := h.userRepo.CreateUser(invite.TenantID, invite.Email, password, invite.Roles); err != nil {
			http.Error(w, "failed to create user: "+err.Error(), http.StatusInternalServerError)
			return
		}
	default:
		http.Error(w, "failed to load user: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if _, err := h.inviteRepo.MarkInviteAccepted(invite.ID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "invite no longer valid", http.StatusGone)
			return
		}
		http.Error(w, "failed to finalize invite: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func generateInviteToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func hashInviteToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func mergeRoles(existing, invited []models.UserRole) []models.UserRole {
	combined := make([]models.UserRole, 0, len(existing)+len(invited))
	combined = append(combined, existing...)
	combined = append(combined, invited...)
	normalized := models.NormalizeRoles(combined)
	return models.EnsureDefaultRole(normalized)
}
