package handlers

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/stanstork/stratum-api/internal/authz"
	"github.com/stanstork/stratum-api/internal/models"
	"github.com/stanstork/stratum-api/internal/repository"
)

type TenantHandler struct {
	tenantRepo repository.TenantRepository
	userRepo   repository.UserRepository
}

func NewTenantHandler(tenantRepo repository.TenantRepository, userRepo repository.UserRepository) *TenantHandler {
	return &TenantHandler{
		tenantRepo: tenantRepo,
		userRepo:   userRepo,
	}
}

func (h *TenantHandler) CreateTenant(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}
	payload.Name = strings.TrimSpace(payload.Name)
	if payload.Name == "" {
		http.Error(w, "Tenant name is required", http.StatusBadRequest)
		return
	}

	tenant, err := h.tenantRepo.CreateTenant(payload.Name)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate") {
			http.Error(w, "Tenant name already exists", http.StatusConflict)
			return
		}
		http.Error(w, "Failed to create tenant: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(tenant)
}

func (h *TenantHandler) AddUser(w http.ResponseWriter, r *http.Request) {
	requesterRoles, _ := authz.RolesFromRequest(r)
	isSuperAdmin := models.HasAtLeast(requesterRoles, models.RoleSuperAdmin)

	tenantID := mux.Vars(r)["tenantID"]
	if tenantID == "" {
		http.Error(w, "Tenant ID is required", http.StatusBadRequest)
		return
	}

	if !isSuperAdmin {
		if tid, ok := authz.TenantIDFromRequest(r); !ok || tid != tenantID {
			http.Error(w, "insufficient permissions for tenant", http.StatusForbidden)
			return
		}
	}

	if _, err := h.tenantRepo.GetTenantByID(tenantID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "Tenant not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to load tenant: "+err.Error(), http.StatusInternalServerError)
		return
	}

	var payload struct {
		Email    string   `json:"email"`
		Password string   `json:"password"`
		Role     string   `json:"role"`
		Roles    []string `json:"roles"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	payload.Email = strings.TrimSpace(payload.Email)
	if payload.Email == "" || payload.Password == "" {
		http.Error(w, "Email and password are required", http.StatusBadRequest)
		return
	}

	var roles []models.UserRole
	if len(payload.Roles) > 0 {
		for _, roleStr := range payload.Roles {
			role := models.UserRole(strings.ToLower(strings.TrimSpace(roleStr)))
			roles = append(roles, role)
		}
	} else if payload.Role != "" {
		roles = []models.UserRole{models.UserRole(strings.ToLower(strings.TrimSpace(payload.Role)))}
	} else {
		roles = []models.UserRole{models.RoleViewer}
	}
	roles = models.NormalizeRoles(roles)
	if !models.IsValidRoleList(roles) {
		http.Error(w, "Invalid roles", http.StatusBadRequest)
		return
	}

	if !isSuperAdmin && models.HasAtLeast(roles, models.RoleSuperAdmin) {
		http.Error(w, "insufficient permissions to assign role", http.StatusForbidden)
		return
	}

	user, err := h.userRepo.CreateUser(tenantID, payload.Email, payload.Password, roles)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate") {
			http.Error(w, "User already exists", http.StatusConflict)
			return
		}
		http.Error(w, "Failed to create user: "+err.Error(), http.StatusInternalServerError)
		return
	}

	response := struct {
		ID       string            `json:"id"`
		TenantID string            `json:"tenant_id"`
		Email    string            `json:"email"`
		IsActive bool              `json:"is_active"`
		Roles    []models.UserRole `json:"roles"`
	}{
		ID:       user.ID,
		TenantID: user.TenantID,
		Email:    user.Email,
		IsActive: user.IsActive,
		Roles:    user.Roles,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}
