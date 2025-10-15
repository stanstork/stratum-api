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

type tenantUserResponse struct {
	ID        string            `json:"id"`
	TenantID  string            `json:"tenant_id"`
	Email     string            `json:"email"`
	FirstName string            `json:"first_name"`
	LastName  string            `json:"last_name"`
	IsActive  bool              `json:"is_active"`
	Roles     []models.UserRole `json:"roles"`
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
		Email     string   `json:"email"`
		Password  string   `json:"password"`
		Role      string   `json:"role"`
		Roles     []string `json:"roles"`
		FirstName string   `json:"first_name"`
		LastName  string   `json:"last_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	firstName := strings.TrimSpace(payload.FirstName)
	lastName := strings.TrimSpace(payload.LastName)

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

	user, err := h.userRepo.CreateUser(tenantID, payload.Email, payload.Password, firstName, lastName, roles)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate") {
			http.Error(w, "User already exists", http.StatusConflict)
			return
		}
		http.Error(w, "Failed to create user: "+err.Error(), http.StatusInternalServerError)
		return
	}

	response := struct {
		ID        string            `json:"id"`
		TenantID  string            `json:"tenant_id"`
		Email     string            `json:"email"`
		FirstName string            `json:"first_name"`
		LastName  string            `json:"last_name"`
		IsActive  bool              `json:"is_active"`
		Roles     []models.UserRole `json:"roles"`
	}{
		ID:        user.ID,
		TenantID:  user.TenantID,
		Email:     user.Email,
		FirstName: user.FirstName,
		LastName:  user.LastName,
		IsActive:  user.IsActive,
		Roles:     user.Roles,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func (h *TenantHandler) ListUsers(w http.ResponseWriter, r *http.Request) {
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

	h.writeTenantUsersResponse(w, tenantID)
}

func (h *TenantHandler) ListCurrentTenantUsers(w http.ResponseWriter, r *http.Request) {
	requesterRoles, _ := authz.RolesFromRequest(r)
	isTenantAdmin := models.HasAtLeast(requesterRoles, models.RoleAdmin)

	tenantID, ok := authz.TenantIDFromRequest(r)
	if !ok || tenantID == "" {
		http.Error(w, "tenant context missing", http.StatusForbidden)
		return
	}

	if !isTenantAdmin {
		http.Error(w, "insufficient permissions for tenant", http.StatusForbidden)
		return
	}

	h.writeTenantUsersResponse(w, tenantID)
}

func (h *TenantHandler) writeTenantUsersResponse(w http.ResponseWriter, tenantID string) {
	if _, err := h.tenantRepo.GetTenantByID(tenantID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "Tenant not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to load tenant: "+err.Error(), http.StatusInternalServerError)
		return
	}

	users, err := h.userRepo.ListUsersByTenant(tenantID)
	if err != nil {
		http.Error(w, "Failed to list users: "+err.Error(), http.StatusInternalServerError)
		return
	}

	response := make([]tenantUserResponse, 0, len(users))
	for _, user := range users {
		response = append(response, tenantUserResponse{
			ID:        user.ID,
			TenantID:  user.TenantID,
			Email:     user.Email,
			FirstName: user.FirstName,
			LastName:  user.LastName,
			IsActive:  user.IsActive,
			Roles:     user.Roles,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
}

func (h *TenantHandler) UpdateUserRoles(w http.ResponseWriter, r *http.Request) {
	userID := mux.Vars(r)["userID"]
	if strings.TrimSpace(userID) == "" {
		http.Error(w, "User ID is required", http.StatusBadRequest)
		return
	}

	requesterRoles, _ := authz.RolesFromRequest(r)
	isSuperAdmin := models.HasAtLeast(requesterRoles, models.RoleSuperAdmin)

	existingUser, err := h.userRepo.GetUserByID(userID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "User not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to load user: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if !isSuperAdmin {
		requesterTenantID, ok := authz.TenantIDFromRequest(r)
		if !ok || requesterTenantID == "" {
			http.Error(w, "tenant context missing", http.StatusForbidden)
			return
		}
		if existingUser.TenantID != requesterTenantID {
			http.Error(w, "insufficient permissions for tenant", http.StatusForbidden)
			return
		}
	}

	var payload struct {
		Roles []string `json:"roles"`
		Role  string   `json:"role"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
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
	}

	roles = models.NormalizeRoles(roles)
	if !models.IsValidRoleList(roles) {
		http.Error(w, "Invalid roles", http.StatusBadRequest)
		return
	}

	updatedUser, err := h.userRepo.UpdateUserRoles(existingUser.ID, roles)
	if err != nil {
		if strings.Contains(err.Error(), "invalid roles") || strings.Contains(err.Error(), "cannot be empty") {
			http.Error(w, "Invalid roles", http.StatusBadRequest)
			return
		}
		http.Error(w, "Failed to update user roles: "+err.Error(), http.StatusInternalServerError)
		return
	}

	response := tenantUserResponse{
		ID:        updatedUser.ID,
		TenantID:  updatedUser.TenantID,
		Email:     updatedUser.Email,
		FirstName: updatedUser.FirstName,
		LastName:  updatedUser.LastName,
		IsActive:  updatedUser.IsActive,
		Roles:     updatedUser.Roles,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
}

func (h *TenantHandler) DeleteUser(w http.ResponseWriter, r *http.Request) {
	userID := mux.Vars(r)["userID"]
	if strings.TrimSpace(userID) == "" {
		http.Error(w, "User ID is required", http.StatusBadRequest)
		return
	}

	requesterRoles, _ := authz.RolesFromRequest(r)
	isSuperAdmin := models.HasAtLeast(requesterRoles, models.RoleSuperAdmin)

	existingUser, err := h.userRepo.GetUserByID(userID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "User not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to load user: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if !isSuperAdmin {
		requesterTenantID, ok := authz.TenantIDFromRequest(r)
		if !ok || requesterTenantID == "" {
			http.Error(w, "tenant context missing", http.StatusForbidden)
			return
		}
		if existingUser.TenantID != requesterTenantID {
			http.Error(w, "insufficient permissions for tenant", http.StatusForbidden)
			return
		}
	}

	if err := h.userRepo.DeleteUser(existingUser.ID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "User not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to delete user: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
