package handlers

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
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
	tenantID := mux.Vars(r)["tenantID"]
	if tenantID == "" {
		http.Error(w, "Tenant ID is required", http.StatusBadRequest)
		return
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
		Email    string `json:"email"`
		Password string `json:"password"`
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

	user, err := h.userRepo.CreateUser(tenantID, payload.Email, payload.Password)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate") {
			http.Error(w, "User already exists", http.StatusConflict)
			return
		}
		http.Error(w, "Failed to create user: "+err.Error(), http.StatusInternalServerError)
		return
	}

	response := struct {
		ID       string `json:"id"`
		TenantID string `json:"tenant_id"`
		Email    string `json:"email"`
		IsActive bool   `json:"is_active"`
	}{
		ID:       user.ID,
		TenantID: user.TenantID,
		Email:    user.Email,
		IsActive: user.IsActive,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}
