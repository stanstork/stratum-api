package handlers

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/rs/zerolog"
	"github.com/stanstork/stratum-api/internal/authz"
	"github.com/stanstork/stratum-api/internal/config"
	"github.com/stanstork/stratum-api/internal/models"
	"github.com/stanstork/stratum-api/internal/repository"
)

type AuthHandler struct {
	userRepository repository.UserRepository
	jwtSecret      string
	logger         zerolog.Logger
}

type signupRequest struct {
	TenantID  string `json:"tenant_id"`
	Email     string `json:"email"`
	Password  string `json:"password"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}

type loginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

func NewAuthHandler(db *sql.DB, cfg *config.Config, logger zerolog.Logger) *AuthHandler {
	return &AuthHandler{
		userRepository: repository.NewUserRepository(db),
		jwtSecret:      cfg.JWTSecret,
		logger:         logger,
	}
}

func (h *AuthHandler) SignUp(w http.ResponseWriter, r *http.Request) {
	var req signupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	req.Email = strings.TrimSpace(req.Email)
	req.FirstName = strings.TrimSpace(req.FirstName)
	req.LastName = strings.TrimSpace(req.LastName)

	user, err := h.userRepository.CreateUser(req.TenantID, req.Email, req.Password, req.FirstName, req.LastName, []models.UserRole{models.RoleViewer})
	if err != nil {
		http.Error(w, "Failed to create user: "+err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(models.User{ID: user.ID, Email: user.Email, TenantID: user.TenantID, Roles: user.Roles})
}

func (h *AuthHandler) Login(w http.ResponseWriter, r *http.Request) {
	var req loginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	user, err := h.userRepository.AuthenticateUser(req.Email, req.Password)
	if err != nil {
		http.Error(w, "Authentication failed: "+err.Error(), http.StatusUnauthorized)
		return
	}

	rolesClaim := make([]string, 0, len(user.Roles))
	for _, role := range user.Roles {
		rolesClaim = append(rolesClaim, string(role))
	}
	highest := models.HighestRole(user.Roles)

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub":   user.ID,
		"tid":   user.TenantID,
		"role":  string(highest),
		"roles": rolesClaim,
		"exp":   time.Now().Add(24 * time.Hour).Unix(),
	})
	tokenString, err := token.SignedString([]byte(h.jwtSecret))
	if err != nil {
		http.Error(w, "Failed to generate token: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"token": tokenString})
}

func (h *AuthHandler) JWTMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth == "" {
			http.Error(w, "Authorization header required", http.StatusUnauthorized)
			return
		}
		parts := strings.SplitN(auth, " ", 2)
		if len(parts) != 2 || parts[0] != "Bearer" {
			http.Error(w, "Invalid authorization format", http.StatusUnauthorized)
			return
		}
		tokenString := parts[1]
		token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, jwt.ErrSignatureInvalid
			}
			return []byte(h.jwtSecret), nil
		})
		if err != nil || !token.Valid {
			http.Error(w, "Invalid token: "+err.Error(), http.StatusUnauthorized)
			return
		}
		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok || !claims.VerifyExpiresAt(time.Now().Unix(), true) {
			http.Error(w, "Token expired", http.StatusUnauthorized)
			return
		}
		userRoles, ok := extractRolesFromClaims(claims)
		if !ok {
			http.Error(w, "Missing role claim", http.StatusUnauthorized)
			return
		}

		tenantID, ok := claims["tid"].(string)
		if !ok {
			http.Error(w, "Missing token claim", http.StatusUnauthorized)
			return
		}
		userID, _ := claims["sub"].(string)
		ctx := authz.WithIdentity(r.Context(), tenantID, userID, userRoles)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func extractRolesFromClaims(claims jwt.MapClaims) ([]models.UserRole, bool) {
	rawRoles, ok := claims["roles"]
	if !ok {
		if single, ok := claims["role"].(string); ok && single != "" {
			role := models.UserRole(single)
			if !models.IsValidRole(role) {
				return nil, false
			}
			return []models.UserRole{role}, true
		}
		return nil, false
	}

	var roles []models.UserRole
	switch v := rawRoles.(type) {
	case []interface{}:
		for _, val := range v {
			str, ok := val.(string)
			if !ok {
				return nil, false
			}
			role := models.UserRole(str)
			if !models.IsValidRole(role) {
				return nil, false
			}
			roles = append(roles, role)
		}
	case []string:
		for _, str := range v {
			role := models.UserRole(str)
			if !models.IsValidRole(role) {
				return nil, false
			}
			roles = append(roles, role)
		}
	case string:
		roles = []models.UserRole{models.UserRole(v)}
	default:
		return nil, false
	}

	if !models.IsValidRoleList(roles) {
		return nil, false
	}
	normalized := models.EnsureDefaultRole(models.NormalizeRoles(roles))
	if !models.IsValidRoleList(normalized) {
		return nil, false
	}
	return normalized, true
}
