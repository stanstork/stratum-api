package repository

import (
	"database/sql"
	"errors"

	"github.com/lib/pq"
	"github.com/stanstork/stratum-api/internal/models"
	"golang.org/x/crypto/bcrypt"
)

type UserRepository interface {
	CreateUser(tenantID, email, password string, roles []models.UserRole) (models.User, error)
	AuthenticateUser(email, password string) (models.User, error)
}

type userRepository struct {
	db *sql.DB
}

func NewUserRepository(db *sql.DB) UserRepository {
	return &userRepository{db: db}
}

func (u *userRepository) CreateUser(tenantID string, email string, password string, roles []models.UserRole) (models.User, error) {
	if len(roles) == 0 {
		roles = []models.UserRole{models.RoleViewer}
	}
	if !models.IsValidRoleList(roles) {
		return models.User{}, errors.New("invalid roles")
	}
	normalized := models.EnsureDefaultRole(models.NormalizeRoles(roles))

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return models.User{}, err
	}

	user := models.User{
		TenantID:     tenantID,
		Email:        email,
		PasswordHash: string(hash),
		IsActive:     true,
		Roles:        normalized,
	}

	query := `
		INSERT INTO tenant.users (tenant_id, email, password_hash, is_active, roles)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id`
	err = u.db.QueryRow(query, user.TenantID, user.Email, user.PasswordHash, user.IsActive, pq.Array(toStringSlice(user.Roles))).Scan(&user.ID)
	if err != nil {
		return models.User{}, err
	}

	return user, nil
}

func (u *userRepository) AuthenticateUser(email string, password string) (models.User, error) {
	var user models.User
	var roles pq.StringArray

	query := `
		SELECT id, tenant_id, email, password_hash, is_active, roles
		FROM tenant.users
		WHERE email = $1`
	err := u.db.QueryRow(query, email).Scan(&user.ID, &user.TenantID, &user.Email, &user.PasswordHash, &user.IsActive, &roles)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return models.User{}, errors.New("invalid credentials")
		}
		return models.User{}, err
	}

	user.Roles = toUserRoleSlice(roles)
	user.Roles = models.EnsureDefaultRole(models.NormalizeRoles(user.Roles))
	if !models.IsValidRoleList(user.Roles) {
		return models.User{}, errors.New("user has invalid roles")
	}

	if !user.IsActive {
		return models.User{}, errors.New("user is inactive")
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)); err != nil {
		return models.User{}, errors.New("invalid credentials")
	}

	return user, nil
}

func toStringSlice(roles []models.UserRole) []string {
	result := make([]string, 0, len(roles))
	for _, role := range roles {
		result = append(result, string(role))
	}
	return result
}

func toUserRoleSlice(roles []string) []models.UserRole {
	result := make([]models.UserRole, 0, len(roles))
	for _, role := range roles {
		result = append(result, models.UserRole(role))
	}
	return models.NormalizeRoles(result)
}
