package repository

import (
	"database/sql"
	"errors"
	"strings"

	"github.com/lib/pq"
	"github.com/stanstork/stratum-api/internal/models"
	"golang.org/x/crypto/bcrypt"
)

type UserRepository interface {
	CreateUser(tenantID, email, password, firstName, lastName string, roles []models.UserRole) (models.User, error)
	AuthenticateUser(email, password string) (models.User, error)
	ListUsersByTenant(tenantID string) ([]models.User, error)
	GetUserByEmail(email string) (models.User, error)
	GetUserByID(userID string) (models.User, error)
	UpdateUserRoles(userID string, roles []models.UserRole) (models.User, error)
	DeleteUser(userID string) error
}

type userRepository struct {
	db *sql.DB
}

func NewUserRepository(db *sql.DB) UserRepository {
	return &userRepository{db: db}
}

func (u *userRepository) CreateUser(tenantID string, email string, password string, firstName string, lastName string, roles []models.UserRole) (models.User, error) {
	if len(roles) == 0 {
		roles = []models.UserRole{models.RoleViewer}
	}
	if !models.IsValidRoleList(roles) {
		return models.User{}, errors.New("invalid roles")
	}
	normalized := models.EnsureDefaultRole(models.NormalizeRoles(roles))

	firstName = strings.TrimSpace(firstName)
	lastName = strings.TrimSpace(lastName)

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return models.User{}, err
	}

	user := models.User{
		TenantID:     tenantID,
		Email:        email,
		FirstName:    firstName,
		LastName:     lastName,
		PasswordHash: string(hash),
		IsActive:     true,
		Roles:        normalized,
	}

	query := `
		INSERT INTO tenant.users (tenant_id, email, first_name, last_name, password_hash, is_active, roles)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id`
	err = u.db.QueryRow(query, user.TenantID, user.Email, user.FirstName, user.LastName, user.PasswordHash, user.IsActive, pq.Array(toStringSlice(user.Roles))).Scan(&user.ID)
	if err != nil {
		return models.User{}, err
	}

	return user, nil
}

func (u *userRepository) AuthenticateUser(email string, password string) (models.User, error) {
	var user models.User
	var roles pq.StringArray

	query := `
		SELECT id, tenant_id, email, first_name, last_name, password_hash, is_active, roles
		FROM tenant.users
		WHERE email = $1 AND deleted_at IS NULL`
	err := u.db.QueryRow(query, email).Scan(
		&user.ID,
		&user.TenantID,
		&user.Email,
		&user.FirstName,
		&user.LastName,
		&user.PasswordHash,
		&user.IsActive,
		&roles,
	)
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

func (u *userRepository) GetUserByEmail(email string) (models.User, error) {
	var user models.User
	var roles pq.StringArray

	const query = `
		SELECT id, tenant_id, email, first_name, last_name, password_hash, is_active, roles
		FROM tenant.users
		WHERE email = $1 AND deleted_at IS NULL`

	err := u.db.QueryRow(query, email).Scan(
		&user.ID,
		&user.TenantID,
		&user.Email,
		&user.FirstName,
		&user.LastName,
		&user.PasswordHash,
		&user.IsActive,
		&roles,
	)
	if err != nil {
		return models.User{}, err
	}

	user.Roles = models.EnsureDefaultRole(toUserRoleSlice(roles))
	if !models.IsValidRoleList(user.Roles) {
		return models.User{}, errors.New("user has invalid roles")
	}

	return user, nil
}

func (u *userRepository) GetUserByID(userID string) (models.User, error) {
	var user models.User
	var roles pq.StringArray

	const query = `
		SELECT id, tenant_id, email, first_name, last_name, password_hash, is_active, roles
		FROM tenant.users
		WHERE id = $1 AND deleted_at IS NULL`

	err := u.db.QueryRow(query, userID).Scan(
		&user.ID,
		&user.TenantID,
		&user.Email,
		&user.FirstName,
		&user.LastName,
		&user.PasswordHash,
		&user.IsActive,
		&roles,
	)
	if err != nil {
		return models.User{}, err
	}

	user.Roles = models.EnsureDefaultRole(toUserRoleSlice(roles))
	if !models.IsValidRoleList(user.Roles) {
		return models.User{}, errors.New("user has invalid roles")
	}

	return user, nil
}

func (u *userRepository) UpdateUserRoles(userID string, roles []models.UserRole) (models.User, error) {
	if len(roles) == 0 {
		return models.User{}, errors.New("roles cannot be empty")
	}

	normalized := models.EnsureDefaultRole(models.NormalizeRoles(roles))
	if !models.IsValidRoleList(normalized) {
		return models.User{}, errors.New("invalid roles")
	}

	const query = `
		UPDATE tenant.users
		SET roles = $2, updated_at = now()
		WHERE id = $1 AND deleted_at IS NULL
		RETURNING id, tenant_id, email, first_name, last_name, password_hash, is_active, roles
	`

	var user models.User
	var updatedRoles pq.StringArray
	err := u.db.QueryRow(query, userID, pq.Array(toStringSlice(normalized))).Scan(
		&user.ID,
		&user.TenantID,
		&user.Email,
		&user.FirstName,
		&user.LastName,
		&user.PasswordHash,
		&user.IsActive,
		&updatedRoles,
	)
	if err != nil {
		return models.User{}, err
	}

	user.Roles = models.EnsureDefaultRole(toUserRoleSlice(updatedRoles))
	if !models.IsValidRoleList(user.Roles) {
		return models.User{}, errors.New("user has invalid roles after update")
	}

	return user, nil
}

func (u *userRepository) DeleteUser(userID string) error {
	const query = `
		UPDATE tenant.users
		SET is_active = FALSE, deleted_at = now(), updated_at = now()
		WHERE id = $1 AND deleted_at IS NULL`

	result, err := u.db.Exec(query, userID)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return sql.ErrNoRows
	}

	return nil
}

func (u *userRepository) ListUsersByTenant(tenantID string) ([]models.User, error) {
	const query = `
		SELECT id, tenant_id, email, first_name, last_name, is_active, roles
		FROM tenant.users
		WHERE tenant_id = $1 AND deleted_at IS NULL
		ORDER BY email`

	rows, err := u.db.Query(query, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []models.User
	for rows.Next() {
		var user models.User
		var roles pq.StringArray

		if err := rows.Scan(&user.ID, &user.TenantID, &user.Email, &user.FirstName, &user.LastName, &user.IsActive, &roles); err != nil {
			return nil, err
		}

		normalized := models.EnsureDefaultRole(toUserRoleSlice(roles))
		if !models.IsValidRoleList(normalized) {
			return nil, errors.New("encountered user with invalid roles")
		}
		user.Roles = normalized

		users = append(users, user)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return users, nil
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
