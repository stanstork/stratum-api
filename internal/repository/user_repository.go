package repository

import (
	"database/sql"
	"errors"

	"github.com/stanstork/stratum-api/internal/models"
	"golang.org/x/crypto/bcrypt"
)

type UserRepository interface {
	CreateUser(tenantID, email, password string) (models.User, error)
	AuthenticateUser(tenantID, email, password string) (models.User, error)
}

type userRepository struct {
	db *sql.DB
}

func NewUserRepository(db *sql.DB) UserRepository {
	return &userRepository{db: db}
}

func (u *userRepository) CreateUser(tenantID string, email string, password string) (models.User, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return models.User{}, err
	}

	user := models.User{
		TenantID:     tenantID,
		Email:        email,
		PasswordHash: string(hash),
		IsActive:     true,
	}

	query := `INSERT INTO tenant.users (tenant_id, email, password_hash, is_active) VALUES ($1,$2,$3,$4) RETURNING id`
	err = u.db.QueryRow(query, user.TenantID, user.Email, user.PasswordHash, user.IsActive).Scan(&user.ID)
	if err != nil {
		return models.User{}, err
	}

	return user, nil
}

func (u *userRepository) AuthenticateUser(tenantID string, email string, password string) (models.User, error) {
	var user models.User

	query := `SELECT id, tenant_id, email, password_hash, is_active FROM tenant.users WHERE tenant_id = $1 AND email = $2`
	err := u.db.QueryRow(query, tenantID, email).Scan(&user.ID, &user.TenantID, &user.Email, &user.PasswordHash, &user.IsActive)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return models.User{}, errors.New("invalid credentials")
		}
		return models.User{}, err
	}

	if !user.IsActive {
		return models.User{}, errors.New("user is inactive")
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)); err != nil {
		return models.User{}, errors.New("invalid credentials")
	}

	user.TenantID = tenantID
	user.Email = email

	return user, nil
}
