package models

type User struct {
	ID           string `json:"id"`
	TenantID     string `json:"tenant_id"`
	Email        string `json:"email"`
	PasswordHash string `json:"password_hash"`
	IsActive     bool   `json:"is_active"`
}
