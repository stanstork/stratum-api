package models

import "time"

// Invite represents a pending invitation to join a tenant.
type Invite struct {
	ID         string     `json:"id"`
	TenantID   string     `json:"tenant_id"`
	Email      string     `json:"email"`
	Roles      []UserRole `json:"roles"`
	TokenHash  string     `json:"-"`
	ExpiresAt  time.Time  `json:"expires_at"`
	AcceptedAt *time.Time `json:"accepted_at,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
	CreatedBy  *string    `json:"created_by,omitempty"`
}

// IsExpired determines whether the invite has expired.
func (i Invite) IsExpired(now time.Time) bool {
	return now.After(i.ExpiresAt)
}

// IsUsed indicates whether the invite has already been accepted.
func (i Invite) IsUsed() bool {
	return i.AcceptedAt != nil
}
