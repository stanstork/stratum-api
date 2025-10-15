package models

// UserRole represents the permission tier for a user within a tenant.
type UserRole string

const (
	RoleViewer     UserRole = "viewer"
	RoleEditor     UserRole = "editor"
	RoleAdmin      UserRole = "admin"
	RoleSuperAdmin UserRole = "super_admin"
)

// AllUserRoles enumerates valid roles for quick validation checks.
var AllUserRoles = []UserRole{
	RoleViewer,
	RoleEditor,
	RoleAdmin,
	RoleSuperAdmin,
}

var roleRank = map[UserRole]int{
	RoleViewer:     1,
	RoleEditor:     2,
	RoleAdmin:      3,
	RoleSuperAdmin: 4,
}

// IsValidRole returns true if the provided role exists in the allowed set.
func IsValidRole(role UserRole) bool {
	_, ok := roleRank[role]
	return ok
}

// IsValidRoleList returns true when every role in the slice is recognised.
func IsValidRoleList(roles []UserRole) bool {
	if len(roles) == 0 {
		return false
	}
	for _, role := range roles {
		if !IsValidRole(role) {
			return false
		}
	}
	return true
}

// NormalizeRoles lowercases, trims, and deduplicates a slice of roles.
func NormalizeRoles(roles []UserRole) []UserRole {
	seen := make(map[UserRole]struct{}, len(roles))
	var result []UserRole
	for _, role := range roles {
		normalized := UserRole(role)
		if !IsValidRole(normalized) {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		result = append(result, normalized)
	}
	return result
}

// EnsureDefaultRole returns a viewer role when the slice is empty.
func EnsureDefaultRole(roles []UserRole) []UserRole {
	if len(roles) == 0 {
		return []UserRole{RoleViewer}
	}
	return roles
}

// HasAtLeast returns true when any of the input roles meets or exceeds the required tier.
func HasAtLeast(roles []UserRole, required UserRole) bool {
	reqRank, ok := roleRank[required]
	if !ok {
		return false
	}
	for _, role := range roles {
		if roleRank[role] >= reqRank {
			return true
		}
	}
	return false
}

// HighestRole returns the top tier within the supplied role set.
func HighestRole(roles []UserRole) UserRole {
	highest := RoleViewer
	highestRank := roleRank[highest]
	for _, role := range roles {
		if rank := roleRank[role]; rank > highestRank {
			highest = role
			highestRank = rank
		}
	}
	return highest
}

type User struct {
	ID           string     `json:"id"`
	TenantID     string     `json:"tenant_id"`
	Email        string     `json:"email"`
	FirstName    string     `json:"first_name"`
	LastName     string     `json:"last_name"`
	PasswordHash string     `json:"password_hash"`
	IsActive     bool       `json:"is_active"`
	Roles        []UserRole `json:"roles"`
}
