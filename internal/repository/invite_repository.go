package repository

import (
	"database/sql"

	"github.com/lib/pq"
	"github.com/stanstork/stratum-api/internal/models"
)

type InviteRepository interface {
	CreateInvite(invite models.Invite) (models.Invite, error)
	GetInviteByTokenHash(tokenHash string) (models.Invite, error)
	MarkInviteAccepted(inviteID string) (models.Invite, error)
	ListInvitesByTenant(tenantID string) ([]models.Invite, error)
	CancelInvite(inviteID, tenantID string) error
}

type inviteRepository struct {
	db *sql.DB
}

func NewInviteRepository(db *sql.DB) InviteRepository {
	return &inviteRepository{db: db}
}

func (r *inviteRepository) CreateInvite(invite models.Invite) (models.Invite, error) {
	const query = `
		INSERT INTO tenant.invites (tenant_id, email, roles, token_hash, created_by, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, tenant_id, email, roles, token_hash, created_by, created_at, updated_at, expires_at, accepted_at;
	`

	var (
		roles     pq.StringArray
		createdBy sql.NullString
	)
	var createdByValue interface{}
	if invite.CreatedBy != nil && *invite.CreatedBy != "" {
		createdByValue = *invite.CreatedBy
	}

	err := r.db.QueryRow(query,
		invite.TenantID,
		invite.Email,
		pq.Array(toStringSlice(invite.Roles)),
		invite.TokenHash,
		createdByValue,
		invite.ExpiresAt,
	).Scan(
		&invite.ID,
		&invite.TenantID,
		&invite.Email,
		&roles,
		&invite.TokenHash,
		&createdBy,
		&invite.CreatedAt,
		&invite.UpdatedAt,
		&invite.ExpiresAt,
		&invite.AcceptedAt,
	)
	if err != nil {
		return models.Invite{}, err
	}

	invite.Roles = toUserRoleSlice(roles)
	if createdBy.Valid {
		invite.CreatedBy = &createdBy.String
	} else {
		invite.CreatedBy = nil
	}

	return invite, nil
}

func (r *inviteRepository) GetInviteByTokenHash(tokenHash string) (models.Invite, error) {
	const query = `
		SELECT id, tenant_id, email, roles, token_hash, created_by, created_at, updated_at, expires_at, accepted_at
		FROM tenant.invites
		WHERE token_hash = $1;
	`

	var (
		invite    models.Invite
		roles     pq.StringArray
		createdBy sql.NullString
	)
	err := r.db.QueryRow(query, tokenHash).Scan(
		&invite.ID,
		&invite.TenantID,
		&invite.Email,
		&roles,
		&invite.TokenHash,
		&createdBy,
		&invite.CreatedAt,
		&invite.UpdatedAt,
		&invite.ExpiresAt,
		&invite.AcceptedAt,
	)
	if err != nil {
		return models.Invite{}, err
	}

	invite.Roles = toUserRoleSlice(roles)
	if createdBy.Valid {
		invite.CreatedBy = &createdBy.String
	} else {
		invite.CreatedBy = nil
	}

	return invite, nil
}

func (r *inviteRepository) MarkInviteAccepted(inviteID string) (models.Invite, error) {
	const query = `
		UPDATE tenant.invites
		SET accepted_at = now(), updated_at = now()
		WHERE id = $1 AND accepted_at IS NULL
		RETURNING id, tenant_id, email, roles, token_hash, created_by, created_at, updated_at, expires_at, accepted_at;
	`

	var (
		invite    models.Invite
		roles     pq.StringArray
		createdBy sql.NullString
	)
	err := r.db.QueryRow(query, inviteID).Scan(
		&invite.ID,
		&invite.TenantID,
		&invite.Email,
		&roles,
		&invite.TokenHash,
		&createdBy,
		&invite.CreatedAt,
		&invite.UpdatedAt,
		&invite.ExpiresAt,
		&invite.AcceptedAt,
	)
	if err != nil {
		return models.Invite{}, err
	}

	invite.Roles = toUserRoleSlice(roles)
	if createdBy.Valid {
		invite.CreatedBy = &createdBy.String
	} else {
		invite.CreatedBy = nil
	}

	return invite, nil
}

func (r *inviteRepository) ListInvitesByTenant(tenantID string) ([]models.Invite, error) {
	const query = `
		SELECT id, tenant_id, email, roles, token_hash, created_by, created_at, updated_at, expires_at, accepted_at
		FROM tenant.invites
		WHERE tenant_id = $1
		ORDER BY created_at DESC;
	`

	rows, err := r.db.Query(query, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var invites []models.Invite
	for rows.Next() {
		var (
			invite    models.Invite
			roles     pq.StringArray
			createdBy sql.NullString
		)
		if err := rows.Scan(
			&invite.ID,
			&invite.TenantID,
			&invite.Email,
			&roles,
			&invite.TokenHash,
			&createdBy,
			&invite.CreatedAt,
			&invite.UpdatedAt,
			&invite.ExpiresAt,
			&invite.AcceptedAt,
		); err != nil {
			return nil, err
		}

		invite.Roles = toUserRoleSlice(roles)
		if createdBy.Valid {
			invite.CreatedBy = &createdBy.String
		} else {
			invite.CreatedBy = nil
		}

		invites = append(invites, invite)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return invites, nil
}

func (r *inviteRepository) CancelInvite(inviteID, tenantID string) error {
	const query = `
		DELETE FROM tenant.invites
		WHERE id = $1 AND tenant_id = $2 AND accepted_at IS NULL;
	`

	result, err := r.db.Exec(query, inviteID, tenantID)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return sql.ErrNoRows
	}

	return nil
}
