package repository

import (
	"database/sql"

	"github.com/stanstork/stratum-api/internal/models"
)

type TenantRepository interface {
	CreateTenant(name string) (models.Tenant, error)
	GetTenantByID(id string) (models.Tenant, error)
}

type tenantRepository struct {
	db *sql.DB
}

func NewTenantRepository(db *sql.DB) TenantRepository {
	return &tenantRepository{db: db}
}

func (r *tenantRepository) CreateTenant(name string) (models.Tenant, error) {
	const query = `
		INSERT INTO tenant.tenants (name)
		VALUES ($1)
		RETURNING id, name, created_at, updated_at;
	`
	var tenant models.Tenant
	err := r.db.QueryRow(query, name).Scan(&tenant.ID, &tenant.Name, &tenant.CreatedAt, &tenant.UpdatedAt)
	return tenant, err
}

func (r *tenantRepository) GetTenantByID(id string) (models.Tenant, error) {
	const query = `
		SELECT id, name, created_at, updated_at
		FROM tenant.tenants
		WHERE id = $1;
	`
	var tenant models.Tenant
	err := r.db.QueryRow(query, id).Scan(&tenant.ID, &tenant.Name, &tenant.CreatedAt, &tenant.UpdatedAt)
	return tenant, err
}
