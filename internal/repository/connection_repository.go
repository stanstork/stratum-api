package repository

import (
	"database/sql"
	"fmt"

	"github.com/stanstork/stratum-api/internal/models"
	"github.com/stanstork/stratum-api/internal/utils"
)

type connectionRepository struct {
	db *sql.DB
}

type ConnectionRepository interface {
	List(tenantID string) ([]*models.Connection, error)
	Get(tenantID, id string) (*models.Connection, error)
	Create(conn *models.Connection) (*models.Connection, error)
	Update(conn *models.Connection) (*models.Connection, error)
	Delete(tenantID, id string) error
}

func NewConnectionRepository(db *sql.DB) ConnectionRepository {
	return &connectionRepository{db: db}
}

func (r *connectionRepository) List(tenantID string) ([]*models.Connection, error) {
	const q = `
SELECT id, tenant_id, name, data_format, host, port, username, password, db_name, status, created_at, updated_at
FROM tenant.connections
WHERE tenant_id = $1
ORDER BY name;
`
	rows, err := r.db.Query(q, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var conns []*models.Connection
	for rows.Next() {
		var c models.Connection
		var encPwd []byte
		if err := rows.Scan(
			&c.ID, &c.TenantID, &c.Name, &c.DataFormat,
			&c.Host, &c.Port, &c.Username, &encPwd, &c.DBName, &c.Status,
			&c.CreatedAt, &c.UpdatedAt,
		); err != nil {
			return nil, err
		}
		pwd, err := utils.DecryptPassword(encPwd)
		if err != nil {
			return nil, fmt.Errorf("decrypt password: %w", err)
		}
		c.Password = pwd
		conns = append(conns, &c)
	}
	return conns, rows.Err()
}

func (r *connectionRepository) Get(tenantID, id string) (*models.Connection, error) {
	const q = `
SELECT id, tenant_id, name, data_format, host, port, username, password, db_name, status, created_at, updated_at
FROM tenant.connections
WHERE id = $1 AND tenant_id = $2;
`
	var c models.Connection
	var encPwd []byte
	if err := r.db.QueryRow(q, id, tenantID).Scan(
		&c.ID, &c.TenantID, &c.Name, &c.DataFormat,
		&c.Host, &c.Port, &c.Username, &encPwd, &c.DBName, &c.Status,
		&c.CreatedAt, &c.UpdatedAt,
	); err != nil {
		return nil, err
	}
	pwd, err := utils.DecryptPassword(encPwd)
	if err != nil {
		return nil, fmt.Errorf("decrypt password: %w", err)
	}
	c.Password = pwd
	return &c, nil
}

func (r *connectionRepository) Create(conn *models.Connection) (*models.Connection, error) {
	encPwd, err := utils.EncryptPassword(conn.Password)
	if err != nil {
		return conn, fmt.Errorf("encrypt password: %w", err)
	}
	const q = `
INSERT INTO tenant.connections (
  tenant_id, name, data_format, host, port, username, password, db_name
)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
RETURNING id, tenant_id, created_at, updated_at;
`
	if err := r.db.QueryRow(
		q,
		conn.TenantID, conn.Name, conn.DataFormat,
		conn.Host, conn.Port, conn.Username, encPwd, conn.DBName,
	).Scan(&conn.ID, &conn.TenantID, &conn.CreatedAt, &conn.UpdatedAt); err != nil {
		return conn, err
	}
	return conn, nil
}

func (r *connectionRepository) Update(conn *models.Connection) (*models.Connection, error) {
	encPwd, err := utils.EncryptPassword(conn.Password)
	if err != nil {
		return conn, fmt.Errorf("encrypt password: %w", err)
	}
	const q = `
UPDATE tenant.connections
SET name = $1,
    data_format = $2,
	status = $3,
    host = $4,
    port = $5,
    username = $6,
    password = $7,
    db_name = $8,
    updated_at = now()
WHERE id = $9 AND tenant_id = $10
RETURNING tenant_id, created_at, updated_at;
`
	if err := r.db.QueryRow(
		q,
		conn.Name, conn.DataFormat, conn.Status,
		conn.Host, conn.Port, conn.Username, encPwd, conn.DBName,
		conn.ID, conn.TenantID,
	).Scan(&conn.TenantID, &conn.CreatedAt, &conn.UpdatedAt); err != nil {
		return conn, err
	}
	return conn, nil
}

func (r *connectionRepository) Delete(tenantID, id string) error {
	_, err := r.db.Exec("DELETE FROM tenant.connections WHERE id = $1 AND tenant_id = $2", id, tenantID)
	return err
}
