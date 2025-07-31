package repository

import (
	"database/sql"
	"fmt"

	"github.com/stanstork/stratum-api/internal/models"
	"github.com/stanstork/stratum-api/internal/utils"
)

type ConnectionRepository interface {
	List() ([]*models.Connection, error)
	Get(id string) (*models.Connection, error)
	Create(conn *models.Connection) (*models.Connection, error)
	Update(conn *models.Connection) (*models.Connection, error)
	Delete(id string) error
}

type connectionRepository struct {
	db *sql.DB
}

func NewConnectionRepository(db *sql.DB) ConnectionRepository {
	return &connectionRepository{db: db}
}

func (r *connectionRepository) List() ([]*models.Connection, error) {
	const q = `
SELECT id, name, data_format, host, port, username, password, db_name, status, created_at, updated_at
FROM tenant.connections
ORDER BY name;
`
	rows, err := r.db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var conns []*models.Connection
	for rows.Next() {
		var c models.Connection
		var encPwd []byte
		if err := rows.Scan(
			&c.ID, &c.Name, &c.DataFormat,
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

func (r *connectionRepository) Get(id string) (*models.Connection, error) {
	const q = `
SELECT id, name, data_format, host, port, username, password, db_name, created_at, updated_at
FROM tenant.connections
WHERE id = $1;
`
	var c models.Connection
	var encPwd []byte
	if err := r.db.QueryRow(q, id).Scan(
		&c.ID, &c.Name, &c.DataFormat,
		&c.Host, &c.Port, &c.Username, &encPwd, &c.DBName,
		&c.CreatedAt, &c.UpdatedAt,
	); err != nil {
		return &c, err
	}
	pwd, err := utils.DecryptPassword(encPwd)
	if err != nil {
		return &c, fmt.Errorf("decrypt password: %w", err)
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
  name, data_format, host, port, username, password, db_name
)
VALUES ($1,$2,$3,$4,$5,$6,$7)
RETURNING id, created_at, updated_at;
`
	if err := r.db.QueryRow(
		q,
		conn.Name, conn.DataFormat,
		conn.Host, conn.Port, conn.Username, encPwd, conn.DBName,
	).Scan(&conn.ID, &conn.CreatedAt, &conn.UpdatedAt); err != nil {
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
WHERE id = $9
RETURNING created_at, updated_at;
`
	if err := r.db.QueryRow(
		q,
		conn.Name, conn.DataFormat, conn.Status,
		conn.Host, conn.Port, conn.Username, encPwd, conn.DBName,
		conn.ID,
	).Scan(&conn.CreatedAt, &conn.UpdatedAt); err != nil {
		return conn, err
	}
	return conn, nil
}

func (r *connectionRepository) Delete(id string) error {
	_, err := r.db.Exec("DELETE FROM tenant.connections WHERE id = $1", id)
	if err != nil {
		return err
	}
	return nil
}
