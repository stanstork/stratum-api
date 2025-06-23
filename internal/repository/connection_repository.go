package repository

import (
	"database/sql"

	"github.com/stanstork/stratum-api/internal/models"
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
	rows, err := r.db.Query("SELECT id, name, data_format, conn_string, status, created_at, updated_at FROM tenant.connections")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var connections []*models.Connection
	for rows.Next() {
		conn := &models.Connection{}
		if err := rows.Scan(&conn.ID, &conn.Name, &conn.DataFormat, &conn.ConnString, &conn.Status, &conn.CreatedAt, &conn.UpdatedAt); err != nil {
			return nil, err
		}
		connections = append(connections, conn)
	}

	return connections, nil
}

func (r *connectionRepository) Get(id string) (*models.Connection, error) {
	conn := &models.Connection{}
	err := r.db.QueryRow("SELECT id, name, data_format, conn_string, status, created_at, updated_at FROM tenant.connections WHERE id = $1", id).Scan(
		&conn.ID, &conn.Name, &conn.DataFormat, &conn.ConnString, &conn.Status, &conn.CreatedAt, &conn.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // Not found
		}
		return nil, err
	}
	return conn, nil
}

func (r *connectionRepository) Create(conn *models.Connection) (*models.Connection, error) {
	err := r.db.QueryRow(
		"INSERT INTO tenant.connections (name, data_format, conn_string, status) VALUES ($1, $2, $3, $4) RETURNING id, created_at, updated_at",
		conn.Name, conn.DataFormat, conn.ConnString, conn.Status,
	).Scan(&conn.ID, &conn.CreatedAt, &conn.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (r *connectionRepository) Update(conn *models.Connection) (*models.Connection, error) {
	_, err := r.db.Exec(
		"UPDATE tenant.connections SET name = $1, data_format = $2, conn_string = $3, status = $4, updated_at = NOW() WHERE id = $5",
		conn.Name, conn.DataFormat, conn.ConnString, conn.Status, conn.ID,
	)
	if err != nil {
		return nil, err
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
