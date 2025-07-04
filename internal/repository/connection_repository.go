package repository

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"fmt"
	"io"
	"os"

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

// encryptionKey loads a 32-byte key from environment variable STRATUM_ENC_KEY.
func encryptionKey() ([]byte, error) {
	b64 := os.Getenv("STRATUM_ENC_KEY")
	if b64 == "" {
		return nil, fmt.Errorf("encryption key not set")
	}
	key, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, fmt.Errorf("invalid base64 key: %w", err)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("encryption key must be 32 bytes")
	}
	return key, nil
}

func encryptPassword(plain string) ([]byte, error) {
	key, err := encryptionKey()
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	ciphertext := gcm.Seal(nonce, nonce, []byte(plain), nil)
	return ciphertext, nil
}

func decryptPassword(data []byte) (string, error) {
	key, err := encryptionKey()
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonceSize := gcm.NonceSize()
	if len(data) < nonceSize {
		return "", fmt.Errorf("ciphertext too short")
	}
	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	plain, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", err
	}
	return string(plain), nil
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
		pwd, err := decryptPassword(encPwd)
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
	pwd, err := decryptPassword(encPwd)
	if err != nil {
		return &c, fmt.Errorf("decrypt password: %w", err)
	}
	c.Password = pwd
	return &c, nil
}

func (r *connectionRepository) Create(conn *models.Connection) (*models.Connection, error) {
	encPwd, err := encryptPassword(conn.Password)
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
	encPwd, err := encryptPassword(conn.Password)
	if err != nil {
		return conn, fmt.Errorf("encrypt password: %w", err)
	}
	const q = `
UPDATE tenant.connections
SET name = $1,
    data_format = $2,
    host = $3,
    port = $4,
    username = $5,
    password = $6,
    db_name = $7,
    updated_at = now()
WHERE id = $8
RETURNING created_at, updated_at;
`
	if err := r.db.QueryRow(
		q,
		conn.Name, conn.DataFormat,
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
