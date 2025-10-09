package models

import (
	"fmt"
	"time"
)

type Connection struct {
	ID         string    `json:"id" db:"id"`
	TenantID   string    `json:"tenant_id" db:"tenant_id"`
	Name       string    `json:"name" db:"name"`
	DataFormat string    `json:"data_format" db:"data_format"` // enum: pg, mysql, api, csv
	Host       string    `json:"host" db:"host"`
	Port       int       `json:"port" db:"port"`
	Username   string    `json:"username" db:"username"`
	Password   string    `json:"password,omitempty" db:"password"`
	DBName     string    `json:"db_name" db:"db_name"`
	Status     string    `json:"status" db:"status"` // enum: valid, invalid, untested
	CreatedAt  time.Time `json:"created_at" db:"created_at"`
	UpdatedAt  time.Time `json:"updated_at" db:"updated_at"`
}

func (c *Connection) GenerateConnString() (string, error) {
	switch c.DataFormat {
	case "pg", "postgresql", "postgres":
		return fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
			c.Username, c.Password, c.Host, c.Port, c.DBName), nil
	case "mysql":
		return fmt.Sprintf("mysql://%s:%s@%s:%d/%s",
			c.Username, c.Password, c.Host, c.Port, c.DBName), nil
	default:
		return "", fmt.Errorf("unknown format: %s", c.DataFormat)
	}
}
