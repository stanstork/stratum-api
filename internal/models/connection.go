package models

type Connection struct {
	ID         string `json:"id" db:"id"`
	Name       string `json:"name" db:"name"`
	DataFormat string `json:"data_format" db:"data_format"`
	ConnString string `json:"conn_string" db:"conn_string"`
	Status     string `json:"status" db:"status"` // e.g., "valid", "invalid", "untested"
	CreatedAt  string `json:"created_at" db:"created_at"`
	UpdatedAt  string `json:"updated_at" db:"updated_at"`
}
