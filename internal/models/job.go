package models

import (
	"encoding/json"
	"time"
)

type JobDefinition struct {
	ID                      string          `json:"id" db:"id"`
	TenantID                string          `json:"tenant_id" db:"tenant_id"`
	Name                    string          `json:"name" db:"name"`
	Description             string          `json:"description" db:"description"`
	AST                     json.RawMessage `json:"ast" db:"ast"`
	SourceConnectionID      string          `json:"-" db:"source_connection_id"`
	DestinationConnectionID string          `json:"-" db:"destination_connection_id"`
	SourceConnection        Connection      `json:"source_connection"`
	DestinationConnection   Connection      `json:"destination_connection"`
	CreatedAt               time.Time       `json:"created_at" db:"created_at"`
	UpdatedAt               time.Time       `json:"updated_at" db:"updated_at"`
}

type JobExecution struct {
	ID               string     `json:"id" db:"id"`
	TenantID         string     `json:"tenant_id" db:"tenant_id"`
	JobDefinitionID  string     `json:"job_definition_id" db:"job_definition_id"`
	Status           string     `json:"status" db:"status"`
	CreatedAt        time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at" db:"updated_at"`
	RunStartedAt     *time.Time `json:"run_started_at" db:"run_started_at"`
	RunCompletedAt   *time.Time `json:"run_completed_at" db:"run_completed_at"`
	ErrorMessage     *string    `json:"error_message" db:"error_message"`
	Logs             *string    `json:"logs" db:"logs"`
	RecordsProcessed *int64     `json:"records_processed" db:"records_processed"`
	BytesTransferred *int64     `json:"bytes_transferred" db:"bytes_transferred"`
}
