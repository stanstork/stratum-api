package repository

import (
	"database/sql"
	"errors"

	"github.com/stanstork/stratum-api/internal/models"
)

type JobRepository interface {
	CrateDefinition(def models.JobDefinition) (models.JobDefinition, error)
	ListDefinitions(tenantID string) ([]models.JobDefinition, error)
	CreateExecution(jobDefID string) (models.JobExecution, error)
	GetLastExecution(jobDefID string) (models.JobExecution, error)
}

type jobRepository struct {
	db *sql.DB
}

func NewJobRepository(db *sql.DB) JobRepository {
	return &jobRepository{db: db}
}

func (r *jobRepository) CrateDefinition(def models.JobDefinition) (models.JobDefinition, error) {
	query := `
		INSERT INTO tenant.job_definitions (tenant_id, name, ast, source_connection, destination_connection, engine_settings)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, created_at, updated_at
	`
	err := r.db.QueryRow(query,
		def.TenantID,
		def.Name,
		def.AST,
		def.SourceConnection,
		def.DestinationConnection,
		def.EngineSettings,
	).Scan(&def.ID, &def.CreatedAt, &def.UpdatedAt)

	return def, err
}

func (r *jobRepository) ListDefinitions(tenantID string) ([]models.JobDefinition, error) {
	query := `
		SELECT id, tenant_id, name, ast, source_connection, destination_connection, engine_settings, created_at, updated_at
		FROM tenant.job_definitions
		WHERE tenant_id = $1
	`
	rows, err := r.db.Query(query, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var definitions []models.JobDefinition
	for rows.Next() {
		var def models.JobDefinition
		if err := rows.Scan(&def.ID, &def.TenantID, &def.Name, &def.AST,
			&def.SourceConnection, &def.DestinationConnection,
			&def.EngineSettings, &def.CreatedAt, &def.UpdatedAt); err != nil {
			return nil, err
		}
		definitions = append(definitions, def)
	}

	return definitions, nil
}

func (r *jobRepository) CreateExecution(jobDefID string) (models.JobExecution, error) {
	var exec models.JobExecution
	exec.JobDefinitionID = jobDefID
	exec.Status = "pending"
	query := `
		INSERT INTO tenant.job_executions (job_definition_id, status, run_started_at, run_completed_at)
        VALUES ($1, $2, NULL, NULL)
        RETURNING id, created_at, updated_at
	` // omit timestamps
	err := r.db.QueryRow(query, jobDefID, exec.Status).
		Scan(&exec.ID, &exec.CreatedAt, &exec.UpdatedAt)
	if err != nil {
		return exec, err
	}
	return exec, nil
}

func (r *jobRepository) GetLastExecution(jobDefID string) (models.JobExecution, error) {
	query := `
		SELECT id, job_definition_id, status, created_at, updated_at, run_started_at, run_completed_at, error_message, logs
		FROM tenant.job_executions
		WHERE job_definition_id = $1
		ORDER BY created_at DESC
		LIMIT 1
	`
	var exec models.JobExecution
	err := r.db.QueryRow(query, jobDefID).Scan(
		&exec.ID,
		&exec.JobDefinitionID,
		&exec.Status,
		&exec.CreatedAt,
		&exec.UpdatedAt,
		&exec.RunStartedAt,
		&exec.RunCompletedAt,
		&exec.ErrorMessage,
		&exec.Logs,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return exec, errors.New("no executions found") // No execution found
		}
		return exec, err // Other error
	}
	return exec, nil // Return the found execution
}
