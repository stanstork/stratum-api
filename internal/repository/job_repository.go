package repository

import (
	"database/sql"
	"errors"
	"fmt"
	"log"

	"github.com/stanstork/stratum-api/internal/models"
)

type JobRepository interface {
	// JobDefinition methods
	CrateDefinition(def models.JobDefinition) (models.JobDefinition, error)
	GetJobDefinitionByID(jobDefID string) (models.JobDefinition, error)
	ListDefinitions() ([]models.JobDefinition, error)
	DeleteDefinition(jobDefID string) error

	// JobExecution methods
	CreateExecution(jobDefID string) (models.JobExecution, error)
	GetLastExecution(jobDefID string) (models.JobExecution, error)
	UpdateExecution(execID string, status string, errorMessage string, logs string) (int64, error)
	ListExecutions(limit, offset int) ([]models.JobExecution, error)
	ListExecutionStats(days int) (models.ExecutionStat, error)
	GetExecution(execID string) (models.JobExecution, error)
	SetExecutionComplete(execID string, status string, recordsProcessed int64, bytesTransferred int64) error
}

type jobRepository struct {
	db *sql.DB
}

func NewJobRepository(db *sql.DB) JobRepository {
	return &jobRepository{db: db}
}

func (r *jobRepository) CrateDefinition(def models.JobDefinition) (models.JobDefinition, error) {
	query := `
		INSERT INTO tenant.job_definitions (tenant_id, name, description, ast, source_connection_id, destination_connection_id)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, created_at, updated_at
	`
	err := r.db.QueryRow(query,
		def.TenantID,
		def.Name,
		def.Description,
		def.AST,
		def.SourceConnectionID,
		def.DestinationConnectionID,
	).Scan(&def.ID, &def.CreatedAt, &def.UpdatedAt)

	return def, err
}

func (r *jobRepository) ListDefinitions() ([]models.JobDefinition, error) {
	query := `
		SELECT
			jd.id, jd.tenant_id, jd.name, jd.description, jd.ast,
			jd.source_connection_id, jd.destination_connection_id,
			sc.id, sc.name, sc.data_format, sc.host, sc.port, sc.username, sc.db_name, sc.created_at, sc.updated_at,
			dc.id, dc.name, dc.data_format, dc.host, dc.port, dc.username, dc.db_name, dc.created_at, dc.updated_at,
			jd.created_at, jd.updated_at
		FROM tenant.job_definitions jd
		JOIN tenant.connections sc ON jd.source_connection_id = sc.id
		JOIN tenant.connections dc ON jd.destination_connection_id = dc.id
		ORDER BY jd.created_at DESC;
	`

	// TODO: Add tenant filtering
	rows, err := r.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var definitions []models.JobDefinition
	for rows.Next() {
		var def models.JobDefinition
		if err := rows.Scan(
			&def.ID,
			&def.TenantID,
			&def.Name,
			&def.Description,
			&def.AST,
			&def.SourceConnectionID,
			&def.DestinationConnectionID,
			&def.SourceConnection.ID,
			&def.SourceConnection.Name,
			&def.SourceConnection.DataFormat,
			&def.SourceConnection.Host,
			&def.SourceConnection.Port,
			&def.SourceConnection.Username,
			&def.SourceConnection.DBName,
			&def.SourceConnection.CreatedAt,
			&def.SourceConnection.UpdatedAt,
			&def.DestinationConnection.ID,
			&def.DestinationConnection.Name,
			&def.DestinationConnection.DataFormat,
			&def.DestinationConnection.Host,
			&def.DestinationConnection.Port,
			&def.DestinationConnection.Username,
			&def.DestinationConnection.DBName,
			&def.DestinationConnection.CreatedAt,
			&def.DestinationConnection.UpdatedAt,
			&def.CreatedAt,
			&def.UpdatedAt,
		); err != nil {
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
		SELECT id, job_definition_id, status, created_at, updated_at, run_started_at, run_completed_at, error_message, logs, records_processed, bytes_transferred
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
		&exec.RecordsProcessed,
		&exec.BytesTransferred,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return exec, errors.New("no executions found") // No execution found
		}
		return exec, err // Other error
	}
	return exec, nil // Return the found execution
}

func (r *jobRepository) GetJobDefinitionByID(jobDefID string) (models.JobDefinition, error) {
	query := `
		SELECT
			jd.id, jd.tenant_id, jd.name, jd.description, jd.ast,
			jd.source_connection_id, jd.destination_connection_id,
			sc.id, sc.name, sc.data_format, sc.host, sc.port, sc.username, sc.db_name, sc.created_at, sc.updated_at, sc.status,
			dc.id, dc.name, dc.data_format, dc.host, dc.port, dc.username, dc.db_name, dc.created_at, dc.updated_at, dc.status,
			jd.created_at, jd.updated_at
		FROM tenant.job_definitions jd
		JOIN tenant.connections sc ON jd.source_connection_id = sc.id
		JOIN tenant.connections dc ON jd.destination_connection_id = dc.id
		WHERE jd.id = $1;
	`
	var def models.JobDefinition
	err := r.db.QueryRow(query, jobDefID).Scan(
		&def.ID,
		&def.TenantID,
		&def.Name,
		&def.Description,
		&def.AST,
		&def.SourceConnectionID,
		&def.DestinationConnectionID,
		&def.SourceConnection.ID,
		&def.SourceConnection.Name,
		&def.SourceConnection.DataFormat,
		&def.SourceConnection.Host,
		&def.SourceConnection.Port,
		&def.SourceConnection.Username,
		&def.SourceConnection.DBName,
		&def.SourceConnection.CreatedAt,
		&def.SourceConnection.UpdatedAt,
		&def.SourceConnection.Status,
		&def.DestinationConnection.ID,
		&def.DestinationConnection.Name,
		&def.DestinationConnection.DataFormat,
		&def.DestinationConnection.Host,
		&def.DestinationConnection.Port,
		&def.DestinationConnection.Username,
		&def.DestinationConnection.DBName,
		&def.DestinationConnection.CreatedAt,
		&def.DestinationConnection.UpdatedAt,
		&def.DestinationConnection.Status,
		&def.CreatedAt,
		&def.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return def, errors.New("job definition not found")
		}
		return def, err
	}
	return def, nil
}

func (r *jobRepository) DeleteDefinition(jobDefID string) error {
	query := `
		DELETE FROM tenant.job_definitions
		WHERE id = $1;
	`
	res, err := r.db.Exec(query, jobDefID)
	if err != nil {
		log.Printf("Error deleting job definition %s: %v", jobDefID, err)
		return fmt.Errorf("failed to delete job definition: %w", err)
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		log.Printf("Error getting rows affected for job definition %s: %v", jobDefID, err)
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rowsAffected == 0 {
		log.Printf("No job definition found with ID %s", jobDefID)
		return errors.New("job definition not found")
	}
	return nil
}

func (r *jobRepository) UpdateExecution(
	execID, status, errorMessage, logs string,
) (int64, error) {
	var (
		query string
		args  []interface{}
	)

	switch status {
	case "running":
		query = `
            UPDATE tenant.job_executions
               SET status          = $1,
                   run_started_at  = NOW(),
                   updated_at      = NOW(),
                   error_message   = NULL,
                   logs            = NULL
             WHERE id = $2
        `
		args = []interface{}{status, execID}

	case "succeeded", "failed":
		query = `
            UPDATE tenant.job_executions
               SET status             = $1,
                   run_completed_at   = NOW(),
                   updated_at         = NOW(),
                   error_message      = NULLIF($2, ''),
                   logs               = NULLIF($3, '')
             WHERE id = $4
        `
		args = []interface{}{status, errorMessage, logs, execID}

	default:
		return 0, fmt.Errorf("invalid status %q", status)
	}

	res, err := r.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (r *jobRepository) ListExecutions(limit, offset int) ([]models.JobExecution, error) {
	const query = `
        SELECT
            id,
            job_definition_id,
            status,
            created_at,
            updated_at,
            run_started_at,
            run_completed_at,
            error_message,
            logs,
            records_processed,
            bytes_transferred
        FROM tenant.job_executions
        ORDER BY created_at DESC
        LIMIT $1
        OFFSET $2
    `
	rows, err := r.db.Query(query, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	executions := make([]models.JobExecution, 0, limit)
	for rows.Next() {
		var e models.JobExecution
		var runStarted sql.NullTime
		var runCompleted sql.NullTime
		var errMsg sql.NullString
		var logs sql.NullString

		if err := rows.Scan(
			&e.ID,
			&e.JobDefinitionID,
			&e.Status,
			&e.CreatedAt,
			&e.UpdatedAt,
			&runStarted,
			&runCompleted,
			&errMsg,
			&logs,
			&e.RecordsProcessed,
			&e.BytesTransferred,
		); err != nil {
			return nil, err
		}

		if runStarted.Valid {
			e.RunStartedAt = &runStarted.Time
		}
		if runCompleted.Valid {
			e.RunCompletedAt = &runCompleted.Time
		}
		if errMsg.Valid {
			e.ErrorMessage = &errMsg.String
		}
		if logs.Valid {
			e.Logs = &logs.String
		}

		executions = append(executions, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return executions, nil
}

func (r *jobRepository) ListExecutionStats(days int) (models.ExecutionStat, error) {
	const query = `
		WITH days AS (
			SELECT generate_series(
				(current_date - ($1 - 1) * INTERVAL '1 day'),
				current_date,
				'1 day'::INTERVAL
			) AS day
		)
		SELECT
			days.day,
			COALESCE(SUM((je.status = 'succeeded')::int), 0)   AS succeeded,
			COALESCE(SUM((je.status = 'failed')::int), 0)      AS failed,
			COALESCE(SUM((je.status = 'running')::int), 0)     AS running,
			COALESCE(SUM((je.status = 'pending')::int), 0)     AS pending
		FROM days
		LEFT JOIN tenant.job_executions je
		ON je.created_at::DATE = days.day
		GROUP BY days.day
		ORDER BY days.day;
	`

	rows, err := r.db.Query(query, days)
	if err != nil {
		return models.ExecutionStat{}, fmt.Errorf("ListExecutionStats query error: %w", err)
	}
	defer rows.Close()

	var perDay []models.ExecutionStatDay
	for rows.Next() {
		var stat models.ExecutionStatDay
		if err := rows.Scan(&stat.Day, &stat.Succeeded, &stat.Failed, &stat.Running, &stat.Pending); err != nil {
			return models.ExecutionStat{}, fmt.Errorf("failed to scan execution stat: %w", err)
		}
		perDay = append(perDay, stat)
	}

	const totalQuery = `
		SELECT
			COALESCE(COUNT(*), 0) AS total,
			COALESCE(SUM((status = 'succeeded')::int), 0) AS succeeded,
			COALESCE(SUM((status = 'failed')::int), 0)    AS failed,
			COALESCE(SUM((status = 'running')::int), 0)   AS running
		FROM tenant.job_executions;
	`

	var stats models.ExecutionStat
	row := r.db.QueryRow(totalQuery)
	if err := row.Scan(&stats.Total, &stats.Succeeded, &stats.Failed, &stats.Running); err != nil {
		return models.ExecutionStat{}, fmt.Errorf("GetExecutionStats total scan error: %w", err)
	}

	const defQuery = `
		SELECT COALESCE(COUNT(*), 0) FROM tenant.job_definitions;
	`
	var totalDefinitions int
	row = r.db.QueryRow(defQuery)
	if err := row.Scan(&totalDefinitions); err != nil {
		return models.ExecutionStat{}, fmt.Errorf("GetExecutionStats total definitions scan error: %w", err)
	}

	if stats.Total > 0 {
		stats.SuccessRate = float64(stats.Succeeded) / float64(stats.Total) * 100.0
	} else {
		stats.SuccessRate = 0.0 // Avoid division by zero
	}
	stats.PerDay = perDay
	stats.TotalDefinitions = totalDefinitions

	return stats, nil
}

func (r *jobRepository) GetExecution(execID string) (models.JobExecution, error) {
	query := `
		SELECT id, job_definition_id, status, created_at, updated_at, run_started_at, run_completed_at, error_message, logs, records_processed, bytes_transferred
		FROM tenant.job_executions
		WHERE id = $1;
	`
	var exec models.JobExecution
	err := r.db.QueryRow(query, execID).Scan(
		&exec.ID,
		&exec.JobDefinitionID,
		&exec.Status,
		&exec.CreatedAt,
		&exec.UpdatedAt,
		&exec.RunStartedAt,
		&exec.RunCompletedAt,
		&exec.ErrorMessage,
		&exec.Logs,
		&exec.RecordsProcessed,
		&exec.BytesTransferred,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return exec, errors.New("execution not found")
		}
		return exec, err
	}
	return exec, nil
}

func (r *jobRepository) SetExecutionComplete(execID string, status string, recordsProcessed int64, bytesTransferred int64) error {
	query := `
		UPDATE tenant.job_executions
		SET status = $1, run_completed_at = NOW(), records_processed = $2, bytes_transferred = $3
		WHERE id = $4;
	`
	_, err := r.db.Exec(query, status, recordsProcessed, bytesTransferred, execID)
	return err
}
