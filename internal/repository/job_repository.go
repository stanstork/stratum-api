package repository

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/stanstork/stratum-api/internal/models"
)

var ErrJobDefinitionNotReady = errors.New("job definition not ready")

type JobRepository interface {
	// JobDefinition methods
	CrateDefinition(def models.JobDefinition) (models.JobDefinition, error)
	GetJobDefinitionByID(tenantID, jobDefID string) (models.JobDefinition, error)
	ListDefinitions(tenantID string) ([]models.JobDefinition, error)
	UpdateDefinition(tenantID, jobDefID string, update DefinitionUpdate) (models.JobDefinition, error)
	DeleteDefinition(tenantID, jobDefID string) error
	ListJobDefinitionsWithStats(tenantID string) ([]models.JobDefinitionStat, error)

	// JobExecution methods
	CreateExecution(tenantID, jobDefID, executionID string) (models.JobExecution, error)
	GetLastExecution(tenantID, jobDefID string) (models.JobExecution, error)
	UpdateExecution(tenantID, execID string, status string, errorMessage string, logs string) (int64, error)
	ListExecutions(tenantID string, limit, offset int) ([]models.JobExecution, error)
	ListExecutionStats(tenantID string, days int) (models.ExecutionStat, error)
	GetExecution(tenantID, execID string) (models.JobExecution, error)
	SetExecutionComplete(tenantID, execID string, status string, recordsProcessed int64, bytesTransferred int64) error
}

type jobRepository struct {
	db *sql.DB
}

type DefinitionUpdate struct {
	Name                    *string
	Description             *string
	AST                     *json.RawMessage
	SourceConnectionID      *string
	DestinationConnectionID *string
	Status                  *string
	ProgressSnapshot        *json.RawMessage
}

const (
	definitionStatusDraft      = "DRAFT"
	definitionStatusValidating = "VALIDATING"
	definitionStatusReady      = "READY"
)

var allowedDefinitionStatuses = map[string]struct{}{
	definitionStatusDraft:      {},
	definitionStatusValidating: {},
	definitionStatusReady:      {},
}

const jobDefinitionSelectColumns = `
	SELECT
		jd.id,
		jd.tenant_id,
		jd.name,
		jd.description,
		jd.ast,
		jd.source_connection_id,
		jd.destination_connection_id,
		jd.status,
		jd.progress_snapshot,
		jd.created_at,
		jd.updated_at,
		sc.id,
		sc.tenant_id,
		sc.name,
		sc.data_format,
		sc.host,
		sc.port,
		sc.username,
		sc.db_name,
		sc.status,
		sc.created_at,
		sc.updated_at,
		dc.id,
		dc.tenant_id,
		dc.name,
		dc.data_format,
		dc.host,
		dc.port,
		dc.username,
		dc.db_name,
		dc.status,
		dc.created_at,
		dc.updated_at
	FROM tenant.job_definitions jd
	LEFT JOIN tenant.connections sc ON jd.source_connection_id = sc.id AND sc.deleted_at IS NULL
	LEFT JOIN tenant.connections dc ON jd.destination_connection_id = dc.id AND dc.deleted_at IS NULL
`

func normalizeDefinitionStatus(status string) string {
	trimmed := strings.ToUpper(strings.TrimSpace(status))
	if trimmed == "" {
		return definitionStatusReady
	}
	return trimmed
}

func validateDefinitionStatus(status string) error {
	if _, ok := allowedDefinitionStatuses[status]; !ok {
		return fmt.Errorf("invalid job definition status %q", status)
	}
	return nil
}

func nullIfEmpty(value string) interface{} {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func NewJobRepository(db *sql.DB) JobRepository {
	return &jobRepository{db: db}
}

func (r *jobRepository) validateTennantConnection(tenantID, connectionID string) error {
	if strings.TrimSpace(connectionID) == "" {
		return nil
	}

	const query = `
		SELECT 1
		FROM tenant.connections
		WHERE id = $1
		  AND tenant_id = $2
		  AND deleted_at IS NULL
	`

	var exists int
	if err := r.db.QueryRow(query, connectionID, tenantID).Scan(&exists); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("connection %s not found for tenant %s", connectionID, tenantID)
		}
		return err
	}
	return nil
}

func (r *jobRepository) getDefinitionStatus(tenantID, jobDefID string) (string, error) {
	const query = `
		SELECT status
		FROM tenant.job_definitions
		WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL
	`
	var status string
	if err := r.db.QueryRow(query, jobDefID, tenantID).Scan(&status); err != nil {
		return "", err
	}
	return status, nil
}

func (r *jobRepository) recordDefinitionSnapshot(jobDefID, status string, snapshot json.RawMessage) error {
	if len(snapshot) == 0 {
		return nil
	}
	status = normalizeDefinitionStatus(status)
	if err := validateDefinitionStatus(status); err != nil {
		return err
	}
	const query = `
		INSERT INTO tenant.job_definition_snapshots (job_definition_id, status, snapshot)
		VALUES ($1, $2, $3)
	`
	_, err := r.db.Exec(query, jobDefID, status, []byte(snapshot))
	return err
}

func scanJobDefinition(scanner interface {
	Scan(dest ...interface{}) error
}) (models.JobDefinition, error) {
	var (
		def          models.JobDefinition
		ast          []byte
		progress     []byte
		srcConnID    sql.NullString
		dstConnID    sql.NullString
		srcID        sql.NullString
		srcTenantID  sql.NullString
		srcName      sql.NullString
		srcFormat    sql.NullString
		srcHost      sql.NullString
		srcPort      sql.NullInt64
		srcUsername  sql.NullString
		srcDBName    sql.NullString
		srcStatus    sql.NullString
		srcCreatedAt sql.NullTime
		srcUpdatedAt sql.NullTime
		dstID        sql.NullString
		dstTenantID  sql.NullString
		dstName      sql.NullString
		dstFormat    sql.NullString
		dstHost      sql.NullString
		dstPort      sql.NullInt64
		dstUsername  sql.NullString
		dstDBName    sql.NullString
		dstStatus    sql.NullString
		dstCreatedAt sql.NullTime
		dstUpdatedAt sql.NullTime
	)

	if err := scanner.Scan(
		&def.ID,
		&def.TenantID,
		&def.Name,
		&def.Description,
		&ast,
		&srcConnID,
		&dstConnID,
		&def.Status,
		&progress,
		&def.CreatedAt,
		&def.UpdatedAt,
		&srcID,
		&srcTenantID,
		&srcName,
		&srcFormat,
		&srcHost,
		&srcPort,
		&srcUsername,
		&srcDBName,
		&srcStatus,
		&srcCreatedAt,
		&srcUpdatedAt,
		&dstID,
		&dstTenantID,
		&dstName,
		&dstFormat,
		&dstHost,
		&dstPort,
		&dstUsername,
		&dstDBName,
		&dstStatus,
		&dstCreatedAt,
		&dstUpdatedAt,
	); err != nil {
		return def, err
	}

	if len(ast) > 0 {
		def.AST = json.RawMessage(append([]byte(nil), ast...))
	}
	if len(progress) > 0 {
		def.ProgressSnapshot = json.RawMessage(append([]byte(nil), progress...))
	}

	if srcConnID.Valid {
		def.SourceConnectionID = srcConnID.String
		if srcID.Valid {
			def.SourceConnection = models.Connection{
				ID:         srcID.String,
				TenantID:   srcTenantID.String,
				Name:       srcName.String,
				DataFormat: srcFormat.String,
				Host:       srcHost.String,
				Port:       int(srcPort.Int64),
				Username:   srcUsername.String,
				DBName:     srcDBName.String,
				Status:     srcStatus.String,
			}
			if srcCreatedAt.Valid {
				def.SourceConnection.CreatedAt = srcCreatedAt.Time
			}
			if srcUpdatedAt.Valid {
				def.SourceConnection.UpdatedAt = srcUpdatedAt.Time
			}
		}
	}

	if dstConnID.Valid {
		def.DestinationConnectionID = dstConnID.String
		if dstID.Valid {
			def.DestinationConnection = models.Connection{
				ID:         dstID.String,
				TenantID:   dstTenantID.String,
				Name:       dstName.String,
				DataFormat: dstFormat.String,
				Host:       dstHost.String,
				Port:       int(dstPort.Int64),
				Username:   dstUsername.String,
				DBName:     dstDBName.String,
				Status:     dstStatus.String,
			}
			if dstCreatedAt.Valid {
				def.DestinationConnection.CreatedAt = dstCreatedAt.Time
			}
			if dstUpdatedAt.Valid {
				def.DestinationConnection.UpdatedAt = dstUpdatedAt.Time
			}
		}
	}

	return def, nil
}

func (r *jobRepository) loadDefinitionSnapshots(jobDefID string) ([]models.JobDefinitionSnapshot, error) {
	const query = `
		SELECT id, job_definition_id, status, snapshot, created_at
		FROM tenant.job_definition_snapshots
		WHERE job_definition_id = $1
		ORDER BY created_at DESC
	`
	rows, err := r.db.Query(query, jobDefID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var snapshots []models.JobDefinitionSnapshot
	for rows.Next() {
		var snap models.JobDefinitionSnapshot
		var payload []byte
		if err := rows.Scan(&snap.ID, &snap.JobDefinitionID, &snap.Status, &payload, &snap.CreatedAt); err != nil {
			return nil, err
		}
		if len(payload) > 0 {
			snap.Snapshot = json.RawMessage(append([]byte(nil), payload...))
		}
		snapshots = append(snapshots, snap)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return snapshots, nil
}

type definitionMetrics struct {
	totalRuns          int64
	lastRunStatus      *string
	totalBytes         int64
	avgDurationSeconds *float64
}

func (r *jobRepository) fetchDefinitionStats(tenantID string) (map[string]definitionMetrics, error) {
	const query = `
		WITH ranked_executions AS (
			SELECT
				job_definition_id,
				status,
				bytes_transferred,
				EXTRACT(EPOCH FROM (run_completed_at - run_started_at)) AS duration_seconds,
				ROW_NUMBER() OVER (PARTITION BY job_definition_id ORDER BY created_at DESC) AS run_rank
			FROM tenant.job_executions
			WHERE tenant_id = $1
		)
		SELECT
			job_definition_id,
			COUNT(*) AS total_runs,
			MAX(CASE WHEN run_rank = 1 THEN status END) AS last_run_status,
			COALESCE(SUM(bytes_transferred), 0) AS total_bytes_transferred,
			AVG(duration_seconds) AS avg_duration_seconds
		FROM ranked_executions
		GROUP BY job_definition_id
	`
	rows, err := r.db.Query(query, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	metrics := make(map[string]definitionMetrics)
	for rows.Next() {
		var (
			jobDefID    string
			totalRuns   sql.NullInt64
			lastStatus  sql.NullString
			totalBytes  sql.NullInt64
			avgDuration sql.NullFloat64
		)
		if err := rows.Scan(&jobDefID, &totalRuns, &lastStatus, &totalBytes, &avgDuration); err != nil {
			return nil, err
		}
		metric := definitionMetrics{}
		if totalRuns.Valid {
			metric.totalRuns = totalRuns.Int64
		}
		if lastStatus.Valid {
			status := lastStatus.String
			metric.lastRunStatus = &status
		}
		if totalBytes.Valid {
			metric.totalBytes = totalBytes.Int64
		}
		if avgDuration.Valid {
			value := avgDuration.Float64
			metric.avgDurationSeconds = &value
		}
		metrics[jobDefID] = metric
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return metrics, nil
}

func (r *jobRepository) CrateDefinition(def models.JobDefinition) (models.JobDefinition, error) {
	if err := r.validateTennantConnection(def.TenantID, def.SourceConnectionID); err != nil {
		return def, err
	}
	if err := r.validateTennantConnection(def.TenantID, def.DestinationConnectionID); err != nil {
		return def, err
	}

	def.Status = normalizeDefinitionStatus(def.Status)
	if err := validateDefinitionStatus(def.Status); err != nil {
		return def, err
	}

	var (
		astPayload       interface{}
		progressSnapshot interface{}
	)
	if len(def.AST) > 0 {
		astPayload = []byte(def.AST)
	}
	if len(def.ProgressSnapshot) > 0 {
		progressSnapshot = []byte(def.ProgressSnapshot)
	}

	query := `
		INSERT INTO tenant.job_definitions (
			tenant_id,
			name,
			description,
			ast,
			source_connection_id,
			destination_connection_id,
			status,
			progress_snapshot
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id
	`

	if err := r.db.QueryRow(
		query,
		def.TenantID,
		def.Name,
		def.Description,
		astPayload,
		nullIfEmpty(def.SourceConnectionID),
		nullIfEmpty(def.DestinationConnectionID),
		def.Status,
		progressSnapshot,
	).Scan(&def.ID); err != nil {
		return def, err
	}

	if progressSnapshot != nil {
		if err := r.recordDefinitionSnapshot(def.ID, def.Status, def.ProgressSnapshot); err != nil {
			return def, err
		}
	}

	return r.GetJobDefinitionByID(def.TenantID, def.ID)
}

func (r *jobRepository) ListDefinitions(tenantID string) ([]models.JobDefinition, error) {
	query := jobDefinitionSelectColumns + `
		WHERE jd.tenant_id = $1
		  AND jd.deleted_at IS NULL
		ORDER BY jd.created_at DESC;
	`

	rows, err := r.db.Query(query, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var definitions []models.JobDefinition
	for rows.Next() {
		def, err := scanJobDefinition(rows)
		if err != nil {
			return nil, err
		}
		definitions = append(definitions, def)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return definitions, nil
}

func (r *jobRepository) UpdateDefinition(tenantID, jobDefID string, update DefinitionUpdate) (models.JobDefinition, error) {
	var result models.JobDefinition

	if update.SourceConnectionID != nil {
		src := strings.TrimSpace(*update.SourceConnectionID)
		if err := r.validateTennantConnection(tenantID, src); err != nil {
			return result, err
		}
		*update.SourceConnectionID = src
	}
	if update.DestinationConnectionID != nil {
		dst := strings.TrimSpace(*update.DestinationConnectionID)
		if err := r.validateTennantConnection(tenantID, dst); err != nil {
			return result, err
		}
		*update.DestinationConnectionID = dst
	}

	var statusValue string
	if update.Status != nil {
		statusValue = normalizeDefinitionStatus(*update.Status)
		if err := validateDefinitionStatus(statusValue); err != nil {
			return result, err
		}
	}

	setClauses := make([]string, 0, 7)
	args := make([]interface{}, 0, 9)
	idx := 1

	if update.Name != nil {
		setClauses = append(setClauses, fmt.Sprintf("name = $%d", idx))
		args = append(args, *update.Name)
		idx++
	}
	if update.Description != nil {
		setClauses = append(setClauses, fmt.Sprintf("description = $%d", idx))
		args = append(args, *update.Description)
		idx++
	}
	if update.AST != nil {
		var payload interface{}
		if len(*update.AST) > 0 {
			payload = []byte(*update.AST)
		}
		setClauses = append(setClauses, fmt.Sprintf("ast = $%d", idx))
		args = append(args, payload)
		idx++
	}
	if update.SourceConnectionID != nil {
		setClauses = append(setClauses, fmt.Sprintf("source_connection_id = $%d", idx))
		args = append(args, nullIfEmpty(*update.SourceConnectionID))
		idx++
	}
	if update.DestinationConnectionID != nil {
		setClauses = append(setClauses, fmt.Sprintf("destination_connection_id = $%d", idx))
		args = append(args, nullIfEmpty(*update.DestinationConnectionID))
		idx++
	}
	if update.Status != nil {
		setClauses = append(setClauses, fmt.Sprintf("status = $%d", idx))
		args = append(args, statusValue)
		idx++
	}
	if update.ProgressSnapshot != nil {
		var payload interface{}
		if len(*update.ProgressSnapshot) > 0 {
			payload = []byte(*update.ProgressSnapshot)
		}
		setClauses = append(setClauses, fmt.Sprintf("progress_snapshot = $%d", idx))
		args = append(args, payload)
		idx++
	}

	if len(setClauses) == 0 {
		return r.GetJobDefinitionByID(tenantID, jobDefID)
	}

	query := fmt.Sprintf(`
		UPDATE tenant.job_definitions
		SET %s
		WHERE id = $%d AND tenant_id = $%d AND deleted_at IS NULL
	`, strings.Join(setClauses, ", "), idx, idx+1)

	args = append(args, jobDefID, tenantID)

	res, err := r.db.Exec(query, args...)
	if err != nil {
		return result, err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return result, err
	}
	if rowsAffected == 0 {
		return result, errors.New("job definition not found")
	}

	if update.ProgressSnapshot != nil && len(*update.ProgressSnapshot) > 0 {
		statusForSnapshot := statusValue
		if statusForSnapshot == "" {
			statusForSnapshot, err = r.getDefinitionStatus(tenantID, jobDefID)
			if err != nil {
				return result, err
			}
		}
		if err := r.recordDefinitionSnapshot(jobDefID, statusForSnapshot, *update.ProgressSnapshot); err != nil {
			return result, err
		}
	}

	return r.GetJobDefinitionByID(tenantID, jobDefID)
}

func (r *jobRepository) CreateExecution(tenantID, jobDefID, executionID string) (models.JobExecution, error) {
	var exec models.JobExecution
	exec.ID = executionID
	exec.JobDefinitionID = jobDefID
	exec.TenantID = tenantID
	exec.Status = "pending"
	currentStatus, err := r.getDefinitionStatus(tenantID, jobDefID)
	if err != nil {
		return exec, err
	}
	if normalizeDefinitionStatus(currentStatus) != definitionStatusReady {
		return exec, fmt.Errorf("%w: current status %s", ErrJobDefinitionNotReady, currentStatus)
	}

	query := `
		INSERT INTO tenant.job_executions (id, tenant_id, job_definition_id, status, run_started_at, run_completed_at)
		VALUES ($1, $2, $3, $4, NULL, NULL)
		RETURNING created_at, updated_at
	`
	if err := r.db.QueryRow(query, executionID, tenantID, jobDefID, exec.Status).
		Scan(&exec.CreatedAt, &exec.UpdatedAt); err != nil {
		return exec, err
	}
	return exec, nil
}

func (r *jobRepository) GetLastExecution(tenantID, jobDefID string) (models.JobExecution, error) {
	query := `
		SELECT id, tenant_id, job_definition_id, status, created_at, updated_at, run_started_at, run_completed_at, error_message, logs, records_processed, bytes_transferred
		FROM tenant.job_executions
		WHERE job_definition_id = $1 AND tenant_id = $2
		ORDER BY created_at DESC
		LIMIT 1
	`
	var exec models.JobExecution
	err := r.db.QueryRow(query, jobDefID, tenantID).Scan(
		&exec.ID,
		&exec.TenantID,
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

func (r *jobRepository) GetJobDefinitionByID(tenantID, jobDefID string) (models.JobDefinition, error) {
	query := jobDefinitionSelectColumns + `
		WHERE jd.id = $1 AND jd.tenant_id = $2 AND jd.deleted_at IS NULL
	`
	row := r.db.QueryRow(query, jobDefID, tenantID)
	def, err := scanJobDefinition(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return def, errors.New("job definition not found")
		}
		return def, err
	}

	snapshots, err := r.loadDefinitionSnapshots(jobDefID)
	if err != nil {
		return def, err
	}
	def.ProgressSnapshots = snapshots
	return def, nil
}

func (r *jobRepository) DeleteDefinition(tenantID, jobDefID string) error {
	query := `
		UPDATE tenant.job_definitions
		SET deleted_at = now(), updated_at = now()
		WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL;
	`
	res, err := r.db.Exec(query, jobDefID, tenantID)
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
	tenantID, execID, status, errorMessage, logs string,
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
             WHERE id = $2 AND tenant_id = $3
        `
		args = []interface{}{status, execID, tenantID}

	case "succeeded", "failed":
		query = `
            UPDATE tenant.job_executions
               SET status             = $1,
                   run_completed_at   = NOW(),
                   updated_at         = NOW(),
                   error_message      = NULLIF($2, ''),
                   logs               = NULLIF($3, '')
             WHERE id = $4 AND tenant_id = $5
        `
		args = []interface{}{status, errorMessage, logs, execID, tenantID}

	default:
		return 0, fmt.Errorf("invalid status %q", status)
	}

	res, err := r.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (r *jobRepository) ListExecutions(tenantID string, limit, offset int) ([]models.JobExecution, error) {
	const query = `
        SELECT
            id,
            tenant_id,
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
        WHERE tenant_id = $1
        ORDER BY created_at DESC
        LIMIT $2
        OFFSET $3
    `
	rows, err := r.db.Query(query, tenantID, limit, offset)
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
			&e.TenantID,
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

func (r *jobRepository) ListExecutionStats(tenantID string, days int) (models.ExecutionStat, error) {
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
		ON je.created_at::DATE = days.day AND je.tenant_id = $2
		GROUP BY days.day
		ORDER BY days.day;
	`

	rows, err := r.db.Query(query, days, tenantID)
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
		FROM tenant.job_executions
		WHERE tenant_id = $1;
	`

	var stats models.ExecutionStat
	row := r.db.QueryRow(totalQuery, tenantID)
	if err := row.Scan(&stats.Total, &stats.Succeeded, &stats.Failed, &stats.Running); err != nil {
		return models.ExecutionStat{}, fmt.Errorf("GetExecutionStats total scan error: %w", err)
	}

	const defQuery = `
		SELECT COALESCE(COUNT(*), 0)
		FROM tenant.job_definitions
		WHERE tenant_id = $1 AND deleted_at IS NULL;
	`
	var totalDefinitions int
	row = r.db.QueryRow(defQuery, tenantID)
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

func (r *jobRepository) GetExecution(tenantID, execID string) (models.JobExecution, error) {
	query := `
		SELECT id, tenant_id, job_definition_id, status, created_at, updated_at, run_started_at, run_completed_at, error_message, logs, records_processed, bytes_transferred
		FROM tenant.job_executions
		WHERE id = $1 AND tenant_id = $2;
	`
	var exec models.JobExecution
	err := r.db.QueryRow(query, execID, tenantID).Scan(
		&exec.ID,
		&exec.TenantID,
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

func (r *jobRepository) SetExecutionComplete(tenantID, execID string, status string, recordsProcessed int64, bytesTransferred int64) error {
	query := `
		UPDATE tenant.job_executions
		SET status = $1, run_completed_at = NOW(), records_processed = $2, bytes_transferred = $3
		WHERE id = $4 AND tenant_id = $5;
	`
	_, err := r.db.Exec(query, status, recordsProcessed, bytesTransferred, execID, tenantID)
	return err
}

// Retrieves all job definitions along with their execution stats.
func (r *jobRepository) ListJobDefinitionsWithStats(tenantID string) ([]models.JobDefinitionStat, error) {
	definitions, err := r.ListDefinitions(tenantID)
	if err != nil {
		return nil, err
	}

	metrics, err := r.fetchDefinitionStats(tenantID)
	if err != nil {
		return nil, err
	}

	stats := make([]models.JobDefinitionStat, 0, len(definitions))
	for _, def := range definitions {
		stat := models.JobDefinitionStat{JobDefinition: def}
		if metric, ok := metrics[def.ID]; ok {
			stat.TotalRuns = metric.totalRuns
			stat.TotalBytesTransferred = metric.totalBytes
			stat.LastRunStatus = metric.lastRunStatus
			stat.AvgDurationSeconds = metric.avgDurationSeconds
		}
		stats = append(stats, stat)
	}

	return stats, nil
}
