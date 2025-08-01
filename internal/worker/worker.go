package worker

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stanstork/stratum-api/internal/repository"
)

var dataFormatMap = map[string]string{
	"pg":         "Postgres",
	"postgresql": "Postgres",
	"postgres":   "Postgres",
	"mysql":      "MySql",
}

type WorkerConfig struct {
	DB                   *sql.DB
	JobRepo              repository.JobRepository
	ConnRepo             repository.ConnectionRepository
	PollInterval         time.Duration
	EngineImage          string
	TempDir              string
	ContainerCPULimit    int64 // CPU limit in millicores (e.g., 1000 millicores = 1 CPU core)
	ContainerMemoryLimit int64 // Memory limit in bytes (e.g., 512 * 1024 * 1024 for 512MB)
}

type Worker struct {
	cfg WorkerConfig
	cli *client.Client // Docker client
}

func NewWorker(cfg WorkerConfig) (*Worker, error) {
	// Create Docker client using environment variables
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("failed to create Docker client: %w", err)
	}
	return &Worker{cfg: cfg, cli: cli}, nil
}

func (w *Worker) Start(ctx context.Context) error {
	log.Println("Worker started, polling for jobs...")
	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Worker stopped")
			return ctx.Err()
		case <-ticker.C:
			if err := w.processNextPendingJob(ctx); err != nil {
				// Log the error, but continue processing other jobs
				log.Printf("error processing jobs: \n%+v\n", err)
			}
		}
	}
}

func (w *Worker) processNextPendingJob(ctx context.Context) error {
	tx, err := w.cfg.DB.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // Ensure rollback on error

	var execID, jobDefID string
	query := `
		SELECT id, job_definition_id
		FROM tenant.job_executions
		WHERE status = 'pending'
		ORDER BY created_at ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`
	if err := tx.QueryRowContext(ctx, query).Scan(&execID, &jobDefID); err != nil {
		if err == sql.ErrNoRows {
			return nil // No pending jobs found
		}
		return fmt.Errorf("failed to fetch next pending job: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE tenant.job_executions
		SET status = 'running'
		WHERE id = $1
	`, execID)
	if err != nil {
		return fmt.Errorf("failed to update job execution status to running: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return w.run(ctx, execID, jobDefID)
}

func (w *Worker) run(ctx context.Context, execID, jobDefID string) error {
	log.Printf("Running job execution %s for job definition %s", execID, jobDefID)

	// Update execution status to running
	if _, err := w.cfg.JobRepo.UpdateExecution(execID, "running", "", ""); err != nil {
		log.Printf("UpdateExecution execID=%s error: %v", execID, err)
		return errors.Wrap(err, "failed to update execution status to running")
	}

	// Fetch job definition
	def, err := w.cfg.JobRepo.GetJobDefinitionByID(jobDefID)
	if err != nil {
		w.cfg.JobRepo.UpdateExecution(execID, "failed", fmt.Sprintf("Failed to fetch job definition: %v", err), "")
		return errors.Wrap(err, "failed to fetch job definition")
	}

	source_conn, err := w.cfg.ConnRepo.Get(def.SourceConnectionID)
	if err != nil {
		w.cfg.JobRepo.UpdateExecution(execID, "failed", fmt.Sprintf("Failed to fetch source connection: %v", err), "")
		return errors.Wrap(err, "failed to fetch source connection")
	}

	dest_conn, err := w.cfg.ConnRepo.Get(def.DestinationConnectionID)
	if err != nil {
		w.cfg.JobRepo.UpdateExecution(execID, "failed", fmt.Sprintf("Failed to fetch destination connection: %v", err), "")
		return errors.Wrap(err, "failed to fetch destination connection")
	}

	// Write AST to temporary file
	tmpFileName := filepath.Join(w.cfg.TempDir, fmt.Sprintf("migration-%s-%s.json", jobDefID, uuid.NewString()))

	// Parse the AST and ensure it has the necessary connections
	var ast map[string]interface{}
	if err := json.Unmarshal(def.AST, &ast); err != nil {
		w.cfg.JobRepo.UpdateExecution(execID, "failed", fmt.Sprintf("Failed to parse AST: %v", err), "")
		return errors.Wrap(err, "failed to parse AST from job definition")
	}
	if ast == nil {
		return errors.New("AST is empty or invalid")
	}

	source_conn_str, err := source_conn.GenerateConnString()
	if err != nil {
		w.cfg.JobRepo.UpdateExecution(execID, "failed", fmt.Sprintf("Failed to generate source connection string: %v", err), "")
		return errors.Wrap(err, "failed to generate source connection string")
	}
	dest_conn_str, err := dest_conn.GenerateConnString()
	if err != nil {
		w.cfg.JobRepo.UpdateExecution(execID, "failed", fmt.Sprintf("Failed to generate destination connection string: %v", err), "")
		return errors.Wrap(err, "failed to generate destination connection string")
	}

	ast["connections"] = map[string]interface{}{
		"source": map[string]interface{}{
			"conn_type": "Source",
			"format":    dataFormatMap[def.SourceConnection.DataFormat],
			"conn_str":  source_conn_str,
		},
		"dest": map[string]interface{}{
			"conn_type": "Dest",
			"format":    dataFormatMap[def.DestinationConnection.DataFormat],
			"conn_str":  dest_conn_str,
		},
	}

	log.Printf("AST for job definition %s: %+v", jobDefID, ast)

	astBytes, err := json.Marshal(ast)
	if err != nil {
		w.cfg.JobRepo.UpdateExecution(execID, "failed", fmt.Sprintf("Failed to marshal AST: %v", err), "")
		return errors.Wrap(err, "failed to marshal AST to JSON")
	}
	if err := os.WriteFile(tmpFileName, astBytes, 0644); err != nil {
		w.cfg.JobRepo.UpdateExecution(execID, "failed", fmt.Sprintf("Failed to write AST to file: %v", err), "")
		return errors.Wrapf(err, "failed to write AST to temporary file %s", tmpFileName)
	}
	log.Printf("AST written to temporary file: %s", tmpFileName)
	defer os.Remove(tmpFileName)

	// Configure container creation arguments

	// Command that the engine expects
	cmd := []string{"migrate", "--config", "/app/config.json", "--from-ast"}

	// Mounts: bind‐mount the temp file into /app/config.smql
	mounts := []mount.Mount{
		{
			Type:   mount.TypeBind,
			Source: tmpFileName,
			Target: "/app/config.json",
		},
	}

	// Resource constraints: CPU shares & memory limit. Docker SDK uses “HostConfig.Resources”.
	hostConfig := &container.HostConfig{
		Mounts: mounts,
		Resources: container.Resources{
			CPUShares: w.cfg.ContainerCPULimit,    // e.g. 1000 millicores = 1 CPU core
			Memory:    w.cfg.ContainerMemoryLimit, // in bytes (e.g., 512 * 1024 * 1024 for 512MB)
		},
		AutoRemove: true, // Automatically remove the container when it exits
	}

	// Container config: which image, which command
	containerConfig := &container.Config{
		Image: w.cfg.EngineImage,
		Cmd:   cmd,
	}

	// Use the Docker SDK to inspect first, pull only if not found locally
	imageName := w.cfg.EngineImage
	_, err = w.cli.ImageInspect(ctx, imageName)
	if err != nil {
		// If not found, pull the image
		log.Printf("Image %s not found locally, pulling...", imageName)

		// Pull the image
		reader, err := w.cli.ImagePull(ctx, w.cfg.EngineImage, image.PullOptions{})
		if err != nil {
			w.cfg.JobRepo.UpdateExecution(execID, "failed", fmt.Sprintf("Failed to pull image: %v", err), "")
			return fmt.Errorf("failed to pull image: %w", err)
		}

		io.Copy(io.Discard, reader) // Discard the output of the pull operation
		defer reader.Close()

	}

	// Create the container
	resp, err := w.cli.ContainerCreate(
		ctx,
		containerConfig,
		hostConfig,
		nil, // NetworkingConfig
		nil, // Platform
		"",  // Container name (empty means Docker will assign a random name)
	)
	if err != nil {
		w.cfg.JobRepo.UpdateExecution(execID, "failed", fmt.Sprintf("Failed to create container: %v", err), "")
		return fmt.Errorf("failed to create container: %w", err)
	}

	containerID := resp.ID
	log.Printf("Container %s created", containerID)

	// Start the container
	if err := w.cli.ContainerStart(ctx, containerID, container.StartOptions{}); err != nil {
		w.cfg.JobRepo.UpdateExecution(execID, "failed", fmt.Sprintf("Failed to start container: %v", err), "")
		return fmt.Errorf("failed to start container: %w", err)
	}

	// Stream container logs
	// For MVP simplicity, we’ll buffer everything in one string.
	logOpts := container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
	}
	logReader, err := w.cli.ContainerLogs(ctx, containerID, logOpts)
	if err != nil {
		w.cfg.JobRepo.UpdateExecution(execID, "failed", fmt.Sprintf("Failed to get container logs: %v", err), "")
		return fmt.Errorf("failed to get container logs: %w", err)
	}
	defer logReader.Close()

	var (
		stdoutBuf = new(bytes.Buffer)
		stderrBuf = new(bytes.Buffer)
	)

	// stdcopy will consume the multiplexed stream and write “pure” output
	if _, err := stdcopy.StdCopy(stdoutBuf, stderrBuf, logReader); err != nil {
		w.cfg.JobRepo.UpdateExecution(execID,
			"failed",
			fmt.Sprintf("Failed to demux container logs: %v", err),
			"",
		)
		return fmt.Errorf("stdcopy error: %w", err)
	}

	// build mergedLogs without any NULs
	mergedLogs := stdoutBuf.String() + stderrBuf.String()

	// Wait for the container to finish
	// This will block until the container stops running.
	waitResp, errCh := w.cli.ContainerWait(ctx, containerID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		w.cfg.JobRepo.UpdateExecution(execID, "failed", fmt.Sprintf("Container wait error: %v", err), mergedLogs)
		return fmt.Errorf("container wait error: %w", err)
	case status := <-waitResp:
		exitCode := status.StatusCode
		if exitCode != 0 {
			w.cfg.JobRepo.UpdateExecution(execID, "failed", fmt.Sprintf("Container exited with code %d", exitCode), mergedLogs)
			log.Printf("Container %s exited with code %d", containerID, exitCode)
			return fmt.Errorf("container exited with code %d", exitCode)
		}
		log.Printf("Container %s completed successfully", containerID)
	}

	// If we reach here, the container ran successfully
	n, err := w.cfg.JobRepo.UpdateExecution(execID, "succeeded", "", mergedLogs)
	if err != nil {
		log.Printf("UpdateExecution execID=%s error: %v", execID, err)
	} else if n == 0 {
		log.Printf("UpdateExecution execID=%s did not match any rows", execID)
	} else {
		log.Printf("Execution %s updated (%d row(s))", execID, n)
	}

	log.Printf("Job execution %s for job definition %s completed successfully", execID, jobDefID)
	return nil
}
