package activities

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"go.temporal.io/sdk/activity"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/golang-jwt/jwt/v4"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stanstork/stratum-api/internal/repository"
	"github.com/stanstork/stratum-api/internal/temporal"
)

type Activities struct {
	JobRepo           repository.JobRepository
	ConnRepo          repository.ConnectionRepository
	DockerClient      *client.Client
	EngineImage       string
	JWTSigningKey     []byte
	TempDir           string
	ContainerCPULimit int64
	ContainerMemLimit int64
}

var dataFormatMap = map[string]string{
	"pg":         "Postgres",
	"postgresql": "Postgres",
	"postgres":   "Postgres",
	"mysql":      "MySql",
}

func (a *Activities) CreateExecutionActivity(ctx context.Context, tenantID, jobDefID string) error {
	logger := activity.GetLogger(ctx)
	logger.Info("Creating job execution record in database", "tenantID", tenantID, "jobDefID", jobDefID)

	_, err := a.JobRepo.CreateExecution(tenantID, jobDefID)
	if err != nil {
		logger.Error("Failed to create execution record in database", "error", err)
	}
	return err
}

func (a *Activities) UpdateJobStatusActivity(ctx context.Context, tenantID, executionID, status, message, logs string) error {
	logger := activity.GetLogger(ctx)
	logger.Info("Updating job status", "tenantID", tenantID, "executionID", executionID, "status", status)
	_, err := a.JobRepo.UpdateExecution(tenantID, executionID, status, message, logs)
	if err != nil {
		logger.Error("Failed to update job status", "error", err)
	}
	return err
}

func (a *Activities) PrepareExecutionActivity(ctx context.Context, params temporal.ExecutionParams) (*temporal.PrepareActivityResult, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Preparing execution", "tenantID", params.TenantID, "executionID", params.ExecutionID)

	def, err := a.JobRepo.GetJobDefinitionByID(params.TenantID, params.JobDefinitionID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch job definition")
	}

	source_conn, err := a.ConnRepo.Get(params.TenantID, def.SourceConnectionID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch source connection")
	}

	dest_conn, err := a.ConnRepo.Get(params.TenantID, def.DestinationConnectionID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch destination connection")
	}

	var ast map[string]interface{}
	if err := json.Unmarshal(def.AST, &ast); err != nil {
		return nil, errors.Wrap(err, "failed to parse AST from job definition")
	}

	source_conn_str, err := source_conn.GenerateConnString()
	if err != nil {
		return nil, errors.Wrap(err, "failed to generate source connection string")
	}

	dest_conn_str, err := dest_conn.GenerateConnString()
	if err != nil {
		return nil, errors.Wrap(err, "failed to generate destination connection string")
	}

	ast["connections"] = map[string]interface{}{
		"source": map[string]interface{}{"conn_type": "Source", "format": dataFormatMap[def.SourceConnection.DataFormat], "conn_str": source_conn_str},
		"dest":   map[string]interface{}{"conn_type": "Dest", "format": dataFormatMap[def.DestinationConnection.DataFormat], "conn_str": dest_conn_str},
	}

	astBytes, err := json.Marshal(ast)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal AST to JSON")
	}

	tmpFileName := filepath.Join(a.TempDir, fmt.Sprintf("migration-%s-%s.json", params.JobDefinitionID, uuid.NewString()))
	if err := os.WriteFile(tmpFileName, astBytes, 0644); err != nil {
		return nil, errors.Wrapf(err, "failed to write AST to temporary file %s", tmpFileName)
	}
	logger.Info("Wrote AST to temporary file", "file", tmpFileName)

	authToken, err := generateJobToken(params.ExecutionID, params.TenantID, a.JWTSigningKey)
	if err != nil {
		return nil, errors.Wrap(err, "failed to generate job auth token")
	}

	hostIP, err := getOutboundIP()
	if err != nil {
		return nil, errors.Wrap(err, "could not get host IP for callback URL")
	}
	hostCallbackURL := fmt.Sprintf("http://%s:8080/api/jobs/executions/%s/complete", hostIP, params.ExecutionID)

	return &temporal.PrepareActivityResult{
		ASTFilePath:     tmpFileName,
		AuthToken:       authToken,
		HostCallbackURL: hostCallbackURL,
		TenantID:        params.TenantID,
		ExecutionID:     params.ExecutionID,
	}, nil
}

func (a *Activities) RunExecutionContainerActivity(ctx context.Context, params temporal.PrepareActivityResult) (*temporal.RunContainerResult, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Starting Docker container for execution", "ExecutionID", params.ExecutionID)

	// Pull the engine image if not present
	if _, err := a.DockerClient.ImageInspect(ctx, a.EngineImage); err != nil {
		logger.Info("Image not found locally, pulling...", "image", a.EngineImage)
		activity.RecordHeartbeat(ctx, "pulling-image")
		reader, pullErr := a.DockerClient.ImagePull(ctx, a.EngineImage, image.PullOptions{})
		if pullErr != nil {
			return nil, fmt.Errorf("failed to pull image: %w", pullErr)
		}
		io.Copy(io.Discard, reader)
		reader.Close()
	}

	// Create container
	resp, err := a.DockerClient.ContainerCreate(ctx,
		&container.Config{
			Image: a.EngineImage,
			Cmd:   []string{"migrate", "--config", "/app/config.json", "--from-ast"},
			Env: []string{
				fmt.Sprintf("REPORT_CALLBACK_URL=%s", params.HostCallbackURL),
				fmt.Sprintf("AUTH_TOKEN=%s", params.AuthToken),
			},
		},
		&container.HostConfig{
			Mounts: []mount.Mount{{Type: mount.TypeBind, Source: params.ASTFilePath, Target: "/app/config.json"}},
			Resources: container.Resources{
				CPUShares: a.ContainerCPULimit,
				Memory:    a.ContainerMemLimit,
			},
			AutoRemove: true,
		}, nil, nil, "")
	if err != nil {
		return nil, fmt.Errorf("failed to create container: %w", err)
	}

	containerID := resp.ID
	logger.Info("Container created", "containerID", containerID)

	// Start container
	if err := a.DockerClient.ContainerStart(ctx, containerID, container.StartOptions{}); err != nil {
		return nil, fmt.Errorf("failed to start container: %w", err)
	}

	// Stream logs
	logReader, err := a.DockerClient.ContainerLogs(ctx, containerID, container.LogsOptions{ShowStdout: true, ShowStderr: true, Follow: true})
	if err != nil {
		return nil, fmt.Errorf("failed to get container logs: %w", err)
	}
	defer logReader.Close()

	var stdoutBuf, stderrBuf bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdoutBuf, &stderrBuf, logReader); err != nil {
		return nil, fmt.Errorf("failed to demux container logs: %w", err)
	}
	mergedLogs := stdoutBuf.String() + stderrBuf.String()

	// Wait for container to finish
	activity.RecordHeartbeat(ctx, "waiting-for-container")
	waitResp, errCh := a.DockerClient.ContainerWait(ctx, containerID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		return nil, fmt.Errorf("container wait error: %w", err)
	case status := <-waitResp:
		logger.Info("Container finished.", "ContainerID", containerID, "ExitCode", status.StatusCode)
		return &temporal.RunContainerResult{
			ExitCode:    status.StatusCode,
			Logs:        mergedLogs,
			TenantID:    params.TenantID,
			ExecutionID: params.ExecutionID,
		}, nil
	case <-ctx.Done():
		// If the activity is cancelled, we should try to stop the container.
		logger.Warn("Activity context cancelled, stopping container", "ContainerID", containerID)
		// Use a background context for the stop command.
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		a.DockerClient.ContainerStop(stopCtx, containerID, container.StopOptions{})
		return nil, ctx.Err()
	}
}

func (a *Activities) HandleCompletionActivity(ctx context.Context, result temporal.RunContainerResult) error {
	logger := activity.GetLogger(ctx)

	if result.ExitCode != 0 {
		msg := fmt.Sprintf("Container exited with non-zero code %d", result.ExitCode)
		logger.Error(msg, "ExecutionID", result.ExecutionID)
		return a.UpdateJobStatusActivity(ctx, result.TenantID, result.ExecutionID, "failed", msg, result.Logs)
	}

	logger.Info("Container succeeded. Waiting for engine report...", "ExecutionID", result.ExecutionID)
	time.Sleep(5 * time.Second) // Give the engine's API call a few seconds to arrive.

	exec, err := a.JobRepo.GetExecution(result.TenantID, result.ExecutionID)
	if err != nil {
		logger.Error("Failed to re-fetch execution after run", "error", err)
		return errors.Wrap(err, "failed to re-fetch execution after run")
	}

	if exec.Status == "running" {
		// The callback didn't update the status in time.
		logger.Warn("Engine report did not arrive in time. Marking as succeeded without metrics.", "ExecutionID", result.ExecutionID)
		return a.UpdateJobStatusActivity(ctx, result.TenantID, result.ExecutionID, "succeeded", "", result.Logs)
	}

	// The callback updated the status. We just need to save the logs.
	logger.Info("Engine report received. Final status set by engine.", "ExecutionID", result.ExecutionID, "Status", exec.Status)
	_, err = a.JobRepo.UpdateExecution(result.TenantID, result.ExecutionID, exec.Status, "", result.Logs)
	return err
}

func (a *Activities) CleanupActivity(ctx context.Context, filePath string) error {
	logger := activity.GetLogger(ctx)
	logger.Info("Cleaning up temporary file", "path", filePath)
	err := os.Remove(filePath)
	if err != nil && !os.IsNotExist(err) {
		logger.Error("Failed to remove temporary file", "path", filePath, "error", err)
		return err // The error will be logged by Temporal, but won't fail the workflow.
	}
	return nil
}

func generateJobToken(execID string, tenantID string, signingKey []byte) (string, error) {
	claims := jwt.MapClaims{
		"sub": execID,
		"tid": tenantID,
		"aud": "job-worker",
		"iss": "job-orchestrator",
		"exp": time.Now().Add(time.Hour).Unix(),
		"iat": time.Now().Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(signingKey)
}

func getOutboundIP() (string, error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "", err
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String(), nil
}
