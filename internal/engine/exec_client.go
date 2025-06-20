package engine

import (
	"bytes"
	"context"
	"fmt"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

func TestConnectionByExec(ctx context.Context, dockerClient *client.Client, containerName, driver, dsn string) (string, error) {
	execConfig := container.ExecOptions{
		Cmd:          []string{"stratum", "test-conn", "--format", driver, "--conn-str", dsn},
		AttachStdout: true,
		AttachStderr: true,
	}
	execResp, err := dockerClient.ContainerExecCreate(ctx, containerName, execConfig)
	if err != nil {
		return "", fmt.Errorf("create exec: %w", err)
	}

	attachResp, err := dockerClient.ContainerExecAttach(ctx, execResp.ID, container.ExecAttachOptions{})
	if err != nil {
		return "", fmt.Errorf("attach exec: %w", err)
	}
	defer attachResp.Close()

	var stdout, stderr bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdout, &stderr, attachResp.Reader); err != nil {
		return "", fmt.Errorf("reading exec output: %w", err)
	}

	insp, err := dockerClient.ContainerExecInspect(ctx, execResp.ID)
	if err != nil {
		return "", fmt.Errorf("inspect exec: %w", err)
	}

	logs := stdout.String() + stderr.String()
	if insp.ExitCode != 0 {
		return logs, fmt.Errorf("exec failed with exit code %d: %s", insp.ExitCode, logs)
	}

	return logs, nil
}
