package engine

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"path"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/stanstork/stratum-api/internal/models"
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

func SaveSourceMetadata(ctx context.Context, dockerClient *client.Client, containerName string, conn models.Connection) ([]byte, error) {
	if conn.Status != "valid" {
		return nil, fmt.Errorf("connection is not valid: %s", conn.Status)
	}

	// TODO: make output path configurable and unique per request
	// For now, we use a fixed path in the container
	outputPath := "/tmp/source_metadata.json"

	execConfig := container.ExecOptions{
		Cmd: []string{
			"sh", "-c",
			fmt.Sprintf(
				// ensure parent dir exists, then run CLI
				"mkdir -p $(dirname %[1]s) && :> %[1]s && stratum source info --conn-str %[3]s --format %[2]s --output %[1]s",
				outputPath, conn.DataFormat, conn.ConnString,
			),
		},
		AttachStdout: true,
		AttachStderr: true,
	}
	execResp, err := dockerClient.ContainerExecCreate(ctx, containerName, execConfig)
	if err != nil {
		return nil, fmt.Errorf("create exec: %w", err)
	}

	attachResp, err := dockerClient.ContainerExecAttach(ctx, execResp.ID, container.ExecAttachOptions{})
	if err != nil {
		return nil, fmt.Errorf("attach exec: %w", err)
	}
	defer attachResp.Close()

	insp, err := dockerClient.ContainerExecInspect(ctx, execResp.ID)
	if err != nil {
		return nil, fmt.Errorf("inspect exec: %w", err)
	}
	if insp.ExitCode != 0 {
		return nil, fmt.Errorf("exec failed with exit code %d", insp.ExitCode)
	}

	// Copy the output file from the container
	reader, _, err := dockerClient.CopyFromContainer(ctx, containerName, outputPath)
	if err != nil {
		return nil, fmt.Errorf("copy from container: %w", err)
	}
	defer reader.Close()

	tr := tar.NewReader(reader)
	target := path.Base(outputPath)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("reading tar: %w", err)
		}
		if hdr.Typeflag == tar.TypeReg && hdr.Name == target {
			var buf bytes.Buffer
			if _, err := io.Copy(&buf, tr); err != nil {
				return nil, fmt.Errorf("extracting file: %w", err)
			}
			return buf.Bytes(), nil
		}
	}
	return nil, fmt.Errorf("file %q not found in archive", outputPath)
}
