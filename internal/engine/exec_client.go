package engine

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
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
		log.Printf("Error creating exec in container %s: %v", containerName, err)
		return "", fmt.Errorf("create exec: %w", err)
	}

	attachResp, err := dockerClient.ContainerExecAttach(ctx, execResp.ID, container.ExecAttachOptions{})
	if err != nil {
		log.Printf("Error attaching to exec in container %s: %v", containerName, err)
		return "", fmt.Errorf("attach exec: %w", err)
	}
	defer attachResp.Close()

	var stdout, stderr bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdout, &stderr, attachResp.Reader); err != nil {
		log.Printf("Error reading exec output in container %s: %v", containerName, err)
		return "", fmt.Errorf("reading exec output: %w", err)
	}

	insp, err := dockerClient.ContainerExecInspect(ctx, execResp.ID)
	if err != nil {
		log.Printf("Error inspecting exec in container %s: %v", containerName, err)
		return "", fmt.Errorf("inspect exec: %w", err)
	}

	logs := stdout.String() + stderr.String()
	if insp.ExitCode != 0 {
		log.Printf("Exec command in container %s failed with exit code %d: %s", containerName, insp.ExitCode, logs)
		return logs, fmt.Errorf("exec failed with exit code %d: %s", insp.ExitCode, logs)
	}

	return logs, nil
}

func SaveSourceMetadata(ctx context.Context, dockerClient *client.Client, containerName string, conn models.Connection) ([]byte, error) {
	outputPath := "/tmp/source_metadata.json"

	conn_str, err := conn.GenerateConnString()
	if err != nil {
		return nil, fmt.Errorf("generate connection string: %w", err)
	}
	command := fmt.Sprintf(
		// Ensure parent dir exists, then run the CLI command
		"mkdir -p $(dirname %[1]s) && stratum source info --conn-str '%[3]s' --format %[2]s --output %[1]s",
		outputPath,
		conn.DataFormat,
		conn_str,
	)
	println("Executing command in container:", command)

	execConfig := container.ExecOptions{
		Cmd: []string{
			"sh", "-c",
			fmt.Sprintf(
				// Ensure parent dir exists, then run the CLI command
				"mkdir -p $(dirname %[1]s) && stratum source info --conn-str '%[3]s' --format %[2]s --output %[1]s",
				outputPath,
				conn.DataFormat,
				conn_str,
			)},
		AttachStdout: true,
		AttachStderr: true,
	}

	execResp, err := dockerClient.ContainerExecCreate(ctx, containerName, execConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create exec in container: %w", err)
	}

	attachResp, err := dockerClient.ContainerExecAttach(ctx, execResp.ID, container.ExecAttachOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to attach to exec in container: %w", err)
	}
	defer attachResp.Close()

	// IMPORTANT: Wait for the command to finish.
	// We can drain the output to ensure completion.
	var stderrBuf bytes.Buffer
	// The output from Stdout will be empty because we redirect it to a file.
	// We still need to copy it to allow the command to finish.
	_, err = io.Copy(io.Discard, attachResp.Reader)
	if err != nil {
		return nil, fmt.Errorf("error while waiting for command to finish: %w", err)
	}
	// It's good practice to also read Stderr to help with debugging.
	io.Copy(&stderrBuf, attachResp.Reader)

	// Now that the command has finished, inspect the exit code.
	insp, err := dockerClient.ContainerExecInspect(ctx, execResp.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect exec in container: %w", err)
	}

	if insp.ExitCode != 0 {
		return nil, fmt.Errorf("exec command failed with exit code %d: %s", insp.ExitCode, stderrBuf.String())
	}

	// The command has completed successfully, so the file should exist.
	reader, _, err := dockerClient.CopyFromContainer(ctx, containerName, outputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to copy file from container: %w", err)
	}
	defer reader.Close()

	tr := tar.NewReader(reader)
	hdr, err := tr.Next() // We expect only one file in the archive
	if err == io.EOF {
		return nil, fmt.Errorf("archive from container is empty")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read tar header from container: %w", err)
	}

	// Check if the file in the archive is the one we want.
	if hdr.Name != path.Base(outputPath) {
		return nil, fmt.Errorf("expected file %q but found %q in archive", path.Base(outputPath), hdr.Name)
	}

	var fileContent bytes.Buffer
	if _, err := io.Copy(&fileContent, tr); err != nil {
		return nil, fmt.Errorf("failed to extract file from archive: %w", err)
	}

	return fileContent.Bytes(), nil
}
