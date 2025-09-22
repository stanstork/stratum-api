package engine

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/stanstork/stratum-api/internal/models"
)

type Client struct {
	Runner        Runner
	ContainerName string
	Bin           string // e.g. "stratum"
	WorkDir       string // optional default workdir in container
}

func NewClient(r Runner, containerName string) *Client {
	return &Client{
		Runner:        r,
		ContainerName: containerName,
		Bin:           "stratum",
	}
}

func (c *Client) TestConnection(ctx context.Context, driver, dsn string) (string, error) {
	cmd := []string{c.Bin, "test-conn", "--format", driver, "--conn-str", dsn}
	res, err := c.Runner.Exec(ctx, c.ContainerName, cmd, WithWorkDir(c.WorkDir), WithTimeout(60*time.Second))
	if err != nil {
		return "", err
	}
	logs := res.Stdout + res.Stderr
	if res.ExitCode != 0 {
		return logs, fmt.Errorf("test-conn failed (%d): %s", res.ExitCode, logs)
	}
	return logs, nil
}

func (c *Client) SaveSourceMetadata(ctx context.Context, conn models.Connection) ([]byte, error) {
	outPath := "/tmp/source_metadata.json"
	connStr, err := conn.GenerateConnString()
	if err != nil {
		return nil, fmt.Errorf("conn string: %w", err)
	}

	script := fmt.Sprintf("mkdir -p $(dirname %s) && %s source info --conn-str '%s' --format %s --output %s",
		outPath, c.Bin, connStr, conn.DataFormat, outPath)

	res, err := c.Runner.Sh(ctx, c.ContainerName, script, WithWorkDir(c.WorkDir), WithTimeout(120*time.Second))
	if err != nil {
		return nil, err
	}
	if res.ExitCode != 0 {
		return nil, fmt.Errorf("source info failed (%d): %s", res.ExitCode, res.Stdout+res.Stderr)
	}
	return c.Runner.CopyFrom(ctx, c.ContainerName, outPath)
}

func (c *Client) DryRun(ctx context.Context, conn models.Connection, configJSON []byte) ([]byte, error) {
	const tmpDir = "/tmp/stratum"
	const cfgName = "config.json"
	const reportPath = "/tmp/dry_run_report.json"

	if _, err := c.Runner.Sh(ctx, c.ContainerName, "mkdir -p "+tmpDir, WithTimeout(10*time.Second)); err != nil {
		return nil, fmt.Errorf("mkdir tmp: %w", err)
	}
	if err := c.Runner.CopyTo(ctx, c.ContainerName, tmpDir, configJSON, cfgName); err != nil {
		return nil, fmt.Errorf("upload config: %w", err)
	}
	if _, err := c.Runner.Sh(ctx, c.ContainerName, fmt.Sprintf("mkdir -p $(dirname %s)", reportPath), WithTimeout(10*time.Second)); err != nil {
		return nil, fmt.Errorf("mkdir output parent: %w", err)
	}

	script := fmt.Sprintf("%s validate --config %s --output %s --from-ast",
		c.Bin, path.Join(tmpDir, cfgName), reportPath)
	res, err := c.Runner.Sh(ctx, c.ContainerName, script, WithTimeout(5*time.Minute))
	if err != nil {
		return nil, err
	}
	if res.ExitCode != 0 {
		return nil, fmt.Errorf("dry-run report failed (%d): %s", res.ExitCode, res.Stdout+res.Stderr)
	}
	return c.Runner.CopyFrom(ctx, c.ContainerName, reportPath)
}
