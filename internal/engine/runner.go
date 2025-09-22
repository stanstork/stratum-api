package engine

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

type ExecResult struct {
	ExitCode int
	Stdout   string
	Stderr   string
}

type execOptions struct {
	Env         []string
	WorkDir     string
	AttachStdin bool
	Stdin       io.Reader
	Timeout     time.Duration
}

type ExecOpt func(*execOptions)

type Runner interface {
	Exec(ctx context.Context, containerName string, cmd []string, opts ...ExecOpt) (*ExecResult, error)
	Sh(ctx context.Context, containerName, script string, opts ...ExecOpt) (*ExecResult, error)
	CopyFrom(ctx context.Context, containerName, filePath string) ([]byte, error)
	CopyTo(ctx context.Context, containerName, dstPath string, content []byte, filename string) error
}

type dockerRunner struct {
	cli *client.Client
}

func WithEnv(env ...string) ExecOpt {
	return func(o *execOptions) { o.Env = append(o.Env, env...) }
}

func WithWorkDir(dir string) ExecOpt {
	return func(o *execOptions) { o.WorkDir = dir }
}

func WithStdin(r io.Reader) ExecOpt {
	return func(o *execOptions) { o.AttachStdin = true; o.Stdin = r }
}

func WithTimeout(d time.Duration) ExecOpt {
	return func(o *execOptions) { o.Timeout = d }
}

func (d *dockerRunner) CopyFrom(ctx context.Context, containerName string, filePath string) ([]byte, error) {
	reader, _, err := d.cli.CopyFromContainer(ctx, containerName, filePath)
	if err != nil {
		return nil, fmt.Errorf("copy from container: %w", err)
	}
	defer reader.Close()

	tr := tar.NewReader(reader)
	hdr, err := tr.Next()
	if err == io.EOF {
		return nil, fmt.Errorf("empty archive for %s", filePath)
	}
	if err != nil {
		return nil, fmt.Errorf("tar read header: %w", err)
	}

	if path.Base(filePath) != path.Base(hdr.Name) {
		// some Docker versions prefix with the dir name; tolerate it
	}

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, tr); err != nil {
		return nil, fmt.Errorf("tar read file: %w", err)
	}

	return buf.Bytes(), nil
}

func (d *dockerRunner) CopyTo(ctx context.Context, containerName string, dstPath string, content []byte, filename string) error {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	hdr := &tar.Header{
		Name: filename,
		Mode: 0644,
		Size: int64(len(content)),
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("tar write header: %w", err)
	}
	if _, err := tw.Write(content); err != nil {
		return fmt.Errorf("tar write content: %w", err)
	}
	if err := tw.Close(); err != nil {
		return fmt.Errorf("tar close: %w", err)
	}

	if err := d.cli.CopyToContainer(ctx, containerName, dstPath, &buf, container.CopyToContainerOptions{AllowOverwriteDirWithFile: false}); err != nil {
		return fmt.Errorf("copy to container: %w", err)
	}
	return nil
}

func (d *dockerRunner) Exec(ctx context.Context, containerName string, cmd []string, opts ...ExecOpt) (*ExecResult, error) {
	o := &execOptions{}
	for _, opt := range opts {
		opt(o)
	}

	execCfg := container.ExecOptions{
		Cmd:          cmd,
		AttachStdout: true,
		AttachStderr: true,
		AttachStdin:  o.AttachStdin,
		Env:          o.Env,
		WorkingDir:   o.WorkDir,
	}

	created, err := d.cli.ContainerExecCreate(ctx, containerName, execCfg)
	if err != nil {
		return nil, fmt.Errorf("exec create: %w", err)
	}

	attach, err := d.cli.ContainerExecAttach(ctx, created.ID, container.ExecAttachOptions{
		Detach: false,
		Tty:    false,
	})
	if err != nil {
		return nil, fmt.Errorf("exec attach: %w", err)
	}

	defer attach.Close()

	// Handle stdin if provided
	if o.AttachStdin && o.Stdin != nil {
		go func() {
			io.Copy(attach.Conn, o.Stdin)
			attach.CloseWrite()
		}()
	}

	var outBuf, errBuf bytes.Buffer
	outputDone := make(chan error, 1)
	go func() {
		_, copyErr := stdcopy.StdCopy(&outBuf, &errBuf, attach.Reader)
		outputDone <- copyErr
	}()

	if o.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, o.Timeout)
		defer cancel()
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err = <-outputDone:
		if err != nil {
			return nil, fmt.Errorf("exec stream: %w", err)
		}
	}

	inspect, err := d.cli.ContainerExecInspect(ctx, created.ID)
	if err != nil {
		return nil, fmt.Errorf("exec inspect: %w", err)
	}

	return &ExecResult{
		ExitCode: inspect.ExitCode,
		Stdout:   outBuf.String(),
		Stderr:   errBuf.String(),
	}, nil
}

func (d *dockerRunner) Sh(ctx context.Context, containerName string, script string, opts ...ExecOpt) (*ExecResult, error) {
	return d.Exec(ctx, containerName, []string{"sh", "-lc", script}, opts...)
}

func NewDockerRunner(cli *client.Client) Runner {
	return &dockerRunner{cli: cli}
}
