package ags

import (
	"bytes"
	"context"
	"io"
	"sync"
	"time"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/dataplane"
)

type runtimeDataPlane struct {
	wire     *dataplane.Client
	timeout  time.Duration
	mu       sync.Mutex
	handles  []interface{ invalidate() error }
	closed   bool
	lifetime context.Context
	cancel   context.CancelCauseFunc
}

func newRuntimeDataPlane(wire *dataplane.Client, timeout time.Duration) *runtimeDataPlane {
	lifetime, cancel := context.WithCancelCause(context.Background())
	return &runtimeDataPlane{wire: wire, timeout: timeout, lifetime: lifetime, cancel: cancel}
}
func (d *runtimeDataPlane) Read(ctx context.Context, path, user string) (out io.ReadCloser, err error) {
	ctx, finish, started := d.requestOperation(ctx, "Files.Read")
	defer func() {
		err = operationError(ctx, "Files.Read", err)
		if out == nil {
			finish()
		}
	}()
	body, err := d.wire.ReadFile(ctx, path, user)
	if err != nil {
		return nil, mapDataPlaneError("Files.Read", err)
	}
	started()
	return &generationReader{ReadCloser: body, ctx: ctx, finish: finish, op: "Files.Read"}, nil
}
func (d *runtimeDataPlane) Write(ctx context.Context, path string, body io.Reader, user string) (out FileInfo, err error) {
	ctx, finish, _ := d.requestOperation(ctx, "Files.Write")
	defer finish()
	defer func() { err = operationError(ctx, "Files.Write", err) }()
	info, err := d.wire.WriteFile(ctx, path, body, user)
	if err != nil {
		return FileInfo{}, mapDataPlaneError("Files.Write", err)
	}
	return mapDataPlaneFileInfo(info), nil
}
func (d *runtimeDataPlane) List(ctx context.Context, path string, depth int, user string) (result []FileInfo, err error) {
	ctx, finish, _ := d.requestOperation(ctx, "Files.List")
	defer finish()
	defer func() { err = operationError(ctx, "Files.List", err) }()
	entries, err := d.wire.ListFiles(ctx, path, depth, user)
	if err != nil {
		return nil, err
	}
	out := make([]FileInfo, 0, len(entries))
	for _, entry := range entries {
		out = append(out, mapDataPlaneFileInfo(entry))
	}
	return out, nil
}
func (d *runtimeDataPlane) Run(ctx context.Context, command string, opts CommandOptions) (result CommandResult, err error) {
	ctx, finish, started := d.requestOperation(ctx, "Commands.Run")
	defer finish()
	defer func() { err = operationError(ctx, "Commands.Run", err) }()
	cfg := dataplane.ProcessConfig{Command: command, Args: opts.Args, Env: opts.Env, CWD: opts.Cwd}
	limit := opts.MaxOutputBytes
	if limit == 0 {
		limit = DefaultMaxOutputBytes
	}
	stdout, stderr := &limitedOutput{limit: limit}, &limitedOutput{limit: limit}
	stream, err := d.wire.StartProcess(ctx, cfg, string(opts.User))
	if err != nil {
		return CommandResult{}, err
	}
	defer stream.Close()
	started()
	for {
		event, receiveErr := stream.Recv()
		if receiveErr != nil {
			if receiveErr == io.EOF {
				break
			}
			return CommandResult{}, receiveErr
		}
		switch event.Kind {
		case dataplane.ProcessStdout:
			stdout.write(event.Data)
		case dataplane.ProcessStderr:
			stderr.write(event.Data)
		case dataplane.ProcessEnd:
			return CommandResult{ExitCode: event.Exit.Code, Stdout: stdout.bytes(), Stderr: stderr.bytes(), StdoutTruncated: stdout.truncated, StderrTruncated: stderr.truncated}, nil
		}
	}
	if ctx.Err() != nil && context.Cause(d.lifetime) == nil {
		killCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = d.wire.SendProcessSignal(killCtx, stream.PID, dataplane.SignalKILL, string(opts.User))
		code := DeadlineExceeded
		if ctx.Err() == context.Canceled {
			code = Canceled
		}
		return CommandResult{}, &Error{Code: code, Operation: "Commands.Run", Cause: ctx.Err()}
	}
	return CommandResult{}, codeError(Protocol, "Commands.Run", "END_EVENT_MISSING")
}

// DefaultMaxOutputBytes limits each retained command-output stream to 4 MiB when no explicit
// limit is supplied.
const DefaultMaxOutputBytes int64 = 4 << 20

type limitedOutput struct {
	mu        sync.Mutex
	value     bytes.Buffer
	limit     int64
	truncated bool
}

func (w *limitedOutput) write(value []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	remaining := w.limit - int64(w.value.Len())
	if remaining <= 0 {
		if len(value) > 0 {
			w.truncated = true
		}
		return
	}
	if int64(len(value)) > remaining {
		value = value[:remaining]
		w.truncated = true
	}
	_, _ = w.value.Write(value)
}
func (w *limitedOutput) bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]byte(nil), w.value.Bytes()...)
}
func mapDataPlaneFileInfo(v dataplane.FileInfo) FileInfo {
	kind := UnknownFileType
	switch v.Type {
	case dataplane.FileTypeFile:
		kind = File
	case dataplane.FileTypeDirectory:
		kind = Directory
	}
	return FileInfo{Name: v.Name, Path: v.Path, Type: kind, Size: v.Size, Mode: v.Mode, Permissions: v.Permissions, Owner: v.Owner, Group: v.Group, ModifiedAt: v.ModifiedAt, SymlinkTarget: v.SymlinkTarget}
}
func mapDataPlaneError(operation string, err error) error {
	return normalizeError(operation, err)
}
func (d *runtimeDataPlane) Ready(ctx context.Context) error {
	if _, err := d.List(ctx, "/tmp", 1, "user"); err == nil {
		return nil
	}
	_, err := d.List(ctx, "/tmp", 1, "root")
	return err
}
func (d *runtimeDataPlane) register(handle interface{ invalidate() error }) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return codeError(InstancePaused, "dataPlane", "INSTANCE_PAUSED")
	}
	d.handles = append(d.handles, handle)
	return nil
}
func (d *runtimeDataPlane) Close() error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	d.cancel(codeError(InstancePaused, "dataPlane", "INSTANCE_PAUSED"))
	handles := append([]interface{ invalidate() error }{}, d.handles...)
	d.handles = nil
	d.mu.Unlock()
	for _, handle := range handles {
		_ = handle.invalidate()
	}
	return nil
}
