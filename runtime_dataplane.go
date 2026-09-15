package ags

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/dataplane"
	fsproto "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/gen/filesystem"
	processproto "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/gen/process"
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

func newRuntimeDataPlane(host, token string) *runtimeDataPlane {
	return newDataPlane("https://"+host, token, &http.Client{})
}
func newDataPlane(base, token string, client *http.Client) *runtimeDataPlane {
	return newDataPlaneWithTimeout(base, token, client, 30*time.Second)
}
func newDataPlaneWithTimeout(base, token string, client *http.Client, timeout time.Duration) *runtimeDataPlane {
	lifetime, cancel := context.WithCancelCause(context.Background())
	return &runtimeDataPlane{wire: dataplane.New(base, token, client), timeout: timeout, lifetime: lifetime, cancel: cancel}
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
	return mapWriteInfo(info), nil
}
func (d *runtimeDataPlane) List(ctx context.Context, path string, depth int, user string) (result []FileInfo, err error) {
	ctx, finish, _ := d.requestOperation(ctx, "Files.List")
	defer finish()
	defer func() { err = operationError(ctx, "Files.List", err) }()
	response, err := d.wire.Filesystem().ListDir(ctx, dataplane.Request(d.wire, &fsproto.ListDirRequest{Path: path, Depth: uint32(depth)}, user))
	if err != nil {
		return nil, err
	}
	out := make([]FileInfo, 0, len(response.Msg.GetEntries()))
	for _, e := range response.Msg.GetEntries() {
		out = append(out, mapProtoFileInfo(e))
	}
	return out, nil
}
func (d *runtimeDataPlane) Run(ctx context.Context, command string, opts CommandOptions) (result CommandResult, err error) {
	ctx, finish, started := d.requestOperation(ctx, "Commands.Run")
	defer finish()
	defer func() { err = operationError(ctx, "Commands.Run", err) }()
	cfg := &processproto.ProcessConfig{Cmd: command, Args: opts.Args, Envs: opts.Env}
	if opts.Cwd != "" {
		cfg.Cwd = &opts.Cwd
	}
	limit := opts.MaxOutputBytes
	if limit == 0 {
		limit = DefaultMaxOutputBytes
	}
	stdout, stderr := &limitedOutput{limit: limit}, &limitedOutput{limit: limit}
	stream, err := d.wire.Process().Start(ctx, dataplane.Request(d.wire, &processproto.StartRequest{Process: cfg}, string(opts.User)))
	if err != nil {
		return CommandResult{}, err
	}
	defer stream.Close()
	var pid uint32
	for stream.Receive() {
		if start := stream.Msg().GetEvent().GetStart(); start != nil {
			pid = start.GetPid()
			break
		}
	}
	if pid == 0 {
		if err := stream.Err(); err != nil {
			return CommandResult{}, err
		}
		return CommandResult{}, codeError(Protocol, "Commands.Run", "START_EVENT_MISSING")
	}
	started()
	for stream.Receive() {
		event := stream.Msg().GetEvent()
		if data := event.GetData(); data != nil {
			stdout.write(data.GetStdout())
			stderr.write(data.GetStderr())
		}
		if end := event.GetEnd(); end != nil {
			return CommandResult{ExitCode: int(end.GetExitCode()), Stdout: stdout.bytes(), Stderr: stderr.bytes(), StdoutTruncated: stdout.truncated, StderrTruncated: stderr.truncated}, nil
		}
	}
	if ctx.Err() != nil && context.Cause(d.lifetime) == nil {
		killCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, _ = d.wire.Process().SendSignal(killCtx, dataplane.Request(d.wire, &processproto.SendSignalRequest{Process: &processproto.ProcessSelector{Selector: &processproto.ProcessSelector_Pid{Pid: pid}}, Signal: processproto.Signal_SIGNAL_SIGKILL}, string(opts.User)))
		code := DeadlineExceeded
		if ctx.Err() == context.Canceled {
			code = Canceled
		}
		return CommandResult{}, &Error{Code: code, Operation: "Commands.Run", Cause: ctx.Err()}
	}
	if err := stream.Err(); err != nil {
		return CommandResult{}, err
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
func mapWriteInfo(v dataplane.WriteInfo) FileInfo {
	out := FileInfo{Name: v.Name, Path: v.Path}
	if v.Type != nil {
		if *v.Type == "dir" {
			out.Type = Directory
		} else {
			out.Type = File
		}
	}
	return out
}
func mapProtoFileInfo(v *fsproto.EntryInfo) FileInfo {
	if v == nil {
		return FileInfo{}
	}
	kind := UnknownFileType
	switch v.GetType() {
	case fsproto.FileType_FILE_TYPE_FILE:
		kind = File
	case fsproto.FileType_FILE_TYPE_DIRECTORY:
		kind = Directory
	}
	out := FileInfo{Name: v.GetName(), Path: v.GetPath(), Type: kind, Size: v.GetSize(), Mode: v.GetMode(), Permissions: v.GetPermissions(), Owner: v.GetOwner(), Group: v.GetGroup()}
	if v.ModifiedTime != nil {
		out.ModifiedAt = v.ModifiedTime.AsTime()
	}
	if v.SymlinkTarget != nil {
		target := v.GetSymlinkTarget()
		out.SymlinkTarget = &target
	}
	return out
}
func mapDataPlaneError(operation string, err error) error {
	if errors.Is(err, dataplane.ErrWriteInfoMissing) {
		return codeError(Protocol, operation, "WRITE_INFO_MISSING")
	}
	var status *dataplane.HTTPError
	if errors.As(err, &status) {
		return dataPlaneHTTPError(operation, status.StatusCode)
	}
	return err
}
func dataPlaneHTTPError(operation string, status int) error {
	code, retryable := Unavailable, true
	switch status {
	case 400:
		code, retryable = InvalidArgument, false
	case 401:
		code, retryable = Unauthenticated, false
	case 403:
		code, retryable = PermissionDenied, false
	case 404:
		code, retryable = NotFound, false
	case 409:
		code, retryable = Conflict, false
	case 429:
		code = ResourceExhausted
	}
	return &Error{Code: code, Operation: operation, Reason: fmt.Sprintf("HTTP_%d", status), Retryable: retryable}
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
