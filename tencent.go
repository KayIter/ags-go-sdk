package ags

import (
	"bytes"
	"connectrpc.com/connect"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	fsproto "github.com/TencentCloudAgentRuntime/ags-go-sdk/pb/filesystem"
	fsconnect "github.com/TencentCloudAgentRuntime/ags-go-sdk/pb/filesystem/filesystemconnect"
	processproto "github.com/TencentCloudAgentRuntime/ags-go-sdk/pb/process"
	processconnect "github.com/TencentCloudAgentRuntime/ags-go-sdk/pb/process/processconnect"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type legacyDataPlane struct {
	config   *dataPlaneConfig
	client   *http.Client
	timeout  time.Duration
	files    fsconnect.FilesystemClient
	process  processconnect.ProcessClient
	mu       sync.Mutex
	handles  []interface{ invalidate() error }
	closed   bool
	lifetime context.Context
	cancel   context.CancelCauseFunc
}
type dataPlaneConfig struct{ BaseURL, Domain, AccessToken string }

func newLegacyDataPlane(host, token string) *legacyDataPlane {
	return newDataPlane("https://"+host, token, &http.Client{})
}
func newDataPlane(base, token string, client *http.Client) *legacyDataPlane {
	return newDataPlaneWithTimeout(base, token, client, 30*time.Second)
}
func newDataPlaneWithTimeout(base, token string, client *http.Client, timeout time.Duration) *legacyDataPlane {
	u, _ := url.Parse(base)
	cfg := &dataPlaneConfig{BaseURL: base, Domain: u.Host, AccessToken: token}
	lifetime, cancel := context.WithCancelCause(context.Background())
	return &legacyDataPlane{config: cfg, client: client, timeout: timeout, lifetime: lifetime, cancel: cancel, files: fsconnect.NewFilesystemClient(client, base, connect.WithProtoJSON()), process: processconnect.NewProcessClient(client, base, connect.WithProtoJSON())}
}
func (d *legacyDataPlane) Read(ctx context.Context, path, user string) (out io.ReadCloser, err error) {
	ctx, finish, started := d.requestOperation(ctx, "Files.Read")
	defer func() {
		err = operationError(ctx, "Files.Read", err)
		if out == nil {
			finish()
		}
	}()
	user = dataPlaneUser(user)
	u, _ := url.Parse(d.config.BaseURL + "/files")
	query := u.Query()
	query.Set("path", path)
	query.Set("username", user)
	u.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	d.setHeaders(req, user)
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_ = resp.Body.Close()
		return nil, dataPlaneHTTPError("Files.Read", resp.StatusCode)
	}
	started()
	return &generationReader{ReadCloser: resp.Body, ctx: ctx, finish: finish, op: "Files.Read"}, nil
}
func (d *legacyDataPlane) Write(ctx context.Context, path string, body io.Reader, user string) (out FileInfo, err error) {
	ctx, finish, _ := d.requestOperation(ctx, "Files.Write")
	defer finish()
	defer func() { err = operationError(ctx, "Files.Write", err) }()
	user = dataPlaneUser(user)
	pipeReader, pipeWriter := io.Pipe()
	multipartWriter := multipart.NewWriter(pipeWriter)
	copyDone := make(chan error, 1)
	go func() {
		part, err := multipartWriter.CreateFormFile("file", path)
		if err == nil {
			_, err = io.Copy(part, body)
		}
		if closeErr := multipartWriter.Close(); err == nil {
			err = closeErr
		}
		_ = pipeWriter.CloseWithError(err)
		copyDone <- err
	}()
	u, _ := url.Parse(d.config.BaseURL + "/files")
	query := u.Query()
	query.Set("path", path)
	query.Set("username", user)
	u.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), pipeReader)
	if err != nil {
		_ = pipeReader.Close()
		return FileInfo{}, err
	}
	req.Header.Set("Content-Type", multipartWriter.FormDataContentType())
	req.Header.Set("X-Access-Token", d.config.AccessToken)
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(user+":")))
	resp, err := d.client.Do(req)
	if err != nil {
		_ = pipeReader.Close()
		return FileInfo{}, err
	}
	defer resp.Body.Close()
	if copyErr := <-copyDone; copyErr != nil {
		return FileInfo{}, copyErr
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return FileInfo{}, dataPlaneHTTPError("Files.Write", resp.StatusCode)
	}
	var infos []writeInfo
	if err := json.NewDecoder(resp.Body).Decode(&infos); err != nil {
		return FileInfo{}, err
	}
	if len(infos) == 0 {
		return FileInfo{}, codeError(Protocol, "Files.Write", "WRITE_INFO_MISSING")
	}
	return mapWriteInfo(infos[0]), nil
}
func (d *legacyDataPlane) List(ctx context.Context, path string, depth int, user string) (result []FileInfo, err error) {
	ctx, finish, _ := d.requestOperation(ctx, "Files.List")
	defer finish()
	defer func() { err = operationError(ctx, "Files.List", err) }()
	response, err := d.files.ListDir(ctx, dataPlaneRequest(&fsproto.ListDirRequest{Path: path, Depth: uint32(depth)}, d.config.AccessToken, dataPlaneUser(user)))
	if err != nil {
		return nil, err
	}
	out := make([]FileInfo, 0, len(response.Msg.GetEntries()))
	for _, e := range response.Msg.GetEntries() {
		out = append(out, mapProtoFileInfo(e))
	}
	return out, nil
}
func (d *legacyDataPlane) Run(ctx context.Context, command string, opts CommandOptions) (result CommandResult, err error) {
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
	stream, err := d.process.Start(ctx, dataPlaneRequest(&processproto.StartRequest{Process: cfg}, d.config.AccessToken, dataPlaneUser(string(opts.User))))
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
		_, _ = d.process.SendSignal(killCtx, dataPlaneRequest(&processproto.SendSignalRequest{Process: &processproto.ProcessSelector{Selector: &processproto.ProcessSelector_Pid{Pid: pid}}, Signal: processproto.Signal_SIGNAL_SIGKILL}, d.config.AccessToken, dataPlaneUser(string(opts.User))))
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
func dataPlaneUser(v string) string {
	if v == "" || v == string(User) {
		return "user"
	}
	if v == string(Root) {
		return "root"
	}
	return v
}

type writeInfo struct {
	Name string  `json:"name"`
	Type *string `json:"type"`
	Path string  `json:"path"`
}

func mapWriteInfo(v writeInfo) FileInfo {
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
func (d *legacyDataPlane) setHeaders(req *http.Request, user string) {
	req.Header.Set("X-Access-Token", d.config.AccessToken)
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(user+":")))
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
func (d *legacyDataPlane) Ready(ctx context.Context) error {
	if _, err := d.List(ctx, "/tmp", 1, "user"); err == nil {
		return nil
	}
	_, err := d.List(ctx, "/tmp", 1, "root")
	return err
}
func (d *legacyDataPlane) register(handle interface{ invalidate() error }) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return codeError(InstancePaused, "dataPlane", "INSTANCE_PAUSED")
	}
	d.handles = append(d.handles, handle)
	return nil
}
func (d *legacyDataPlane) Close() error {
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
