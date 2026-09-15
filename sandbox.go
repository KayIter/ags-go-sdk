package ags

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/model"
	internalruntime "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/runtime"
)

// runtimeDataPlane is a thin public-model adapter. Generation state, request
// lifetime, stream pumps, and bounded queues are owned by internal/runtime.
type runtimeDataPlane struct{ inner *internalruntime.Generation }

func newRuntimeDataPlane(generation *internalruntime.Generation) *runtimeDataPlane {
	return &runtimeDataPlane{inner: generation}
}
func (d *runtimeDataPlane) generation() *internalruntime.Generation { return d.inner }
func (d *runtimeDataPlane) Ready(ctx context.Context) error {
	return normalizeError("dataPlane.Ready", d.inner.Ready(ctx))
}
func (d *runtimeDataPlane) Close() error { return normalizeError("dataPlane.Close", d.inner.Close()) }

func (d *runtimeDataPlane) OpenPTY(ctx context.Context, opts PTYOptions) (*PTYSession, error) {
	handle, err := internalruntime.OpenPTY(d.inner, ctx, model.PTYConfig{ProcessConfig: model.ProcessConfig{Command: opts.Command, Args: append([]string(nil), opts.Args...), Env: cloneStringMap(opts.Env), CWD: opts.Cwd, User: string(opts.User)}, Cols: opts.Cols, Rows: opts.Rows}, internalruntime.PTYMapper[PTYEvent, ExitStatus]{Event: mapPTYEvent, Exit: mapExitStatus, Error: func(err error) error { return normalizeError("PTY", err) }})
	if err != nil {
		return nil, normalizeError("PTY.Open", err)
	}
	return &PTYSession{inner: handle}, nil
}

func mapPTYEvent(value model.PTYEvent) PTYEvent {
	kind := PTYStart
	switch value.Kind {
	case model.PTYOutput:
		kind = PTYOutput
	case model.PTYEnd:
		kind = PTYEnd
	}
	out := PTYEvent{Type: kind, Data: append([]byte(nil), value.Data...)}
	if value.Exit != nil {
		exit := mapExitStatus(*value.Exit)
		out.Exit = &exit
	}
	return out
}

// Sandbox is the only P0 instance object. Its services are facades over one connection manager.
type Sandbox struct {
	id       string
	client   *Client
	owner    *internalruntime.Owner
	files    *Files
	commands *Commands
	pty      *PTY
	code     *Code
	metrics  *Metrics
}

func newSandbox(client *Client, id string, plane dataPlane) *Sandbox {
	var generation *internalruntime.Generation
	if adapter, ok := plane.(*runtimeDataPlane); ok && adapter != nil {
		generation = adapter.generation()
	}
	s := &Sandbox{id: id, client: client, owner: internalruntime.NewOwner(generation)}
	s.files = &Files{s}
	s.commands = &Commands{s}
	s.pty = &PTY{s}
	s.code = &Code{sandbox: s}
	s.metrics = &Metrics{s}
	return s
}

// ID returns the immutable remote identity.
func (s *Sandbox) ID() string { return s.id }

// GetHost returns the public hostname for a valid Sandbox port. It does not include a URL
// scheme or credentials and does not probe whether the selected Tool exposes the port.
func (s *Sandbox) GetHost(port int) (string, error) {
	if port < 1 || port > 65535 {
		return "", codeError(InvalidArgument, "Sandbox.GetHost", "PORT_OUT_OF_RANGE")
	}
	return fmt.Sprintf("%d-%s.%s.tencentags.com", port, s.id, s.client.cfg.region), nil
}

// Files returns the filesystem service bound to this Sandbox. The service follows
// the Sandbox generation and cannot be rebound to another Client or instance.
func (s *Sandbox) Files() *Files { return s.files }

// Commands returns the process service bound to this Sandbox. Started handles are
// invalidated when the Sandbox generation changes.
func (s *Sandbox) Commands() *Commands { return s.commands }

// PTY returns the interactive-terminal service bound to this Sandbox.
func (s *Sandbox) PTY() *PTY { return s.pty }

// Code returns the execution service bound to this Sandbox and its current generation.
func (s *Sandbox) Code() *Code { return s.code }

// Metrics returns the Cloud Monitor service bound to the same Client identity as
// the Sandbox control plane.
func (s *Sandbox) Metrics() *Metrics { return s.metrics }

// Info reads current control-plane information without changing lifetime or acquiring envd.
func (s *Sandbox) Info(ctx context.Context) (SandboxInfo, error) {
	return s.client.Sandboxes().Get(ctx, s.id)
}

// WaitFor polls until target is observed or ctx expires. Failed and Stopped states reject
// other targets. Waiting does not mutate the sandbox.
func (s *Sandbox) WaitFor(ctx context.Context, target SandboxState) (SandboxInfo, error) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		info, err := s.Info(ctx)
		if err != nil {
			return SandboxInfo{}, err
		}
		if info.State == target {
			return info, nil
		}
		if info.State == Failed || info.State == Stopped {
			return SandboxInfo{}, codeError(Conflict, "Sandbox.WaitFor", "TERMINAL_STATE")
		}
		select {
		case <-ctx.Done():
			return SandboxInfo{}, normalizeError("Sandbox.WaitFor", ctx.Err())
		case <-ticker.C:
		}
	}
}

// Pause invalidates existing local streams before requesting pause. Empty Mode selects memory.
// Old handles remain invalid even if the pause fails; use the current generation or reconnect.
// Cancellation does not establish the remote outcome.
func (s *Sandbox) Pause(ctx context.Context, opts PauseOptions) (SandboxInfo, error) {
	if err := s.owner.Lock(context.Background()); err != nil {
		return SandboxInfo{}, normalizeError("Sandbox.Pause", err)
	}
	defer s.owner.Unlock()
	if s.isClosed() {
		return SandboxInfo{}, codeError(Conflict, "Sandbox.Pause", "SANDBOX_CLOSED")
	}
	if opts.Mode == "" {
		opts.Mode = PauseMemory
	}
	if opts.Mode != PauseMemory && opts.Mode != PauseDisk {
		return SandboxInfo{}, codeError(InvalidArgument, "Sandbox.Pause", "INVALID_PAUSE_MODE")
	}
	_ = s.invalidate()
	info, err := s.client.cfg.control.Pause(ctx, s.id, opts)
	if err != nil {
		if plane, reconnectErr := s.client.cfg.control.dataPlane(ctx, s.id); reconnectErr == nil {
			if readyErr := plane.Ready(ctx); readyErr == nil {
				if adapter, ok := plane.(*runtimeDataPlane); ok {
					_ = s.owner.Replace(adapter.generation())
				}
			} else {
				_ = plane.Close()
			}
		}
	}
	return info, err
}

// Resume requests resume, acquires fresh connection material and waits for data-plane
// readiness. It never revives a locally closed Sandbox; obtain a new handle with Connect
// instead.
func (s *Sandbox) Resume(ctx context.Context, opts ResumeOptions) (SandboxInfo, error) {
	copy, err := copyResumeOptions(opts)
	if err != nil {
		return SandboxInfo{}, err
	}
	if err := s.owner.Lock(context.Background()); err != nil {
		return SandboxInfo{}, normalizeError("Sandbox.Resume", err)
	}
	defer s.owner.Unlock()
	if s.isClosed() {
		return SandboxInfo{}, codeError(Conflict, "Sandbox.Resume", "SANDBOX_CLOSED")
	}
	return s.resume(ctx, copy)
}
func (s *Sandbox) resume(ctx context.Context, opts ResumeOptions) (SandboxInfo, error) {
	info, err := s.client.cfg.control.Resume(ctx, s.id, opts)
	if err != nil {
		return SandboxInfo{}, err
	}
	plane, err := s.client.cfg.control.dataPlane(ctx, s.id)
	if err != nil {
		return SandboxInfo{}, err
	}
	if err := plane.Ready(ctx); err != nil {
		_ = plane.Close()
		return SandboxInfo{}, err
	}
	adapter, ok := plane.(*runtimeDataPlane)
	if !ok {
		_ = plane.Close()
		return SandboxInfo{}, codeError(Internal, "Sandbox.Resume", "RUNTIME_GENERATION_MISSING")
	}
	if err := s.owner.Replace(adapter.generation()); err != nil {
		return SandboxInfo{}, normalizeError("Sandbox.Resume", err)
	}
	return info, nil
}

// Delete invalidates local streams and explicitly stops the remote instance, resuming a paused
// Cloud instance first if needed. It is not an ownership check; only delete resources the
// caller is authorized to remove.
func (s *Sandbox) Delete(ctx context.Context) error {
	if err := s.owner.Lock(context.Background()); err != nil {
		return normalizeError("Sandbox.Delete", err)
	}
	defer s.owner.Unlock()
	_ = s.invalidate()
	return s.client.Sandboxes().Delete(ctx, s.id)
}
func (s *Sandbox) invalidate() error {
	return normalizeError("Sandbox.invalidate", s.owner.Invalidate())
}
func (s *Sandbox) dataPlane(op string) (dataPlane, error) {
	generation, err := s.owner.Generation(op)
	if err != nil {
		return nil, normalizeError(op, err)
	}
	return &runtimeDataPlane{inner: generation}, nil
}

// Close releases this handle's local data-plane resources. It is idempotent and
// never deletes the remote sandbox or closes a shared HTTP client. Info, Metrics
// and explicit Delete remain available; Connect creates a new usable handle.
func (s *Sandbox) Close() error {
	if err := s.owner.Lock(context.Background()); err != nil {
		return normalizeError("Sandbox.Close", err)
	}
	defer s.owner.Unlock()
	return normalizeError("Sandbox.Close", s.owner.Close())
}

func (s *Sandbox) isClosed() bool { return s.owner.IsClosed() }

// Files operates on the current data-plane generation; filesystem enforcement belongs to the
// runtime.
type Files struct{ sandbox *Sandbox }

// Read opens a streamed response for a nonempty path. Close the returned reader even after a
// partial read; its context and sandbox generation govern its lifetime.
func (f *Files) Read(ctx context.Context, path string, opts ReadOptions) (io.ReadCloser, error) {
	if path == "" {
		return nil, codeError(InvalidArgument, "Files.Read", "PATH_REQUIRED")
	}
	if err := validateUser(opts.User, "Files.Read"); err != nil {
		return nil, err
	}
	p, e := f.sandbox.dataPlane("Files.Read")
	if e != nil {
		return nil, e
	}
	return p.Read(ctx, path, string(opts.User))
}

// Write incrementally uploads body to a nonempty remote path without whole-file buffering or
// replay. The caller owns body and any underlying closer; arrange for blocked reads to unblock
// on cancellation. Failure may leave partial remote data.
func (f *Files) Write(ctx context.Context, path string, body io.Reader, opts WriteOptions) (FileInfo, error) {
	if path == "" || body == nil {
		return FileInfo{}, codeError(InvalidArgument, "Files.Write", "PATH_AND_CONTENT_REQUIRED")
	}
	if err := validateUser(opts.User, "Files.Write"); err != nil {
		return FileInfo{}, err
	}
	p, e := f.sandbox.dataPlane("Files.Write")
	if e != nil {
		return FileInfo{}, e
	}
	return p.Write(ctx, path, body, string(opts.User))
}

// List returns metadata under a nonempty runtime path. Zero Depth selects 1; negative depth is
// invalid. Missing types or link targets are not inferred.
func (f *Files) List(ctx context.Context, path string, opts FileListOptions) ([]FileInfo, error) {
	if path == "" {
		return nil, codeError(InvalidArgument, "Files.List", "PATH_REQUIRED")
	}
	if opts.Depth < 0 {
		return nil, codeError(InvalidArgument, "Files.List", "DEPTH_NEGATIVE")
	}
	if err := validateUser(opts.User, "Files.List"); err != nil {
		return nil, err
	}
	if opts.Depth == 0 {
		opts.Depth = 1
	}
	p, e := f.sandbox.dataPlane("Files.List")
	if e != nil {
		return nil, e
	}
	return p.List(ctx, path, opts.Depth, string(opts.User))
}

// Watch waits for the start barrier and returns a bounded notification stream. Drain events
// promptly and close when finished. Context cancellation or generation replacement ends this
// non-resumable observation.
func (f *Files) Watch(ctx context.Context, path string, opts WatchOptions) (*WatchHandle, error) {
	if path == "" {
		return nil, codeError(InvalidArgument, "Files.Watch", "PATH_REQUIRED")
	}
	if opts.Buffer < 0 {
		return nil, codeError(InvalidArgument, "Files.Watch", "BUFFER_NEGATIVE")
	}
	if err := validateUser(opts.User, "Files.Watch"); err != nil {
		return nil, err
	}
	p, e := f.sandbox.dataPlane("Files.Watch")
	if e != nil {
		return nil, e
	}
	return p.Watch(ctx, path, opts)
}

// WatchHandle owns one bounded local notification stream. Close it when finished; it cannot
// survive generation replacement.
type WatchHandle struct {
	inner *internalruntime.WatchHandle[FileEvent]
}

// DefaultWatchBuffer is the pending-event capacity when WatchOptions.Buffer is zero.
const DefaultWatchBuffer = 32

// Events returns the watch's channel; use one consumer and drain until closed before
// inspecting Err.
func (h *WatchHandle) Events() <-chan FileEvent { return h.inner.Events() }

// Err returns the last observed stream error, including overflow. Nil before channel closure
// does not prove successful completion.
func (h *WatchHandle) Err() error { return h.inner.Err() }

// Close idempotently ends local observation without deleting files or the remote sandbox.
func (h *WatchHandle) Close() error { return h.inner.Close() }

// Commands executes runtime processes without automatic replay of command side effects.
type Commands struct{ sandbox *Sandbox }

// Run executes one required runtime command and returns bounded stdout/stderr plus exit code.
// Nonzero process exit is data, not a transport error. There is no automatic command replay;
// cancellation does not prove remote termination.
func (c *Commands) Run(ctx context.Context, command string, opts CommandOptions) (CommandResult, error) {
	if command == "" {
		return CommandResult{}, codeError(InvalidArgument, "Commands.Run", "COMMAND_REQUIRED")
	}
	if opts.MaxOutputBytes < 0 {
		return CommandResult{}, codeError(InvalidArgument, "Commands.Run", "MAX_OUTPUT_BYTES_NEGATIVE")
	}
	if err := validateUser(opts.User, "Commands.Run"); err != nil {
		return CommandResult{}, err
	}
	p, e := c.sandbox.dataPlane("Commands.Run")
	if e != nil {
		return CommandResult{}, e
	}
	return p.Run(ctx, command, opts)
}

// PTY opens generation-bound interactive terminals with explicit input and resize operations.
type PTY struct{ sandbox *Sandbox }

// Open waits for the terminal start barrier. Supply a command and positive Cols/Rows, then
// drain Events concurrently with Wait to avoid bounded-buffer overflow. Close the session when
// finished.
func (p *PTY) Open(ctx context.Context, opts PTYOptions) (*PTYSession, error) {
	if opts.Command == "" || opts.Cols < 1 || opts.Rows < 1 {
		return nil, codeError(InvalidArgument, "PTY.Open", "COMMAND_AND_SIZE_REQUIRED")
	}
	if err := validateUser(opts.User, "PTY.Open"); err != nil {
		return nil, err
	}
	d, e := p.sandbox.dataPlane("PTY.Open")
	if e != nil {
		return nil, e
	}
	return d.OpenPTY(ctx, opts)
}

func validateUser(user SandboxUser, operation string) error {
	if user == "" || user == User || user == Root {
		return nil
	}
	return codeError(InvalidArgument, operation, "INVALID_SANDBOX_USER")
}

// PTYSession owns one terminal stream. Drain Events concurrently with Wait because output
// always enters the bounded event channel.
type PTYSession struct {
	inner *internalruntime.PTYSession[PTYEvent, ExitStatus]
}

// ID returns a stable SDK-local handle identity, not a server reconnect token.
func (s *PTYSession) ID() string { return s.inner.ID() }

// Events returns the bounded 32-event terminal channel. Consume continuously even while
// waiting; overflow fails with ResourceExhausted.
func (s *PTYSession) Events() <-chan PTYEvent { return s.inner.Events() }

// Write sends nonempty terminal input once. Do not mutate data during the call; cancellation
// cannot undo accepted input.
func (s *PTYSession) Write(ctx context.Context, data []byte) error {
	if len(data) == 0 {
		return codeError(InvalidArgument, "PTY.Write", "DATA_REQUIRED")
	}
	return s.inner.Write(ctx, append([]byte(nil), data...))
}

// Resize requests positive terminal dimensions in character cells without replay.
func (s *PTYSession) Resize(ctx context.Context, cols, rows uint32) error {
	if cols < 1 || rows < 1 {
		return codeError(InvalidArgument, "PTY.Resize", "SIZE_REQUIRED")
	}
	return s.inner.Resize(ctx, cols, rows)
}

// Wait observes terminal status until ctx expires; it does not signal the process. Drain
// Events concurrently, since unconsumed output can fail the observation.
func (s *PTYSession) Wait(ctx context.Context) (ExitStatus, error) { return s.inner.Wait(ctx) }

// Close idempotently sends TERM to a still-live process with a three-second signal-request
// budget, then closes local resources. It does not wait for proof of exit or send KILL.
// Generation invalidation instead closes locally without signaling; neither path deletes the
// sandbox.
func (s *PTYSession) Close() error { return s.inner.Close() }
