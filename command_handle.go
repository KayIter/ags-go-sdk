package ags

import (
	"context"
	"strings"
	"sync"
)

// StartOptions configures one new process, not the sandbox's persistent environment.
type StartOptions struct {
	// Args supplies runtime command arguments; nil means none.
	Args []string
	// Env supplies per-process variables; nil means no overrides. The SDK copies the map at
	// startup.
	Env map[string]string
	// Cwd is the working directory; empty preserves the runtime default.
	Cwd string
	// User selects the runtime account; empty defaults to User.
	User SandboxUser
	// MaxOutputBytes bounds each retained output stream separately; zero selects
	// DefaultMaxOutputBytes and negative values are invalid.
	MaxOutputBytes int64
}

// ProcessSignal selects explicit remote termination; observation cancellation alone sends no
// signal.
type ProcessSignal string

const (
	// SignalTERM requests cooperative remote termination.
	SignalTERM ProcessSignal = "TERM"
	// SignalKILL requests forced remote termination.
	SignalKILL ProcessSignal = "KILL"
)

// CommandEventType distinguishes retained output from terminal status.
type CommandEventType string

const (
	// CommandStdout carries retained standard-output bytes.
	CommandStdout CommandEventType = "STDOUT"
	// CommandStderr carries retained standard-error bytes.
	CommandStderr CommandEventType = "STDERR"
	// CommandExit carries terminal status independently retained by Wait.
	CommandExit CommandEventType = "EXIT"
)

// CommandEvent contains caller-owned output or an exit status; there is no replay.
type CommandEvent struct {
	// Type identifies standard output, standard error or terminal exit.
	Type CommandEventType
	// Data contains caller-owned bytes for output events; exit events may have none.
	Data []byte
	// Exit is the terminal status for CommandExit, otherwise nil.
	Exit *ExitStatus
}

type commandStarter interface {
	Start(context.Context, string, StartOptions) (*CommandHandle, error)
}

type commandConnector interface {
	ConnectCommand(context.Context, uint32, ConnectCommandOptions) (*CommandHandle, error)
	ListCommands(context.Context, SandboxUser) ([]ProcessInfo, error)
}

// ConnectCommandOptions configures observation and control of an existing runtime process.
type ConnectCommandOptions struct {
	// User selects the runtime account used for subsequent input and signals.
	User SandboxUser
	// MaxOutputBytes bounds newly observed stdout and stderr independently.
	MaxOutputBytes int64
}

// CommandListOptions configures the runtime account used to list processes.
type CommandListOptions struct {
	// User selects the runtime account; empty defaults to User.
	User SandboxUser
}

// ProcessInfo describes a runtime process without exposing protobuf values.
type ProcessInfo struct {
	// PID is the runtime process identifier in the current Sandbox generation.
	PID uint32
	// Tag is an optional runtime-defined process label.
	Tag *string
	// Cmd is the reported executable or command.
	Cmd string
	// Args contains a caller-owned copy of reported arguments.
	Args []string
	// Env contains a caller-owned copy of reported process environment entries.
	Env map[string]string
	// CWD is the reported working directory when available.
	CWD *string
}
type commandFrame struct {
	kind CommandEventType
	data []byte
	exit *ExitStatus
}
type commandTransport interface {
	receive() (commandFrame, error)
	input(context.Context, []byte) error
	signal(context.Context, ProcessSignal) error
	currentError() error
	close()
}

// Start waits for a nonzero PID. Its context owns the observation lifetime;
// cancellation/Close never sends a remote signal. Use Signal explicitly.
func (c *Commands) Start(ctx context.Context, command string, opts StartOptions) (*CommandHandle, error) {
	if strings.TrimSpace(command) == "" {
		return nil, codeError(InvalidArgument, "Commands.Start", "COMMAND_REQUIRED")
	}
	if opts.MaxOutputBytes < 0 {
		return nil, codeError(InvalidArgument, "Commands.Start", "MAX_OUTPUT_BYTES_NEGATIVE")
	}
	if err := validateUser(opts.User, "Commands.Start"); err != nil {
		return nil, err
	}
	opts.Args = append([]string(nil), opts.Args...)
	env := make(map[string]string, len(opts.Env))
	for k, v := range opts.Env {
		env[k] = v
	}
	opts.Env = env
	p, err := c.sandbox.dataPlane("Commands.Start")
	if err != nil {
		return nil, err
	}
	starter, ok := p.(commandStarter)
	if !ok {
		return nil, codeError(Unsupported, "Commands.Start", "COMMAND_START_UNAVAILABLE")
	}
	return starter.Start(ctx, command, opts)
}

// Connect attaches a generation-bound handle to an existing nonzero PID. PID validity and
// cross-generation availability are decided by the runtime; a new Sandbox generation does not
// make an old handle reusable.
func (c *Commands) Connect(ctx context.Context, pid uint32, opts ConnectCommandOptions) (*CommandHandle, error) {
	if pid == 0 {
		return nil, codeError(InvalidArgument, "Commands.Connect", "PID_REQUIRED")
	}
	if opts.MaxOutputBytes < 0 {
		return nil, codeError(InvalidArgument, "Commands.Connect", "MAX_OUTPUT_BYTES_NEGATIVE")
	}
	if err := validateUser(opts.User, "Commands.Connect"); err != nil {
		return nil, err
	}
	plane, err := c.sandbox.dataPlane("Commands.Connect")
	if err != nil {
		return nil, err
	}
	connector, ok := plane.(commandConnector)
	if !ok {
		return nil, codeError(Unsupported, "Commands.Connect", "COMMAND_CONNECT_UNAVAILABLE")
	}
	return connector.ConnectCommand(ctx, pid, opts)
}

// List returns the processes visible in the current Sandbox generation. At most one options
// value is accepted; omitting it selects User.
func (c *Commands) List(ctx context.Context, options ...CommandListOptions) ([]ProcessInfo, error) {
	if len(options) > 1 {
		return nil, codeError(InvalidArgument, "Commands.List", "SINGLE_OPTIONS_REQUIRED")
	}
	var opts CommandListOptions
	if len(options) == 1 {
		opts = options[0]
	}
	if err := validateUser(opts.User, "Commands.List"); err != nil {
		return nil, err
	}
	plane, err := c.sandbox.dataPlane("Commands.List")
	if err != nil {
		return nil, err
	}
	connector, ok := plane.(commandConnector)
	if !ok {
		return nil, codeError(Unsupported, "Commands.List", "COMMAND_LIST_UNAVAILABLE")
	}
	return connector.ListCommands(ctx, opts.User)
}

// CommandHandle observes one process in one data-plane generation. Close is
// local-only; PID must never be reused to control a process after reconnection.
type CommandHandle struct {
	pid                              uint32
	transport                        commandTransport
	mu                               sync.Mutex
	writeGate                        lifecycleMutex
	done                             chan struct{}
	terminal                         bool
	result                           CommandResult
	failure                          error
	limit                            int64
	stdout, stderr                   []byte
	stdoutTruncated, stderrTruncated bool
	observed                         bool
	events                           chan CommandEvent
	queue                            []CommandEvent
	queuedBytes                      int
	wake                             chan struct{}
	deliveryStop                     chan struct{}
	stopOnce                         sync.Once
	exit                             *ExitStatus
}

func newCommandHandle(pid uint32, t commandTransport, limit int64) *CommandHandle {
	if limit == 0 {
		limit = DefaultMaxOutputBytes
	}
	h := &CommandHandle{pid: pid, transport: t, limit: limit, done: make(chan struct{}), wake: make(chan struct{}, 1), deliveryStop: make(chan struct{})}
	go h.pump()
	return h
}

// PID returns the nonzero runtime PID observed before handle delivery, not a reconnectable
// identity.
func (h *CommandHandle) PID() uint32 { return h.pid }

// Events starts one live observer. Repeated calls return the same channel;
// callers must not use multiple consumers. Wait-only callers need not subscribe.
func (h *CommandHandle) Events() <-chan CommandEvent {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.events != nil {
		return h.events
	}
	h.events = make(chan CommandEvent)
	if h.terminal {
		close(h.events)
		return h.events
	}
	h.observed = true
	go h.dispatch()
	return h.events
}
func (h *CommandHandle) notify() {
	select {
	case h.wake <- struct{}{}:
	default:
	}
}
func (h *CommandHandle) finish(exit *ExitStatus, err error) {
	h.mu.Lock()
	if h.terminal {
		h.mu.Unlock()
		return
	}
	h.terminal = true
	h.failure = err
	h.exit = exit
	if err == nil && exit != nil {
		h.result = CommandResult{ExitCode: exit.Code, Stdout: h.stdout, Stderr: h.stderr, StdoutTruncated: h.stdoutTruncated, StderrTruncated: h.stderrTruncated}
	} else {
		h.queue = nil
		h.queuedBytes = 0
	}
	close(h.done)
	h.mu.Unlock()
	h.notify()
	h.transport.close()
}
func (h *CommandHandle) pump() {
	for {
		f, err := h.transport.receive()
		if err != nil {
			h.finish(nil, err)
			return
		}
		if f.exit != nil {
			h.finish(f.exit, nil)
			return
		}
		if len(f.data) == 0 {
			continue
		}
		if !h.output(f.kind, f.data) {
			return
		}
	}
}
func (h *CommandHandle) output(kind CommandEventType, data []byte) bool {
	h.mu.Lock()
	if h.terminal {
		h.mu.Unlock()
		return false
	}
	output, truncated := &h.stdout, &h.stdoutTruncated
	if kind == CommandStderr {
		output, truncated = &h.stderr, &h.stderrTruncated
	}
	remaining := h.limit - int64(len(*output))
	if int64(len(data)) > remaining {
		*truncated = true
		data = data[:remaining]
	}
	*output = append(*output, data...)
	for h.observed && len(data) > 0 {
		n := len(data)
		if n > 64<<10 {
			n = 64 << 10
		}
		if len(h.queue) >= 32 || h.queuedBytes+n > 1<<20 {
			h.mu.Unlock()
			h.finish(nil, codeError(ResourceExhausted, "Commands.events", "COMMAND_BUFFER_FULL"))
			return false
		}
		h.queue = append(h.queue, CommandEvent{Type: kind, Data: append([]byte(nil), data[:n]...)})
		h.queuedBytes += n
		data = data[n:]
	}
	h.mu.Unlock()
	h.notify()
	return true
}
func (h *CommandHandle) dispatch() {
	defer close(h.events)
	for {
		h.mu.Lock()
		if h.terminal && h.failure != nil {
			h.mu.Unlock()
			return
		}
		if len(h.queue) > 0 {
			event := h.queue[0]
			h.mu.Unlock()
			select {
			case h.events <- event:
				h.mu.Lock()
				if len(h.queue) > 0 {
					h.queuedBytes -= len(h.queue[0].Data)
					h.queue[0] = CommandEvent{}
					h.queue = h.queue[1:]
				}
				h.mu.Unlock()
			case <-h.deliveryStop:
				return
			case <-h.wake:
			}
			continue
		}
		if h.terminal {
			exit := h.exit
			h.mu.Unlock()
			if exit != nil {
				copy := *exit
				select {
				case h.events <- CommandEvent{Type: CommandExit, Exit: &copy}:
				case <-h.deliveryStop:
				}
			}
			return
		}
		h.mu.Unlock()
		select {
		case <-h.wake:
		case <-h.deliveryStop:
			return
		}
	}
}

// Wait limits this wait only. Repeated calls return independent result copies.
func (h *CommandHandle) Wait(ctx context.Context) (CommandResult, error) {
	if err := ctx.Err(); err != nil {
		return CommandResult{}, normalizeError("Command.Wait", err)
	}
	select {
	case <-h.done:
	case <-ctx.Done():
		return CommandResult{}, normalizeError("Command.Wait", ctx.Err())
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	r := h.result
	r.Stdout = append([]byte(nil), r.Stdout...)
	r.Stderr = append([]byte(nil), r.Stderr...)
	return r, h.failure
}
func (h *CommandHandle) active() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.terminal {
		if h.failure != nil {
			return h.failure
		}
		return codeError(Conflict, "Command", "COMMAND_ENDED")
	}
	return h.transport.currentError()
}

// Write serializes SendInput requests and waits for each ACK; it never retries.
func (h *CommandHandle) Write(ctx context.Context, data []byte) error {
	if len(data) == 0 {
		return codeError(InvalidArgument, "Command.Write", "DATA_REQUIRED")
	}
	data = append([]byte(nil), data...)
	if err := h.writeGate.acquire(ctx); err != nil {
		return normalizeError("Command.Write", err)
	}
	defer h.writeGate.Unlock()
	if err := h.active(); err != nil {
		return err
	}
	return h.transport.input(ctx, data)
}

// Signal sends TERM or KILL once to the observed process. Cancellation cannot undo a submitted
// signal, and request success does not establish process exit; use Wait.
func (h *CommandHandle) Signal(ctx context.Context, signal ProcessSignal) error {
	if signal != SignalTERM && signal != SignalKILL {
		return codeError(InvalidArgument, "Command.Signal", "SIGNAL_INVALID")
	}
	if err := ctx.Err(); err != nil {
		return normalizeError("Command.Signal", err)
	}
	if err := h.active(); err != nil {
		return err
	}
	return h.transport.signal(ctx, signal)
}

// Err reports a locally observed terminal failure, or nil if none has been recorded. A nil
// result while running does not imply successful process exit.
func (h *CommandHandle) Err() error { h.mu.Lock(); defer h.mu.Unlock(); return h.failure }

// Close stops local observation, preserving an already completed result.
func (h *CommandHandle) Close() error {
	h.finish(nil, codeError(Canceled, "Command.Close", "COMMAND_HANDLE_CLOSED"))
	h.stopOnce.Do(func() { close(h.deliveryStop) })
	return nil
}
