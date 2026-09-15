package ags

import (
	"context"
	"strings"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/model"
	internalruntime "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/runtime"
)

// DefaultMaxOutputBytes limits each retained command-output stream to 4 MiB when no explicit
// limit is supplied.
const DefaultMaxOutputBytes int64 = 4 << 20

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

// ProcessSignal selects explicit remote termination; observation cancellation alone sends no signal.
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

func (d *runtimeDataPlane) Run(ctx context.Context, command string, opts CommandOptions) (CommandResult, error) {
	value, err := d.inner.Run(ctx, model.ProcessConfig{Command: command, Args: opts.Args, Env: opts.Env, CWD: opts.Cwd, User: string(opts.User), MaxOutputBytes: opts.MaxOutputBytes})
	if err != nil {
		return CommandResult{}, normalizeError("Commands.Run", err)
	}
	return commandMapper().Result(value), nil
}

func (d *runtimeDataPlane) Start(ctx context.Context, command string, opts StartOptions) (*CommandHandle, error) {
	handle, err := internalruntime.StartCommand(d.inner, ctx, model.ProcessConfig{Command: command, Args: opts.Args, Env: opts.Env, CWD: opts.Cwd, User: string(opts.User), MaxOutputBytes: opts.MaxOutputBytes}, commandMapper())
	if err != nil {
		return nil, normalizeError("Commands.Start", err)
	}
	return &CommandHandle{inner: handle}, nil
}

func (d *runtimeDataPlane) ConnectCommand(ctx context.Context, pid uint32, opts ConnectCommandOptions) (*CommandHandle, error) {
	handle, err := internalruntime.ConnectCommand(d.inner, ctx, pid, string(opts.User), opts.MaxOutputBytes, commandMapper())
	if err != nil {
		return nil, normalizeError("Commands.Connect", err)
	}
	return &CommandHandle{inner: handle}, nil
}

func (d *runtimeDataPlane) ListCommands(ctx context.Context, user SandboxUser) ([]ProcessInfo, error) {
	values, err := d.inner.ListCommands(ctx, string(user))
	if err != nil {
		return nil, normalizeError("Commands.List", err)
	}
	out := make([]ProcessInfo, 0, len(values))
	for _, value := range values {
		out = append(out, ProcessInfo{PID: value.PID, Tag: value.Tag, Cmd: value.Command, Args: append([]string(nil), value.Args...), Env: cloneStringMap(value.Env), CWD: value.CWD})
	}
	return out, nil
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
	opts.Env = cloneStringMap(opts.Env)
	plane, err := c.sandbox.dataPlane("Commands.Start")
	if err != nil {
		return nil, err
	}
	starter, ok := plane.(commandStarter)
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
	inner *internalruntime.CommandHandle[CommandEvent, CommandResult, ProcessSignal]
}

// PID returns the nonzero runtime PID observed before handle delivery, not a reconnectable identity.
func (h *CommandHandle) PID() uint32 { return h.inner.PID() }

// Events starts one live observer. Repeated calls return the same channel;
// callers must not use multiple consumers. Wait-only callers need not subscribe.
func (h *CommandHandle) Events() <-chan CommandEvent { return h.inner.Events() }

// Wait limits this wait only. Repeated calls return independent result copies.
func (h *CommandHandle) Wait(ctx context.Context) (CommandResult, error) { return h.inner.Wait(ctx) }

// Write serializes SendInput requests and waits for each ACK; it never retries.
func (h *CommandHandle) Write(ctx context.Context, data []byte) error {
	if len(data) == 0 {
		return codeError(InvalidArgument, "Command.Write", "DATA_REQUIRED")
	}
	return h.inner.Write(ctx, data)
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
	return h.inner.Signal(ctx, signal)
}

// Err reports a locally observed terminal failure, or nil if none has been recorded. A nil
// result while running does not imply successful process exit.
func (h *CommandHandle) Err() error { return h.inner.Err() }

// Close stops local observation, preserving an already completed result.
func (h *CommandHandle) Close() error { return h.inner.Close() }

func commandMapper() internalruntime.CommandMapper[CommandEvent, CommandResult, ProcessSignal] {
	return internalruntime.CommandMapper[CommandEvent, CommandResult, ProcessSignal]{
		Event: func(event model.CommandEvent) CommandEvent {
			kind := CommandStdout
			switch event.Kind {
			case model.CommandStderr:
				kind = CommandStderr
			case model.CommandExit:
				kind = CommandExit
			}
			out := CommandEvent{Type: kind, Data: append([]byte(nil), event.Data...)}
			if event.Exit != nil {
				exit := mapExitStatus(*event.Exit)
				out.Exit = &exit
			}
			return out
		},
		Result: func(value model.CommandResult) CommandResult {
			return CommandResult{ExitCode: value.ExitCode, Stdout: value.Stdout, Stderr: value.Stderr, StdoutTruncated: value.StdoutTruncated, StderrTruncated: value.StderrTruncated}
		},
		Signal: func(value ProcessSignal) (model.ProcessSignal, bool) {
			if value == SignalTERM {
				return model.SignalTERM, true
			}
			if value == SignalKILL {
				return model.SignalKILL, true
			}
			return 0, false
		},
		Error: func(err error) error { return normalizeError("Command", err) },
	}
}

func mapExitStatus(value model.ExitStatus) ExitStatus {
	return ExitStatus{Code: value.Code, Exited: value.Exited, Reason: ExitReason(value.Reason), Message: value.Message}
}
