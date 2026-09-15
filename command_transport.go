package ags

import (
	"context"
	"io"
	"sync"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/dataplane"
)

type nativeCommandTransport struct {
	plane  *runtimeDataPlane
	ctx    context.Context
	finish func()
	once   sync.Once
	stream *dataplane.ProcessStream
	pid    uint32
	user   string
}

func (d *runtimeDataPlane) Start(ctx context.Context, command string, opts StartOptions) (out *CommandHandle, err error) {
	ctx, finish, started := d.requestOperation(ctx, "Commands.Start")
	defer func() {
		if out == nil {
			err = operationError(ctx, "Commands.Start", err)
			finish()
		}
	}()
	user := string(opts.User)
	stream, err := d.wire.StartProcess(ctx, dataplane.ProcessConfig{Command: command, Args: opts.Args, Env: opts.Env, CWD: opts.Cwd}, user)
	if err != nil {
		return nil, err
	}
	started()
	return newCommandHandle(stream.PID, &nativeCommandTransport{plane: d, ctx: ctx, finish: finish, pid: stream.PID, user: user, stream: stream}, opts.MaxOutputBytes), nil
}
func (t *nativeCommandTransport) currentError() error { return operationError(t.ctx, "Command", nil) }
func (t *nativeCommandTransport) close() {
	t.once.Do(func() {
		t.finish()
		if t.stream != nil {
			_ = t.stream.Close()
		}
	})
}
func (t *nativeCommandTransport) receive() (commandFrame, error) {
	for {
		event, err := t.stream.Recv()
		if err != nil {
			if err == io.EOF {
				err = codeError(Protocol, "Command.events", "END_EVENT_MISSING")
			}
			return commandFrame{}, operationError(t.ctx, "Command.events", err)
		}
		if err := t.currentError(); err != nil {
			return commandFrame{}, err
		}
		switch event.Kind {
		case dataplane.ProcessStdout:
			return commandFrame{kind: CommandStdout, data: event.Data}, nil
		case dataplane.ProcessStderr:
			return commandFrame{kind: CommandStderr, data: event.Data}, nil
		case dataplane.ProcessPTY:
			return commandFrame{}, codeError(Protocol, "Command.events", "COMMAND_OUTPUT_INVALID")
		case dataplane.ProcessEnd:
			end := event.Exit
			reason := ExitReason("UNKNOWN")
			if end.Exited {
				reason = "EXITED"
			}
			return commandFrame{exit: &ExitStatus{Code: end.Code, Exited: end.Exited, Reason: reason, Message: end.Status}}, nil
		default:
			return commandFrame{}, codeError(Protocol, "Command.events", "COMMAND_EVENT_INVALID")
		}
	}
}

func (d *runtimeDataPlane) ConnectCommand(ctx context.Context, pid uint32, opts ConnectCommandOptions) (out *CommandHandle, err error) {
	ctx, finish, started := d.requestOperation(ctx, "Commands.Connect")
	defer func() {
		if out == nil {
			err = operationError(ctx, "Commands.Connect", err)
			finish()
		}
	}()
	user := string(opts.User)
	stream, err := d.wire.ConnectProcess(ctx, pid, user)
	if err != nil {
		return nil, err
	}
	started()
	return newCommandHandle(pid, &nativeCommandTransport{plane: d, ctx: ctx, finish: finish, pid: pid, user: user, stream: stream}, opts.MaxOutputBytes), nil
}

func (d *runtimeDataPlane) ListCommands(ctx context.Context, user SandboxUser) (out []ProcessInfo, err error) {
	ctx, finish, _ := d.requestOperation(ctx, "Commands.List")
	defer finish()
	defer func() { err = operationError(ctx, "Commands.List", err) }()
	processes, err := d.wire.ListProcesses(ctx, string(user))
	if err != nil {
		return nil, err
	}
	for _, item := range processes {
		out = append(out, ProcessInfo{PID: item.PID, Tag: item.Tag, Cmd: item.Command, Args: item.Args, Env: item.Env, CWD: item.CWD})
	}
	return out, nil
}
func (t *nativeCommandTransport) controlContext(ctx context.Context) (context.Context, func()) {
	child, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(t.ctx, func() { cancel(context.Cause(t.ctx)) })
	if cause := context.Cause(t.ctx); cause != nil {
		cancel(cause)
	}
	return child, func() { stop(); cancel(context.Canceled) }
}
func (t *nativeCommandTransport) input(ctx context.Context, data []byte) (err error) {
	ctx, finish := t.controlContext(ctx)
	defer finish()
	defer func() { err = operationError(ctx, "Command.Write", err) }()
	return t.plane.wire.SendProcessInput(ctx, t.pid, data, t.user)
}
func (t *nativeCommandTransport) signal(ctx context.Context, signal ProcessSignal) (err error) {
	ctx, finish := t.controlContext(ctx)
	defer finish()
	defer func() { err = operationError(ctx, "Command.Signal", err) }()
	value := dataplane.SignalTERM
	if signal == SignalKILL {
		value = dataplane.SignalKILL
	}
	return t.plane.wire.SendProcessSignal(ctx, t.pid, value, t.user)
}
