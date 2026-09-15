package ags

import (
	"connectrpc.com/connect"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"sync"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/dataplane"
	process "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/gen/process"
	rpc "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/gen/process/processconnect"
)

// Bound wire frames independently from retained stdout/stderr and event queues.
const commandFrameLimit = 2 << 20

// Connect's own read limit drains oversized wire frames before reporting the
// error. Inspect their length header first; retain its decompressed-size guard.
type commandHTTP struct{ client *http.Client }

func (c commandHTTP) Do(req *http.Request) (*http.Response, error) {
	response, err := c.client.Do(req)
	if err == nil && response.StatusCode/100 == 2 {
		response.Body = &commandFrameBody{ReadCloser: response.Body, headerOffset: 5}
	}
	return response, err
}

type commandFrameBody struct {
	io.ReadCloser
	header       [5]byte
	headerOffset int
	remaining    uint32
	failure      error
}

func (b *commandFrameBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if b.failure != nil {
		return 0, b.failure
	}
	if b.headerOffset == 5 && b.remaining == 0 {
		if _, err := io.ReadFull(b.ReadCloser, b.header[:]); err != nil {
			b.failure = err
			return 0, err
		}
		b.remaining = binary.BigEndian.Uint32(b.header[1:])
		if b.remaining > commandFrameLimit {
			b.failure = connect.NewError(connect.CodeResourceExhausted, errors.New("COMMAND_FRAME_TOO_LARGE"))
			_ = b.ReadCloser.Close()
			return 0, b.failure
		}
		b.headerOffset = 0
	}
	if b.headerOffset < 5 {
		n := copy(p, b.header[b.headerOffset:])
		b.headerOffset += n
		return n, nil
	}
	if len(p) > int(b.remaining) {
		p = p[:b.remaining]
	}
	n, err := b.ReadCloser.Read(p)
	b.remaining -= uint32(n)
	if err == io.EOF && b.remaining > 0 {
		err = io.ErrUnexpectedEOF
	}
	return n, err
}

type nativeCommandTransport struct {
	plane        *runtimeDataPlane
	ctx          context.Context
	finish       func()
	once         sync.Once
	receiveEvent func() (*process.ProcessEvent, bool, error)
	closeStream  func() error
	pid          uint32
	user         string
}

func (d *runtimeDataPlane) Start(ctx context.Context, command string, opts StartOptions) (out *CommandHandle, err error) {
	ctx, finish, started := d.requestOperation(ctx, "Commands.Start")
	defer func() {
		if out == nil {
			err = operationError(ctx, "Commands.Start", err)
			finish()
		}
	}()
	cfg := &process.ProcessConfig{Cmd: command, Args: opts.Args, Envs: opts.Env}
	if opts.Cwd != "" {
		cfg.Cwd = &opts.Cwd
	}
	client := rpc.NewProcessClient(commandHTTP{d.wire.HTTPClient()}, d.wire.BaseURL(), connect.WithProtoJSON(), connect.WithReadMaxBytes(commandFrameLimit))
	user := dataplane.NormalizeUser(string(opts.User))
	stream, err := client.Start(ctx, dataplane.Request(d.wire, &process.StartRequest{Process: cfg}, user))
	if err != nil {
		return nil, err
	}
	for stream.Receive() {
		event := stream.Msg().GetEvent()
		if event.GetKeepalive() != nil {
			continue
		}
		pid := event.GetStart().GetPid()
		if pid == 0 {
			_ = stream.Close()
			return nil, codeError(Protocol, "Commands.Start", "START_EVENT_MISSING")
		}
		started()
		return newCommandHandle(pid, &nativeCommandTransport{plane: d, ctx: ctx, finish: finish, pid: pid, user: user,
			receiveEvent: func() (*process.ProcessEvent, bool, error) {
				if stream.Receive() {
					return stream.Msg().GetEvent(), true, nil
				}
				return nil, false, stream.Err()
			}, closeStream: stream.Close}, opts.MaxOutputBytes), nil
	}
	err = stream.Err()
	_ = stream.Close()
	if err == nil {
		err = codeError(Protocol, "Commands.Start", "START_EVENT_MISSING")
	}
	return nil, err
}
func (t *nativeCommandTransport) currentError() error { return operationError(t.ctx, "Command", nil) }
func (t *nativeCommandTransport) close() {
	t.once.Do(func() {
		t.finish()
		if t.closeStream != nil {
			_ = t.closeStream()
		}
	})
}
func (t *nativeCommandTransport) receive() (commandFrame, error) {
	for {
		event, ok, err := t.receiveEvent()
		if !ok {
			if err == nil {
				err = codeError(Protocol, "Command.events", "END_EVENT_MISSING")
			}
			return commandFrame{}, operationError(t.ctx, "Command.events", err)
		}
		if err := t.currentError(); err != nil {
			return commandFrame{}, err
		}
		if data := event.GetData(); data != nil {
			switch value := data.Output.(type) {
			case *process.ProcessEvent_DataEvent_Stdout:
				return commandFrame{kind: CommandStdout, data: value.Stdout}, nil
			case *process.ProcessEvent_DataEvent_Stderr:
				return commandFrame{kind: CommandStderr, data: value.Stderr}, nil
			default:
				return commandFrame{}, codeError(Protocol, "Command.events", "COMMAND_OUTPUT_INVALID")
			}
		}
		if end := event.GetEnd(); end != nil {
			reason := ExitReason("UNKNOWN")
			if end.GetExited() {
				reason = "EXITED"
			}
			return commandFrame{exit: &ExitStatus{Code: int(end.GetExitCode()), Exited: end.GetExited(), Reason: reason, Message: end.GetStatus()}}, nil
		}
		if event.GetKeepalive() == nil {
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
	client := rpc.NewProcessClient(commandHTTP{d.wire.HTTPClient()}, d.wire.BaseURL(), connect.WithProtoJSON(), connect.WithReadMaxBytes(commandFrameLimit))
	user := dataplane.NormalizeUser(string(opts.User))
	stream, err := client.Connect(ctx, dataplane.Request(d.wire, &process.ConnectRequest{Process: &process.ProcessSelector{Selector: &process.ProcessSelector_Pid{Pid: pid}}}, user))
	if err != nil {
		return nil, err
	}
	for stream.Receive() {
		event := stream.Msg().GetEvent()
		if event.GetKeepalive() != nil {
			continue
		}
		observed := event.GetStart().GetPid()
		if observed == 0 || observed != pid {
			_ = stream.Close()
			return nil, codeError(Protocol, "Commands.Connect", "START_EVENT_MISMATCH")
		}
		started()
		return newCommandHandle(pid, &nativeCommandTransport{plane: d, ctx: ctx, finish: finish, pid: pid, user: user,
			receiveEvent: func() (*process.ProcessEvent, bool, error) {
				if stream.Receive() {
					return stream.Msg().GetEvent(), true, nil
				}
				return nil, false, stream.Err()
			}, closeStream: stream.Close}, opts.MaxOutputBytes), nil
	}
	err = stream.Err()
	_ = stream.Close()
	if err == nil {
		err = codeError(Protocol, "Commands.Connect", "START_EVENT_MISSING")
	}
	return nil, err
}

func (d *runtimeDataPlane) ListCommands(ctx context.Context, user SandboxUser) (out []ProcessInfo, err error) {
	ctx, finish, _ := d.requestOperation(ctx, "Commands.List")
	defer finish()
	defer func() { err = operationError(ctx, "Commands.List", err) }()
	response, err := d.wire.Process().List(ctx, dataplane.Request(d.wire, &process.ListRequest{}, string(user)))
	if err != nil {
		return nil, err
	}
	if response == nil || response.Msg == nil {
		return nil, codeError(Protocol, "Commands.List", "COMMAND_LIST_RESPONSE_MISSING")
	}
	for _, item := range response.Msg.GetProcesses() {
		if item == nil || item.GetPid() == 0 || item.GetConfig() == nil {
			return nil, codeError(Protocol, "Commands.List", "COMMAND_INFO_INVALID")
		}
		config := item.GetConfig()
		mapped := ProcessInfo{PID: item.GetPid(), Cmd: config.GetCmd(), Args: append([]string(nil), config.GetArgs()...), Env: cloneStringMap(config.GetEnvs())}
		if item.Tag != nil {
			value := item.GetTag()
			mapped.Tag = &value
		}
		if config.Cwd != nil {
			value := config.GetCwd()
			mapped.CWD = &value
		}
		out = append(out, mapped)
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
func (t *nativeCommandTransport) selector() *process.ProcessSelector {
	return &process.ProcessSelector{Selector: &process.ProcessSelector_Pid{Pid: t.pid}}
}
func (t *nativeCommandTransport) input(ctx context.Context, data []byte) (err error) {
	ctx, finish := t.controlContext(ctx)
	defer finish()
	defer func() { err = operationError(ctx, "Command.Write", err) }()
	_, err = t.plane.wire.Process().SendInput(ctx, dataplane.Request(t.plane.wire, &process.SendInputRequest{Process: t.selector(), Input: &process.ProcessInput{Input: &process.ProcessInput_Stdin{Stdin: data}}}, t.user))
	return err
}
func (t *nativeCommandTransport) signal(ctx context.Context, signal ProcessSignal) (err error) {
	ctx, finish := t.controlContext(ctx)
	defer finish()
	defer func() { err = operationError(ctx, "Command.Signal", err) }()
	value := process.Signal_SIGNAL_SIGTERM
	if signal == SignalKILL {
		value = process.Signal_SIGNAL_SIGKILL
	}
	_, err = t.plane.wire.Process().SendSignal(ctx, dataplane.Request(t.plane.wire, &process.SendSignalRequest{Process: t.selector(), Signal: value}, t.user))
	return err
}
