package runtime

import (
	"bytes"
	"context"
	"io"
	"sync"
	"time"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/dataplane"
	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/model"
)

const defaultMaxOutputBytes int64 = 4 << 20

// CommandMapper converts private values at the facade boundary without a
// reverse package dependency or a second delivery goroutine.
type CommandMapper[E, R, S any] struct {
	Event  func(model.CommandEvent) E
	Result func(model.CommandResult) R
	Signal func(S) (model.ProcessSignal, bool)
	Error  func(error) error
}

type commandTransport interface {
	receive() (model.CommandEvent, error)
	input(context.Context, []byte) error
	signal(context.Context, model.ProcessSignal) error
	currentError() error
	close()
}

// CommandHandle owns process observation, aggregation, and bounded delivery.
type CommandHandle[E, R, S any] struct {
	pid                              uint32
	transport                        commandTransport
	mapper                           CommandMapper[E, R, S]
	mu                               sync.Mutex
	writeGate                        gate
	done                             chan struct{}
	terminal                         bool
	result                           model.CommandResult
	failure                          error
	limit                            int64
	stdout, stderr                   []byte
	stdoutTruncated, stderrTruncated bool
	observed                         bool
	events                           chan E
	queue                            []E
	queueBytes                       []int
	queuedBytes                      int
	wake                             chan struct{}
	deliveryStop                     chan struct{}
	stopOnce                         sync.Once
	exit                             *model.ExitStatus
}

func newCommandHandle[E, R, S any](pid uint32, transport commandTransport, limit int64, mapper CommandMapper[E, R, S]) *CommandHandle[E, R, S] {
	if limit == 0 {
		limit = defaultMaxOutputBytes
	}
	h := &CommandHandle[E, R, S]{pid: pid, transport: transport, mapper: mapper, limit: limit, done: make(chan struct{}), wake: make(chan struct{}, 1), deliveryStop: make(chan struct{})}
	go h.pump()
	return h
}

func StartCommand[E, R, S any](g *Generation, ctx context.Context, config model.ProcessConfig, mapper CommandMapper[E, R, S]) (out *CommandHandle[E, R, S], err error) {
	ctx, finish, started := g.requestOperation(ctx)
	defer func() {
		if out == nil {
			err = operationError(ctx, "Commands.Start", err)
			finish()
		}
	}()
	stream, err := g.wire.StartProcess(ctx, dataplane.ProcessConfig{Command: config.Command, Args: config.Args, Env: config.Env, CWD: config.CWD}, config.User)
	if err != nil {
		return nil, err
	}
	started()
	transport := &nativeCommandTransport{generation: g, ctx: ctx, finish: finish, stream: stream, pid: stream.PID, user: config.User}
	return newCommandHandle(stream.PID, transport, config.MaxOutputBytes, mapper), nil
}

func ConnectCommand[E, R, S any](g *Generation, ctx context.Context, pid uint32, user string, limit int64, mapper CommandMapper[E, R, S]) (out *CommandHandle[E, R, S], err error) {
	ctx, finish, started := g.requestOperation(ctx)
	defer func() {
		if out == nil {
			err = operationError(ctx, "Commands.Connect", err)
			finish()
		}
	}()
	stream, err := g.wire.ConnectProcess(ctx, pid, user)
	if err != nil {
		return nil, err
	}
	started()
	transport := &nativeCommandTransport{generation: g, ctx: ctx, finish: finish, stream: stream, pid: pid, user: user}
	return newCommandHandle(pid, transport, limit, mapper), nil
}

func (g *Generation) ListCommands(ctx context.Context, user string) (out []model.ProcessInfo, err error) {
	ctx, finish, _ := g.requestOperation(ctx)
	defer finish()
	defer func() { err = operationError(ctx, "Commands.List", err) }()
	items, err := g.wire.ListProcesses(ctx, user)
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		out = append(out, model.ProcessInfo{PID: item.PID, Tag: item.Tag, CWD: item.CWD, Command: item.Command, Args: item.Args, Env: item.Env})
	}
	return out, nil
}

func (g *Generation) Run(ctx context.Context, config model.ProcessConfig) (result model.CommandResult, err error) {
	ctx, finish, started := g.requestOperation(ctx)
	defer finish()
	defer func() { err = operationError(ctx, "Commands.Run", err) }()
	limit := config.MaxOutputBytes
	if limit == 0 {
		limit = defaultMaxOutputBytes
	}
	stdout, stderr := &limitedOutput{limit: limit}, &limitedOutput{limit: limit}
	stream, err := g.wire.StartProcess(ctx, dataplane.ProcessConfig{Command: config.Command, Args: config.Args, Env: config.Env, CWD: config.CWD}, config.User)
	if err != nil {
		return result, err
	}
	defer stream.Close()
	started()
	for {
		event, receiveErr := stream.Recv()
		if receiveErr != nil {
			if receiveErr == io.EOF {
				break
			}
			return result, receiveErr
		}
		switch event.Kind {
		case dataplane.ProcessStdout:
			stdout.write(event.Data)
		case dataplane.ProcessStderr:
			stderr.write(event.Data)
		case dataplane.ProcessEnd:
			return model.CommandResult{ExitCode: event.Exit.Code, Stdout: stdout.bytes(), Stderr: stderr.bytes(), StdoutTruncated: stdout.truncated, StderrTruncated: stderr.truncated}, nil
		}
	}
	if ctx.Err() != nil && context.Cause(g.lifetime) == nil {
		killCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = g.wire.SendProcessSignal(killCtx, stream.PID, dataplane.SignalKILL, config.User)
		return result, normalize("Commands.Run", ctx.Err())
	}
	return result, failure(model.Protocol, "Commands.Run", "END_EVENT_MISSING")
}

func (h *CommandHandle[E, R, S]) PID() uint32 { return h.pid }

func (h *CommandHandle[E, R, S]) Events() <-chan E {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.events != nil {
		return h.events
	}
	h.events = make(chan E)
	if h.terminal {
		close(h.events)
		return h.events
	}
	h.observed = true
	go h.dispatch()
	return h.events
}

func (h *CommandHandle[E, R, S]) notify() {
	select {
	case h.wake <- struct{}{}:
	default:
	}
}

func (h *CommandHandle[E, R, S]) finish(exit *model.ExitStatus, err error) {
	h.mu.Lock()
	if h.terminal {
		h.mu.Unlock()
		return
	}
	h.terminal, h.failure, h.exit = true, h.mapError(err), exit
	if err == nil && exit != nil {
		h.result = model.CommandResult{ExitCode: exit.Code, Stdout: h.stdout, Stderr: h.stderr, StdoutTruncated: h.stdoutTruncated, StderrTruncated: h.stderrTruncated}
	} else {
		h.queue, h.queueBytes, h.queuedBytes = nil, nil, 0
	}
	close(h.done)
	h.mu.Unlock()
	h.notify()
	h.transport.close()
}

func (h *CommandHandle[E, R, S]) pump() {
	for {
		event, err := h.transport.receive()
		if err != nil {
			h.finish(nil, err)
			return
		}
		if event.Exit != nil {
			h.finish(event.Exit, nil)
			return
		}
		if len(event.Data) > 0 && !h.output(event.Kind, event.Data) {
			return
		}
	}
}

func (h *CommandHandle[E, R, S]) output(kind model.CommandEventKind, data []byte) bool {
	h.mu.Lock()
	if h.terminal {
		h.mu.Unlock()
		return false
	}
	output, truncated := &h.stdout, &h.stdoutTruncated
	if kind == model.CommandStderr {
		output, truncated = &h.stderr, &h.stderrTruncated
	}
	remaining := h.limit - int64(len(*output))
	if remaining < 0 {
		remaining = 0
	}
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
			h.finish(nil, failure(model.ResourceExhausted, "Commands.events", "COMMAND_BUFFER_FULL"))
			return false
		}
		private := model.CommandEvent{Kind: kind, Data: append([]byte(nil), data[:n]...)}
		h.queue = append(h.queue, h.mapper.Event(private))
		h.queueBytes = append(h.queueBytes, n)
		h.queuedBytes += n
		data = data[n:]
	}
	h.mu.Unlock()
	h.notify()
	return true
}

func (h *CommandHandle[E, R, S]) dispatch() {
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
					h.queuedBytes -= h.queueBytes[0]
					var zero E
					h.queue[0] = zero
					h.queue = h.queue[1:]
					h.queueBytes = h.queueBytes[1:]
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
				select {
				case h.events <- h.mapper.Event(model.CommandEvent{Kind: model.CommandExit, Exit: exit}):
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

func (h *CommandHandle[E, R, S]) Wait(ctx context.Context) (R, error) {
	var zero R
	if err := ctx.Err(); err != nil {
		return zero, h.mapError(normalize("Command.Wait", err))
	}
	select {
	case <-h.done:
	case <-ctx.Done():
		return zero, h.mapError(normalize("Command.Wait", ctx.Err()))
	}
	h.mu.Lock()
	result, failure := h.result, h.failure
	h.mu.Unlock()
	result.Stdout = append([]byte(nil), result.Stdout...)
	result.Stderr = append([]byte(nil), result.Stderr...)
	return h.mapper.Result(result), failure
}

func (h *CommandHandle[E, R, S]) active() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.terminal {
		if h.failure != nil {
			return h.failure
		}
		return h.mapError(failure(model.Conflict, "Command", "COMMAND_ENDED"))
	}
	return h.mapError(h.transport.currentError())
}

func (h *CommandHandle[E, R, S]) Write(ctx context.Context, data []byte) error {
	data = append([]byte(nil), data...)
	if err := h.writeGate.acquire(ctx); err != nil {
		return h.mapError(normalize("Command.Write", err))
	}
	defer h.writeGate.release()
	if err := h.active(); err != nil {
		return err
	}
	return h.mapError(h.transport.input(ctx, data))
}

func (h *CommandHandle[E, R, S]) Signal(ctx context.Context, signal S) error {
	value, ok := h.mapper.Signal(signal)
	if !ok {
		return h.mapError(failure(model.InvalidArgument, "Command.Signal", "SIGNAL_INVALID"))
	}
	if err := h.active(); err != nil {
		return err
	}
	return h.mapError(h.transport.signal(ctx, value))
}

func (h *CommandHandle[E, R, S]) Err() error { h.mu.Lock(); defer h.mu.Unlock(); return h.failure }
func (h *CommandHandle[E, R, S]) Close() error {
	h.finish(nil, failure(model.Canceled, "Command.Close", "COMMAND_HANDLE_CLOSED"))
	h.stopOnce.Do(func() { close(h.deliveryStop) })
	return nil
}
func (h *CommandHandle[E, R, S]) mapError(err error) error {
	if err == nil || h.mapper.Error == nil {
		return err
	}
	return h.mapper.Error(err)
}

type nativeCommandTransport struct {
	generation *Generation
	ctx        context.Context
	finish     func()
	once       sync.Once
	stream     *dataplane.ProcessStream
	pid        uint32
	user       string
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
func (t *nativeCommandTransport) receive() (model.CommandEvent, error) {
	for {
		event, err := t.stream.Recv()
		if err != nil {
			if err == io.EOF {
				err = failure(model.Protocol, "Command.events", "END_EVENT_MISSING")
			}
			return model.CommandEvent{}, operationError(t.ctx, "Command.events", err)
		}
		if err = t.currentError(); err != nil {
			return model.CommandEvent{}, err
		}
		switch event.Kind {
		case dataplane.ProcessStdout:
			return model.CommandEvent{Kind: model.CommandStdout, Data: event.Data}, nil
		case dataplane.ProcessStderr:
			return model.CommandEvent{Kind: model.CommandStderr, Data: event.Data}, nil
		case dataplane.ProcessPTY:
			return model.CommandEvent{}, failure(model.Protocol, "Command.events", "COMMAND_OUTPUT_INVALID")
		case dataplane.ProcessEnd:
			reason := "UNKNOWN"
			if event.Exit.Exited {
				reason = "EXITED"
			}
			return model.CommandEvent{Kind: model.CommandExit, Exit: &model.ExitStatus{Code: event.Exit.Code, Exited: event.Exit.Exited, Reason: reason, Message: event.Exit.Status}}, nil
		default:
			return model.CommandEvent{}, failure(model.Protocol, "Command.events", "COMMAND_EVENT_INVALID")
		}
	}
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
	return t.generation.wire.SendProcessInput(ctx, t.pid, data, t.user)
}
func (t *nativeCommandTransport) signal(ctx context.Context, signal model.ProcessSignal) (err error) {
	ctx, finish := t.controlContext(ctx)
	defer finish()
	defer func() { err = operationError(ctx, "Command.Signal", err) }()
	value := dataplane.SignalTERM
	if signal == model.SignalKILL {
		value = dataplane.SignalKILL
	}
	return t.generation.wire.SendProcessSignal(ctx, t.pid, value, t.user)
}

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
