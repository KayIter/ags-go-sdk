package ags

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/dataplane"
)

func (d *runtimeDataPlane) Watch(ctx context.Context, root string, opts WatchOptions) (out watchStream, err error) {
	streamCtx, cancel, started := d.requestOperation(ctx, "Files.Watch")
	defer func() {
		if err != nil {
			if cause := context.Cause(d.lifetime); cause != nil {
				err = cause
			}
			err = normalizeError("Files.Watch", err)
		}
	}()
	stream, err := d.wire.StartWatch(streamCtx, root, opts.Recursive, opts.IncludeEntry, string(opts.User))
	if err != nil {
		cancel()
		return nil, err
	}
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		cancel()
		return nil, err
	}
	watch := &runtimeWatchStream{ctx: streamCtx, cancel: cancel, root: root, watchID: hex.EncodeToString(random), opts: opts, stream: stream}
	watch.release = func() { d.unregister(watch) }
	if err := d.register(watch); err != nil {
		_ = watch.invalidate()
		return nil, err
	}
	started()
	return watch, nil
}

type runtimeWatchStream struct {
	ctx      context.Context
	cancel   context.CancelFunc
	root     string
	watchID  string
	opts     WatchOptions
	stream   *dataplane.WatchStream
	sequence uint64
	once     sync.Once
	paused   atomic.Bool
	release  func()
}

func (w *runtimeWatchStream) Recv() (FileEvent, error) {
	for {
		event, err := w.stream.Recv(w.ctx)
		if err != nil {
			defer w.closeStream(false)
			if err == io.EOF {
				return FileEvent{}, io.EOF
			}
			if w.paused.Load() {
				return FileEvent{}, codeError(InstancePaused, "Files.Watch", "INSTANCE_PAUSED")
			}
			return FileEvent{}, operationError(w.ctx, "Files.Watch", err)
		}
		w.sequence++
		name := event.Name
		eventPath := name
		if !path.IsAbs(name) {
			eventPath = path.Join(w.root, name)
		}
		out := FileEvent{WatchID: w.watchID, Type: fileEventType(event.Type), Path: eventPath, Sequence: w.sequence}
		if event.Entry != nil {
			mapped := mapDataPlaneFileInfo(*event.Entry)
			out.Entry = &mapped
		}
		return out, nil
	}
}
func (w *runtimeWatchStream) Close() error {
	return w.closeStream(false)
}
func (w *runtimeWatchStream) invalidate() error {
	w.paused.Store(true)
	return w.closeStream(true)
}
func (w *runtimeWatchStream) closeStream(invalidate bool) error {
	var err error
	w.once.Do(func() {
		w.cancel()
		if invalidate {
			err = w.stream.Invalidate()
		} else {
			err = w.stream.Close()
		}
		if w.release != nil {
			w.release()
		}
	})
	return err
}
func fileEventType(v dataplane.WatchEventType) FileEventType {
	switch v {
	case dataplane.WatchCreate:
		return FileCreate
	case dataplane.WatchWrite:
		return FileWrite
	case dataplane.WatchRemove:
		return FileRemove
	case dataplane.WatchRename:
		return FileRename
	case dataplane.WatchChmod:
		return FileChmod
	default:
		return FileEventType("UNKNOWN")
	}
}

func (d *runtimeDataPlane) OpenPTY(ctx context.Context, opts PTYOptions) (out ptyStream, err error) {
	streamCtx, cancel, started := d.requestOperation(ctx, "PTY.Open")
	defer func() {
		if err != nil {
			if cause := context.Cause(d.lifetime); cause != nil {
				err = cause
			}
			err = normalizeError("PTY.Open", err)
		}
	}()
	stream, err := d.wire.StartPTY(streamCtx, dataplane.PTYConfig{Command: opts.Command, Args: opts.Args, Env: opts.Env, CWD: opts.Cwd, Size: dataplane.PTYSize{Cols: opts.Cols, Rows: opts.Rows}}, string(opts.User))
	if err != nil {
		cancel()
		return nil, err
	}
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		cancel()
		return nil, err
	}
	pty := &runtimePTYStream{ctx: streamCtx, cancel: cancel, wire: d.wire, user: string(opts.User), stream: stream, pid: stream.PID, sessionID: hex.EncodeToString(random), startPending: true}
	pty.release = func() { d.unregister(pty) }
	if err := d.register(pty); err != nil {
		_ = pty.invalidate()
		return nil, err
	}
	started()
	return pty, nil
}

type runtimePTYStream struct {
	ctx          context.Context
	cancel       context.CancelFunc
	wire         *dataplane.Client
	user         string
	stream       *dataplane.ProcessStream
	pid          uint32
	sessionID    string
	mu           sync.Mutex
	once         sync.Once
	ended        bool
	active       int // Input/Resize acknowledgments may arrive after natural End.
	startPending bool
	paused       atomic.Bool
	release      func()
}

func (p *runtimePTYStream) Recv() (PTYEvent, error) {
	p.mu.Lock()
	if p.startPending {
		p.startPending = false
		out := PTYEvent{Type: PTYStart}
		p.mu.Unlock()
		return out, nil
	}
	p.mu.Unlock()
	for {
		event, err := p.stream.Recv()
		if err != nil {
			if err == io.EOF {
				return PTYEvent{}, io.EOF
			}
			if p.paused.Load() {
				return PTYEvent{}, codeError(InstancePaused, "PTY.Events", "INSTANCE_PAUSED")
			}
			return PTYEvent{}, operationError(p.ctx, "PTY.Events", err)
		}
		if event.Kind == dataplane.ProcessPTY {
			if len(event.Data) > 0 {
				return PTYEvent{Type: PTYOutput, Data: event.Data}, nil
			}
			continue
		}
		if event.Kind == dataplane.ProcessEnd {
			p.mu.Lock()
			p.ended = true
			idle := p.active == 0
			p.mu.Unlock()
			if idle {
				_ = p.Close()
			}
			exit := &ExitStatus{Code: event.Exit.Code, Exited: event.Exit.Exited, Reason: ExitReason(event.Exit.Status), Message: event.Exit.Message}
			return PTYEvent{Type: PTYEnd, Exit: exit}, nil
		}
	}
}
func (p *runtimePTYStream) ID() string { return p.sessionID }
func (p *runtimePTYStream) Input(ctx context.Context, data []byte) error {
	ctx, finish, err := p.operation(ctx, "PTY.Write")
	if err != nil {
		return err
	}
	defer finish()
	err = p.wire.SendPTYInput(ctx, p.pid, data, p.user)
	return operationError(ctx, "PTY.Write", err)
}
func (p *runtimePTYStream) Resize(ctx context.Context, cols, rows uint32) error {
	ctx, finish, err := p.operation(ctx, "PTY.Resize")
	if err != nil {
		return err
	}
	defer finish()
	err = p.wire.ResizePTY(ctx, p.pid, dataplane.PTYSize{Cols: cols, Rows: rows}, p.user)
	return operationError(ctx, "PTY.Resize", err)
}
func (p *runtimePTYStream) operation(ctx context.Context, op string) (context.Context, func(), error) {
	p.mu.Lock()
	if p.paused.Load() {
		p.mu.Unlock()
		return nil, nil, codeError(InstancePaused, op, "INSTANCE_PAUSED")
	}
	if p.ended {
		p.mu.Unlock()
		return nil, nil, codeError(Conflict, op, "SESSION_ENDED")
	}
	p.active++
	p.mu.Unlock()
	child, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(p.ctx, func() { cancel(context.Cause(p.ctx)) })
	if cause := context.Cause(p.ctx); cause != nil {
		cancel(cause)
	}
	return child, func() {
		stop()
		cancel(context.Canceled)
		p.mu.Lock()
		p.active--
		finish := p.ended && p.active == 0
		p.mu.Unlock()
		if finish {
			_ = p.Close()
		}
	}, nil
}
func (p *runtimePTYStream) Close() error {
	var result error
	p.once.Do(func() {
		if p.release != nil {
			defer p.release()
		}
		p.mu.Lock()
		if !p.ended {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			result = p.wire.SendProcessSignal(ctx, p.pid, dataplane.SignalTERM, p.user)
		}
		p.ended = true
		p.mu.Unlock()
		p.cancel()
		if err := p.stream.Close(); result == nil {
			result = err
		}
	})
	return result
}

// invalidate terminates the local handle without signalling the remote process.
// Pause/Resume uses this path because the old access token and stream are stale.
func (p *runtimePTYStream) invalidate() error {
	p.paused.Store(true)
	var result error
	p.once.Do(func() {
		if p.release != nil {
			defer p.release()
		}
		p.mu.Lock()
		p.ended = true
		p.mu.Unlock()
		p.cancel()
		result = p.stream.Invalidate()
	})
	return result
}
