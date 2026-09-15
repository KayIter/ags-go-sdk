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

	"connectrpc.com/connect"
	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/dataplane"
	fsproto "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/gen/filesystem"
	processproto "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/gen/process"
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
	stream, err := d.wire.Filesystem().WatchDir(streamCtx, dataplane.Request(d.wire, &fsproto.WatchDirRequest{Path: root, Recursive: opts.Recursive}, string(opts.User)))
	if err != nil {
		cancel()
		return nil, err
	}
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		cancel()
		return nil, err
	}
	watch := &runtimeWatchStream{ctx: streamCtx, cancel: cancel, root: root, watchID: hex.EncodeToString(random), opts: opts, stream: stream, wire: d.wire}
	watch.release = func() { d.unregister(watch) }
	for stream.Receive() {
		msg := stream.Msg()
		if msg != nil && msg.GetStart() != nil {
			if err := d.register(watch); err != nil {
				_ = watch.invalidate()
				return nil, err
			}
			started()
			return watch, nil
		}
	}
	cancel()
	if err := stream.Err(); err != nil {
		return nil, err
	}
	return nil, codeError(Protocol, "Files.Watch", "START_EVENT_MISSING")
}

type runtimeWatchStream struct {
	ctx      context.Context
	cancel   context.CancelFunc
	root     string
	watchID  string
	opts     WatchOptions
	stream   *connect.ServerStreamForClient[fsproto.WatchDirResponse]
	wire     *dataplane.Client
	sequence uint64
	once     sync.Once
	paused   atomic.Bool
	release  func()
}

func (w *runtimeWatchStream) Recv() (FileEvent, error) {
	for w.stream.Receive() {
		msg := w.stream.Msg()
		if msg == nil || msg.GetKeepalive() != nil || msg.GetStart() != nil {
			continue
		}
		event := msg.GetFilesystem()
		if event == nil {
			continue
		}
		w.sequence++
		name := event.GetName()
		eventPath := name
		if !path.IsAbs(name) {
			eventPath = path.Join(w.root, name)
		}
		out := FileEvent{WatchID: w.watchID, Type: fileEventType(event.GetType()), Path: eventPath, Sequence: w.sequence}
		if w.opts.IncludeEntry && out.Type != FileRemove {
			info, err := w.wire.Filesystem().Stat(w.ctx, dataplane.Request(w.wire, &fsproto.StatRequest{Path: eventPath}, string(w.opts.User)))
			if err == nil && info != nil && info.Msg.GetEntry() != nil {
				mapped := mapProtoFileInfo(info.Msg.GetEntry())
				out.Entry = &mapped
			}
		}
		return out, nil
	}
	defer w.closeStream()
	if err := w.stream.Err(); err != nil {
		if w.paused.Load() {
			return FileEvent{}, codeError(InstancePaused, "Files.Watch", "INSTANCE_PAUSED")
		}
		return FileEvent{}, operationError(w.ctx, "Files.Watch", err)
	}
	return FileEvent{}, io.EOF
}
func (w *runtimeWatchStream) Close() error {
	return w.closeStream()
}
func (w *runtimeWatchStream) invalidate() error {
	w.paused.Store(true)
	return w.closeStream()
}
func (w *runtimeWatchStream) closeStream() error {
	var err error
	w.once.Do(func() {
		w.cancel()
		err = w.stream.Close()
		if w.release != nil {
			w.release()
		}
	})
	return err
}
func fileEventType(v fsproto.EventType) FileEventType {
	switch v {
	case fsproto.EventType_EVENT_TYPE_CREATE:
		return FileCreate
	case fsproto.EventType_EVENT_TYPE_WRITE:
		return FileWrite
	case fsproto.EventType_EVENT_TYPE_REMOVE:
		return FileRemove
	case fsproto.EventType_EVENT_TYPE_RENAME:
		return FileRename
	case fsproto.EventType_EVENT_TYPE_CHMOD:
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
	client := d.wire.Process()
	process := &processproto.ProcessConfig{Cmd: opts.Command, Args: opts.Args, Envs: opts.Env}
	if opts.Cwd != "" {
		process.Cwd = &opts.Cwd
	}
	req := dataplane.Request(d.wire, &processproto.StartRequest{Process: process, Pty: &processproto.PTY{Size: &processproto.PTY_Size{Cols: opts.Cols, Rows: opts.Rows}}}, string(opts.User))
	stream, err := client.Start(streamCtx, req)
	if err != nil {
		cancel()
		return nil, err
	}
	if !stream.Receive() {
		cancel()
		if err := stream.Err(); err != nil {
			return nil, err
		}
		return nil, codeError(Protocol, "PTY.Open", "START_EVENT_MISSING")
	}
	start := stream.Msg().GetEvent().GetStart()
	if start == nil {
		cancel()
		return nil, codeError(Protocol, "PTY.Open", "START_EVENT_MISSING")
	}
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		cancel()
		return nil, err
	}
	pty := &runtimePTYStream{ctx: streamCtx, cancel: cancel, wire: d.wire, user: dataplane.NormalizeUser(string(opts.User)), stream: stream, pid: start.GetPid(), sessionID: hex.EncodeToString(random), startPending: true}
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
	stream       *connect.ServerStreamForClient[processproto.StartResponse]
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

func (p *runtimePTYStream) selector() *processproto.ProcessSelector {
	return &processproto.ProcessSelector{Selector: &processproto.ProcessSelector_Pid{Pid: p.pid}}
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
	for p.stream.Receive() {
		msg := p.stream.Msg()
		if msg == nil || msg.Event == nil {
			continue
		}
		if data := msg.Event.GetData(); data != nil {
			if out := data.GetPty(); len(out) > 0 {
				return PTYEvent{Type: PTYOutput, Data: out}, nil
			}
			continue
		}
		if end := msg.Event.GetEnd(); end != nil {
			p.mu.Lock()
			p.ended = true
			idle := p.active == 0
			p.mu.Unlock()
			if idle {
				_ = p.Close()
			}
			exit := &ExitStatus{Code: int(end.GetExitCode()), Exited: end.GetExited(), Reason: ExitReason(end.GetStatus())}
			if end.Error != nil {
				exit.Message = end.GetError()
			}
			return PTYEvent{Type: PTYEnd, Exit: exit}, nil
		}
	}
	if err := p.stream.Err(); err != nil {
		if p.paused.Load() {
			return PTYEvent{}, codeError(InstancePaused, "PTY.Events", "INSTANCE_PAUSED")
		}
		return PTYEvent{}, operationError(p.ctx, "PTY.Events", err)
	}
	return PTYEvent{}, io.EOF
}
func (p *runtimePTYStream) ID() string { return p.sessionID }
func (p *runtimePTYStream) Input(ctx context.Context, data []byte) error {
	ctx, finish, err := p.operation(ctx, "PTY.Write")
	if err != nil {
		return err
	}
	defer finish()
	_, err = p.wire.Process().SendInput(ctx, dataplane.Request(p.wire, &processproto.SendInputRequest{Process: p.selector(), Input: &processproto.ProcessInput{Input: &processproto.ProcessInput_Pty{Pty: data}}}, p.user))
	return operationError(ctx, "PTY.Write", err)
}
func (p *runtimePTYStream) Resize(ctx context.Context, cols, rows uint32) error {
	ctx, finish, err := p.operation(ctx, "PTY.Resize")
	if err != nil {
		return err
	}
	defer finish()
	_, err = p.wire.Process().Update(ctx, dataplane.Request(p.wire, &processproto.UpdateRequest{Process: p.selector(), Pty: &processproto.PTY{Size: &processproto.PTY_Size{Cols: cols, Rows: rows}}}, p.user))
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
			_, result = p.wire.Process().SendSignal(ctx, dataplane.Request(p.wire,
				&processproto.SendSignalRequest{Process: p.selector(), Signal: processproto.Signal_SIGNAL_SIGTERM},
				p.user,
			))
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
		result = p.stream.Close()
	})
	return result
}
