package ags

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"io"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	legacyfsproto "github.com/TencentCloudAgentRuntime/ags-go-sdk/pb/filesystem"
	legacyprocess "github.com/TencentCloudAgentRuntime/ags-go-sdk/pb/process"
	legacyprocessconnect "github.com/TencentCloudAgentRuntime/ags-go-sdk/pb/process/processconnect"
)

func dataPlaneRequest[T any](msg *T, token, user string) *connect.Request[T] {
	req := connect.NewRequest(msg)
	req.Header().Set("X-Access-Token", token)
	if user == "" {
		user = "user"
	}
	req.Header().Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(user+":")))
	return req
}

func (d *legacyDataPlane) Watch(ctx context.Context, root string, opts WatchOptions) (out watchStream, err error) {
	streamCtx, cancel, started := d.requestOperation(ctx, "Files.Watch")
	defer func() {
		if err != nil {
			if cause := context.Cause(d.lifetime); cause != nil {
				err = cause
			}
			err = normalizeError("Files.Watch", err)
		}
	}()
	stream, err := d.files.WatchDir(streamCtx, dataPlaneRequest(&legacyfsproto.WatchDirRequest{Path: root, Recursive: opts.Recursive}, d.config.AccessToken, dataPlaneUser(string(opts.User))))
	if err != nil {
		cancel()
		return nil, err
	}
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		cancel()
		return nil, err
	}
	watch := &legacyWatchStream{ctx: streamCtx, cancel: cancel, root: root, watchID: hex.EncodeToString(random), opts: opts, stream: stream, files: d.files, token: d.config.AccessToken}
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

type legacyWatchStream struct {
	ctx     context.Context
	cancel  context.CancelFunc
	root    string
	watchID string
	opts    WatchOptions
	stream  *connect.ServerStreamForClient[legacyfsproto.WatchDirResponse]
	files   interface {
		Stat(context.Context, *connect.Request[legacyfsproto.StatRequest]) (*connect.Response[legacyfsproto.StatResponse], error)
	}
	token    string
	sequence uint64
	once     sync.Once
	paused   atomic.Bool
	release  func()
}

func (w *legacyWatchStream) Recv() (FileEvent, error) {
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
			info, err := w.files.Stat(w.ctx, dataPlaneRequest(&legacyfsproto.StatRequest{Path: eventPath}, w.token, dataPlaneUser(string(w.opts.User))))
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
func (w *legacyWatchStream) Close() error {
	return w.closeStream()
}
func (w *legacyWatchStream) invalidate() error {
	w.paused.Store(true)
	return w.closeStream()
}
func (w *legacyWatchStream) closeStream() error {
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
func fileEventType(v legacyfsproto.EventType) FileEventType {
	switch v {
	case legacyfsproto.EventType_EVENT_TYPE_CREATE:
		return FileCreate
	case legacyfsproto.EventType_EVENT_TYPE_WRITE:
		return FileWrite
	case legacyfsproto.EventType_EVENT_TYPE_REMOVE:
		return FileRemove
	case legacyfsproto.EventType_EVENT_TYPE_RENAME:
		return FileRename
	case legacyfsproto.EventType_EVENT_TYPE_CHMOD:
		return FileChmod
	default:
		return FileEventType("UNKNOWN")
	}
}

func (d *legacyDataPlane) OpenPTY(ctx context.Context, opts PTYOptions) (out ptyStream, err error) {
	streamCtx, cancel, started := d.requestOperation(ctx, "PTY.Open")
	defer func() {
		if err != nil {
			if cause := context.Cause(d.lifetime); cause != nil {
				err = cause
			}
			err = normalizeError("PTY.Open", err)
		}
	}()
	client := d.process
	process := &legacyprocess.ProcessConfig{Cmd: opts.Command, Args: opts.Args, Envs: opts.Env}
	if opts.Cwd != "" {
		process.Cwd = &opts.Cwd
	}
	req := dataPlaneRequest(&legacyprocess.StartRequest{Process: process, Pty: &legacyprocess.PTY{Size: &legacyprocess.PTY_Size{Cols: opts.Cols, Rows: opts.Rows}}}, d.config.AccessToken, dataPlaneUser(string(opts.User)))
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
	pty := &legacyPTYStream{ctx: streamCtx, cancel: cancel, token: d.config.AccessToken, user: dataPlaneUser(string(opts.User)), client: client, stream: stream, pid: start.GetPid(), sessionID: hex.EncodeToString(random), startPending: true}
	pty.release = func() { d.unregister(pty) }
	if err := d.register(pty); err != nil {
		_ = pty.invalidate()
		return nil, err
	}
	started()
	return pty, nil
}

type legacyPTYStream struct {
	ctx          context.Context
	cancel       context.CancelFunc
	token, user  string
	client       legacyprocessconnect.ProcessClient
	stream       *connect.ServerStreamForClient[legacyprocess.StartResponse]
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

func (p *legacyPTYStream) selector() *legacyprocess.ProcessSelector {
	return &legacyprocess.ProcessSelector{Selector: &legacyprocess.ProcessSelector_Pid{Pid: p.pid}}
}
func (p *legacyPTYStream) Recv() (PTYEvent, error) {
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
func (p *legacyPTYStream) ID() string { return p.sessionID }
func (p *legacyPTYStream) Input(ctx context.Context, data []byte) error {
	ctx, finish, err := p.operation(ctx, "PTY.Write")
	if err != nil {
		return err
	}
	defer finish()
	_, err = p.client.SendInput(ctx, dataPlaneRequest(&legacyprocess.SendInputRequest{Process: p.selector(), Input: &legacyprocess.ProcessInput{Input: &legacyprocess.ProcessInput_Pty{Pty: data}}}, p.token, p.user))
	return operationError(ctx, "PTY.Write", err)
}
func (p *legacyPTYStream) Resize(ctx context.Context, cols, rows uint32) error {
	ctx, finish, err := p.operation(ctx, "PTY.Resize")
	if err != nil {
		return err
	}
	defer finish()
	_, err = p.client.Update(ctx, dataPlaneRequest(&legacyprocess.UpdateRequest{Process: p.selector(), Pty: &legacyprocess.PTY{Size: &legacyprocess.PTY_Size{Cols: cols, Rows: rows}}}, p.token, p.user))
	return operationError(ctx, "PTY.Resize", err)
}
func (p *legacyPTYStream) operation(ctx context.Context, op string) (context.Context, func(), error) {
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
func (p *legacyPTYStream) Close() error {
	var result error
	p.once.Do(func() {
		if p.release != nil {
			defer p.release()
		}
		p.mu.Lock()
		if !p.ended {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, result = p.client.SendSignal(ctx, dataPlaneRequest(
				&legacyprocess.SendSignalRequest{Process: p.selector(), Signal: legacyprocess.Signal_SIGNAL_SIGTERM},
				p.token,
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
func (p *legacyPTYStream) invalidate() error {
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
