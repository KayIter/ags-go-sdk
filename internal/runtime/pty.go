package runtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/dataplane"
	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/model"
)

const ptyBuffer = 32

// PTYMapper converts private values at the facade boundary without a second pump.
type PTYMapper[E, X any] struct {
	Event func(model.PTYEvent) E
	Exit  func(model.ExitStatus) X
	Error func(error) error
}

// PTYSession owns terminal transport, bounded delivery, and generation invalidation.
type PTYSession[E, X any] struct {
	ctx      context.Context
	cancel   context.CancelFunc
	wire     *dataplane.Client
	stream   *dataplane.ProcessStream
	pid      uint32
	user, id string
	events   chan E
	done     chan struct{}
	mu       sync.Mutex
	once     sync.Once
	ended    bool
	active   int
	paused   atomic.Bool
	exit     model.ExitStatus
	err      error
	release  func()
	mapper   PTYMapper[E, X]
}

func OpenPTY[E, X any](g *Generation, ctx context.Context, options model.PTYConfig, mapper PTYMapper[E, X]) (out *PTYSession[E, X], err error) {
	streamCtx, cancel, started := g.requestOperation(ctx)
	defer func() {
		if err != nil {
			if cause := context.Cause(g.lifetime); cause != nil {
				err = cause
			}
			err = normalize("PTY.Open", err)
		}
	}()
	stream, err := g.wire.StartPTY(streamCtx, dataplane.PTYConfig{Command: options.Command, Args: options.Args, Env: options.Env, CWD: options.CWD, Size: dataplane.PTYSize{Cols: options.Cols, Rows: options.Rows}}, options.User)
	if err != nil {
		cancel()
		return nil, err
	}
	random := make([]byte, 16)
	if _, err = rand.Read(random); err != nil {
		cancel()
		_ = stream.Close()
		return nil, err
	}
	session := &PTYSession[E, X]{ctx: streamCtx, cancel: cancel, wire: g.wire, stream: stream, pid: stream.PID, user: options.User, id: hex.EncodeToString(random), events: make(chan E, ptyBuffer), done: make(chan struct{}), mapper: mapper}
	session.release = func() { g.unregister(session) }
	if err = g.register(session); err != nil {
		_ = session.invalidate()
		return nil, err
	}
	started()
	go session.pump()
	return session, nil
}

func (s *PTYSession[E, X]) pump() {
	defer close(s.events)
	defer close(s.done)
	if !s.deliver(model.PTYEvent{Kind: model.PTYStart}) {
		return
	}
	for {
		event, err := s.stream.Recv()
		if err != nil {
			if err == io.EOF {
				err = failure(model.Protocol, "PTY.Events", "END_EVENT_MISSING")
			} else if s.paused.Load() {
				err = failure(model.InstancePaused, "PTY.Events", "INSTANCE_PAUSED")
			} else {
				err = operationError(s.ctx, "PTY.Events", err)
			}
			s.setTerminal(model.ExitStatus{}, err)
			_ = s.closeStream(false)
			return
		}
		switch event.Kind {
		case dataplane.ProcessPTY:
			if len(event.Data) > 0 && !s.deliver(model.PTYEvent{Kind: model.PTYOutput, Data: append([]byte(nil), event.Data...)}) {
				return
			}
		case dataplane.ProcessEnd:
			exit := model.ExitStatus{Code: event.Exit.Code, Exited: event.Exit.Exited, Reason: event.Exit.Status, Message: event.Exit.Message}
			s.setTerminal(exit, nil)
			_ = s.deliver(model.PTYEvent{Kind: model.PTYEnd, Exit: &exit})
			s.mu.Lock()
			idle := s.active == 0
			s.mu.Unlock()
			if idle {
				_ = s.closeStream(false)
			}
			return
		}
	}
}

func (s *PTYSession[E, X]) deliver(event model.PTYEvent) bool {
	select {
	case s.events <- s.mapper.Event(event):
		return true
	default:
		s.setTerminal(model.ExitStatus{}, failure(model.ResourceExhausted, "PTY.Events", "PTY_BUFFER_FULL"))
		_ = s.closeStream(false)
		return false
	}
}

func (s *PTYSession[E, X]) setTerminal(exit model.ExitStatus, err error) {
	s.mu.Lock()
	s.exit, s.err, s.ended = exit, s.mapError(err), true
	s.mu.Unlock()
}
func (s *PTYSession[E, X]) ID() string       { return s.id }
func (s *PTYSession[E, X]) Events() <-chan E { return s.events }

func (s *PTYSession[E, X]) operation(ctx context.Context, operation string) (context.Context, func(), error) {
	s.mu.Lock()
	if s.paused.Load() {
		s.mu.Unlock()
		return nil, nil, failure(model.InstancePaused, operation, "INSTANCE_PAUSED")
	}
	if s.ended {
		s.mu.Unlock()
		return nil, nil, failure(model.Conflict, operation, "SESSION_ENDED")
	}
	s.active++
	s.mu.Unlock()
	child, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(s.ctx, func() { cancel(context.Cause(s.ctx)) })
	if cause := context.Cause(s.ctx); cause != nil {
		cancel(cause)
	}
	return child, func() {
		stop()
		cancel(context.Canceled)
		s.mu.Lock()
		s.active--
		finish := s.ended && s.active == 0
		s.mu.Unlock()
		if finish {
			_ = s.closeStream(false)
		}
	}, nil
}

func (s *PTYSession[E, X]) Write(ctx context.Context, data []byte) (err error) {
	ctx, finish, err := s.operation(ctx, "PTY.Write")
	if err != nil {
		return err
	}
	defer finish()
	err = s.wire.SendPTYInput(ctx, s.pid, data, s.user)
	return s.mapError(operationError(ctx, "PTY.Write", err))
}

func (s *PTYSession[E, X]) Resize(ctx context.Context, cols, rows uint32) (err error) {
	ctx, finish, err := s.operation(ctx, "PTY.Resize")
	if err != nil {
		return err
	}
	defer finish()
	err = s.wire.ResizePTY(ctx, s.pid, dataplane.PTYSize{Cols: cols, Rows: rows}, s.user)
	return s.mapError(operationError(ctx, "PTY.Resize", err))
}

func (s *PTYSession[E, X]) Wait(ctx context.Context) (X, error) {
	var zero X
	select {
	case <-ctx.Done():
		return zero, s.mapError(normalize("PTY.Wait", ctx.Err()))
	case <-s.done:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mapper.Exit(s.exit), s.err
}

func (s *PTYSession[E, X]) Close() error      { return s.mapError(s.closeStream(false)) }
func (s *PTYSession[E, X]) invalidate() error { s.paused.Store(true); return s.closeStream(true) }

func (s *PTYSession[E, X]) closeStream(invalidate bool) error {
	var result error
	s.once.Do(func() {
		if s.release != nil {
			defer s.release()
		}
		s.mu.Lock()
		if !invalidate && !s.ended {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			result = s.wire.SendProcessSignal(ctx, s.pid, dataplane.SignalTERM, s.user)
			cancel()
		}
		s.ended = true
		s.mu.Unlock()
		s.cancel()
		if invalidate {
			if err := s.stream.Invalidate(); result == nil {
				result = err
			}
		} else if err := s.stream.Close(); result == nil {
			result = err
		}
	})
	return normalize("PTY.Close", result)
}

func (s *PTYSession[E, X]) mapError(err error) error {
	if err == nil || s.mapper.Error == nil {
		return err
	}
	return s.mapper.Error(err)
}
