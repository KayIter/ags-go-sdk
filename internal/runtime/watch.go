package runtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"path"
	"sync"
	"sync/atomic"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/dataplane"
	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/model"
)

const defaultWatchBuffer = 32

// WatchMapper converts private values at the facade boundary without a second pump.
type WatchMapper[E any] struct {
	Event func(model.FileEvent) E
	Error func(error) error
}

// WatchHandle owns stream parsing, generation invalidation, and bounded delivery.
type WatchHandle[E any] struct {
	ctx           context.Context
	cancel        context.CancelFunc
	stream        *dataplane.WatchStream
	root, watchID string
	sequence      uint64
	events        chan E
	mu            sync.Mutex
	err           error
	once          sync.Once
	paused        atomic.Bool
	release       func()
	mapper        WatchMapper[E]
}

func StartWatch[E any](g *Generation, ctx context.Context, root string, options model.WatchOptions, mapper WatchMapper[E]) (out *WatchHandle[E], err error) {
	streamCtx, cancel, started := g.requestOperation(ctx)
	defer func() {
		if err != nil {
			if cause := context.Cause(g.lifetime); cause != nil {
				err = cause
			}
			err = normalize("Files.Watch", err)
		}
	}()
	stream, err := g.wire.StartWatch(streamCtx, root, options.Recursive, options.IncludeEntry, options.User)
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
	buffer := options.Buffer
	if buffer == 0 {
		buffer = defaultWatchBuffer
	}
	handle := &WatchHandle[E]{ctx: streamCtx, cancel: cancel, stream: stream, root: root, watchID: hex.EncodeToString(random), events: make(chan E, buffer), mapper: mapper}
	handle.release = func() { g.unregister(handle) }
	if err = g.register(handle); err != nil {
		_ = handle.invalidate()
		return nil, err
	}
	started()
	go handle.pump()
	return handle, nil
}

func (h *WatchHandle[E]) pump() {
	defer close(h.events)
	for {
		event, err := h.stream.Recv(h.ctx)
		if err != nil {
			if err != io.EOF {
				if h.paused.Load() {
					err = failure(model.InstancePaused, "Files.Watch", "INSTANCE_PAUSED")
				} else {
					err = operationError(h.ctx, "Files.Watch", err)
				}
				h.setError(err)
			}
			_ = h.closeStream(false)
			return
		}
		h.sequence++
		eventPath := event.Name
		if !path.IsAbs(eventPath) {
			eventPath = path.Join(h.root, eventPath)
		}
		out := model.FileEvent{WatchID: h.watchID, Sequence: h.sequence, Kind: watchKind(event.Type), Path: eventPath}
		if event.Entry != nil {
			entry := fileInfo(*event.Entry)
			out.Entry = &entry
		}
		select {
		case h.events <- h.mapper.Event(out):
		default:
			h.setError(failure(model.ResourceExhausted, "Files.Watch", "WATCH_BUFFER_FULL"))
			_ = h.closeStream(false)
			return
		}
	}
}

func (h *WatchHandle[E]) Events() <-chan E  { return h.events }
func (h *WatchHandle[E]) Err() error        { h.mu.Lock(); defer h.mu.Unlock(); return h.err }
func (h *WatchHandle[E]) Close() error      { return h.mapError(h.closeStream(false)) }
func (h *WatchHandle[E]) invalidate() error { h.paused.Store(true); return h.closeStream(true) }

func (h *WatchHandle[E]) setError(err error) { h.mu.Lock(); h.err = h.mapError(err); h.mu.Unlock() }

func (h *WatchHandle[E]) closeStream(invalidate bool) error {
	var err error
	h.once.Do(func() {
		h.cancel()
		if invalidate {
			err = h.stream.Invalidate()
		} else {
			err = h.stream.Close()
		}
		if h.release != nil {
			h.release()
		}
	})
	return normalize("Files.Watch", err)
}

func (h *WatchHandle[E]) mapError(err error) error {
	if err == nil || h.mapper.Error == nil {
		return err
	}
	return h.mapper.Error(err)
}

func watchKind(value dataplane.WatchEventType) model.FileEventKind {
	switch value {
	case dataplane.WatchCreate:
		return model.FileEventCreate
	case dataplane.WatchWrite:
		return model.FileEventWrite
	case dataplane.WatchRemove:
		return model.FileEventRemove
	case dataplane.WatchRename:
		return model.FileEventRename
	case dataplane.WatchChmod:
		return model.FileEventChmod
	default:
		return model.FileEventUnknown
	}
}
