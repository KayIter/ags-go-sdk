// Package runtime owns one Sandbox data-plane generation and every handle
// created from it. It does not depend on the public ags facade.
package runtime

import (
	"context"
	"sync"
	"time"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/dataplane"
	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/model"
)

type invalidator interface{ invalidate() error }

// Generation owns immutable connection material, cancellation, and children.
type Generation struct {
	wire     *dataplane.Client
	timeout  time.Duration
	mu       sync.Mutex
	handles  []invalidator
	closed   bool
	lifetime context.Context
	cancel   context.CancelCauseFunc
}

// NewGeneration creates one private generation.
func NewGeneration(wire *dataplane.Client, timeout time.Duration) *Generation {
	lifetime, cancel := context.WithCancelCause(context.Background())
	return &Generation{wire: wire, timeout: timeout, lifetime: lifetime, cancel: cancel}
}

func (g *Generation) operation(ctx context.Context) (context.Context, func()) {
	child, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(g.lifetime, func() { cancel(context.Cause(g.lifetime)) })
	if cause := context.Cause(g.lifetime); cause != nil {
		cancel(cause)
	}
	return child, func() { stop(); cancel(context.Canceled) }
}

func (g *Generation) requestOperation(ctx context.Context) (context.Context, func(), func()) {
	child, finishLifetime := g.operation(ctx)
	request, cancel := context.WithCancelCause(child)
	var timer *time.Timer
	if g.timeout > 0 {
		timer = time.AfterFunc(g.timeout, func() { cancel(context.DeadlineExceeded) })
	}
	stopBudget := func() {
		if timer != nil {
			timer.Stop()
		}
	}
	var once sync.Once
	finish := func() {
		once.Do(func() { stopBudget(); cancel(context.Canceled); finishLifetime() })
	}
	return request, finish, stopBudget
}

func (g *Generation) register(handle invalidator) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return failure(model.InstancePaused, "dataPlane", "INSTANCE_PAUSED")
	}
	g.handles = append(g.handles, handle)
	return nil
}

func (g *Generation) unregister(handle invalidator) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for i, current := range g.handles {
		if current == handle {
			g.handles = append(g.handles[:i], g.handles[i+1:]...)
			return
		}
	}
}

// Close invalidates local children without sending remote mutation signals.
func (g *Generation) Close() error {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return nil
	}
	g.closed = true
	g.cancel(failure(model.InstancePaused, "dataPlane", "INSTANCE_PAUSED"))
	handles := append([]invalidator(nil), g.handles...)
	g.handles = nil
	g.mu.Unlock()
	for _, handle := range handles {
		_ = handle.invalidate()
	}
	return nil
}

// Ready probes the current runtime without retaining a handle.
func (g *Generation) Ready(ctx context.Context) error {
	if _, err := g.List(ctx, "/tmp", 1, "user"); err == nil {
		return nil
	}
	_, err := g.List(ctx, "/tmp", 1, "root")
	return err
}
