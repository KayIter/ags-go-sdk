package runtime

import (
	"context"
	"sync"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/model"
)

// Owner serializes Sandbox lifecycle changes and owns the current generation.
type Owner struct {
	mu         sync.Mutex
	lifecycle  gate
	generation *Generation
	closed     bool
}

func NewOwner(generation *Generation) *Owner { return &Owner{generation: generation} }

func (o *Owner) Lock(ctx context.Context) error { return o.lifecycle.acquire(ctx) }
func (o *Owner) Unlock()                        { o.lifecycle.release() }

func (o *Owner) Generation(operation string) (*Generation, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return nil, failure(model.Conflict, operation, "SANDBOX_CLOSED")
	}
	if o.generation == nil {
		return nil, failure(model.InstancePaused, operation, "INSTANCE_PAUSED")
	}
	return o.generation, nil
}

func (o *Owner) Replace(generation *Generation) error {
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		if generation != nil {
			_ = generation.Close()
		}
		return failure(model.Conflict, "Sandbox.Resume", "SANDBOX_CLOSED")
	}
	old := o.generation
	o.generation = generation
	o.mu.Unlock()
	if old != nil {
		return old.Close()
	}
	return nil
}

func (o *Owner) Invalidate() error {
	o.mu.Lock()
	old := o.generation
	o.generation = nil
	o.mu.Unlock()
	if old != nil {
		return old.Close()
	}
	return nil
}

func (o *Owner) Close() error {
	o.mu.Lock()
	o.closed = true
	old := o.generation
	o.generation = nil
	o.mu.Unlock()
	if old != nil {
		return old.Close()
	}
	return nil
}

func (o *Owner) IsClosed() bool { o.mu.Lock(); defer o.mu.Unlock(); return o.closed }
