package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/model"
)

func TestGenerationCloseSynchronouslyCancelsOperation(t *testing.T) {
	generation := NewGeneration(nil, 0)
	operation, finish := generation.operation(context.Background())
	defer finish()
	if err := generation.Close(); err != nil {
		t.Fatal(err)
	}
	var failure *model.Error
	if cause := context.Cause(operation); !errors.As(cause, &failure) || failure.Code != model.InstancePaused {
		t.Fatalf("operation cause after Close = %#v", cause)
	}
}

func TestOperationHonorsCallerCancellation(t *testing.T) {
	generation := NewGeneration(nil, 0)
	caller, cancel := context.WithCancel(context.Background())
	operation, finish := generation.operation(caller)
	defer finish()
	cancel()
	select {
	case <-operation.Done():
	case <-time.After(time.Second):
		t.Fatal("caller cancellation was not propagated")
	}
	if !errors.Is(context.Cause(operation), context.Canceled) {
		t.Fatalf("operation cause = %v", context.Cause(operation))
	}
}

func TestOwnerReplacementInvalidatesOldGeneration(t *testing.T) {
	old := NewGeneration(nil, 0)
	owner := NewOwner(old)
	next := NewGeneration(nil, 0)
	if err := owner.Replace(next); err != nil {
		t.Fatal(err)
	}
	old.mu.Lock()
	closed := old.closed
	old.mu.Unlock()
	if !closed {
		t.Fatal("old generation remained usable")
	}
	if got, err := owner.Generation("fixture"); err != nil || got != next {
		t.Fatalf("current generation = %p, %v", got, err)
	}
}
