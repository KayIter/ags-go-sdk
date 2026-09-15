package ags

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/dataplane"
)

func (d *runtimeDataPlane) unregister(handle interface{ invalidate() error }) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i, h := range d.handles {
		if h == handle {
			d.handles = append(d.handles[:i], d.handles[i+1:]...)
			return
		}
	}
}

// Each data-plane generation owns a cancellation root. Caller cancellation is
// distinct from generation invalidation: the latter must not kill remote jobs.
func (d *runtimeDataPlane) operation(ctx context.Context, op string) (context.Context, func()) {
	child, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(d.lifetime, func() { cancel(context.Cause(d.lifetime)) })
	if cause := context.Cause(d.lifetime); cause != nil {
		cancel(cause)
	}
	return child, func() { stop(); cancel(context.Canceled) }
}

// requestOperation adds the configured request budget to an operation. For a
// stream, started must be called after its start barrier so the delivered
// handle remains governed only by the caller and data-plane generation.
func (d *runtimeDataPlane) requestOperation(ctx context.Context, op string) (context.Context, func(), func()) {
	child, finishLifetime := d.operation(ctx, op)
	request, cancel := context.WithCancelCause(child)
	var timer *time.Timer
	if d.timeout > 0 {
		timer = time.AfterFunc(d.timeout, func() { cancel(context.DeadlineExceeded) })
	}
	stopBudget := func() {
		if timer != nil {
			timer.Stop()
		}
	}
	var once sync.Once
	finish := func() {
		once.Do(func() {
			stopBudget()
			cancel(context.Canceled)
			finishLifetime()
		})
	}
	return request, finish, stopBudget
}

func operationError(ctx context.Context, op string, err error) error {
	if cause := context.Cause(ctx); cause != nil {
		return normalizeError(op, cause)
	}
	return normalizeError(op, err)
}

func normalizeError(op string, err error) error {
	if err == nil || err == io.EOF {
		return err
	}
	var sdk *Error
	if errors.As(err, &sdk) {
		return err
	}
	code := Unavailable
	switch {
	case errors.Is(err, context.Canceled):
		code = Canceled
	case errors.Is(err, context.DeadlineExceeded):
		code = DeadlineExceeded
	default:
		var wire *dataplane.WireError
		var timeout net.Error
		var syntax *json.SyntaxError
		var shape *json.UnmarshalTypeError
		if errors.As(err, &wire) {
			code = wireErrorCode(wire)
		} else if errors.As(err, &timeout) && timeout.Timeout() {
			code = DeadlineExceeded
		} else if errors.As(err, &syntax) || errors.As(err, &shape) || errors.Is(err, io.ErrUnexpectedEOF) {
			code = Protocol
		}
	}
	reason := "REQUEST_FAILED"
	retryable := code == Unavailable || code == ResourceExhausted
	cause := err
	var wire *dataplane.WireError
	if errors.As(err, &wire) {
		if wire.Reason != "" {
			reason = wire.Reason
		}
		retryable = wire.Retryable
		if wire.Detail != "" {
			cause = errors.New(wire.Detail)
		}
	}
	return &Error{Code: code, Operation: op, Reason: reason, Cause: cause, Retryable: retryable}
}

func wireErrorCode(err *dataplane.WireError) ErrorCode {
	if err.Kind == dataplane.ErrorProtocol {
		return Protocol
	}
	if err.Kind == dataplane.ErrorHTTP {
		switch err.StatusCode {
		case 400:
			return InvalidArgument
		case 401:
			return Unauthenticated
		case 403:
			return PermissionDenied
		case 404:
			return NotFound
		case 409:
			return Conflict
		case 429:
			return ResourceExhausted
		default:
			return Unavailable
		}
	}
	code := ErrorCode(err.Code)
	switch code {
	case ErrorCode("UNKNOWN"), ErrorCode("INTERNAL"):
		return Internal
	case ErrorCode("DATA_LOSS"), ErrorCode("UNIMPLEMENTED"):
		return Protocol
	case ErrorCode("ALREADY_EXISTS"), ErrorCode("ABORTED"), ErrorCode("FAILED_PRECONDITION"):
		return Conflict
	case ErrorCode("OUT_OF_RANGE"):
		return InvalidArgument
	case "":
		return Unavailable
	default:
		return code
	}
}

type generationReader struct {
	io.ReadCloser
	ctx    context.Context
	finish func()
	op     string
	once   sync.Once
}

func (r *generationReader) Read(p []byte) (int, error) {
	if err := context.Cause(r.ctx); err != nil {
		return 0, normalizeError(r.op, err)
	}
	n, err := r.ReadCloser.Read(p)
	return n, operationError(r.ctx, r.op, err)
}
func (r *generationReader) Close() error {
	var err error
	r.once.Do(func() { err = r.ReadCloser.Close(); r.finish() })
	return normalizeError(r.op, err)
}
