package ags

import "fmt"

// ErrorCode is the stable public error category.
type ErrorCode string

const (
	// InvalidArgument reports invalid caller input.
	InvalidArgument ErrorCode = "INVALID_ARGUMENT"
	// Unauthenticated reports missing or rejected authentication.
	Unauthenticated ErrorCode = "UNAUTHENTICATED"
	// PermissionDenied reports insufficient permission or unavailable required credential
	// capability.
	PermissionDenied ErrorCode = "PERMISSION_DENIED"
	// NotFound reports an absent resource.
	NotFound ErrorCode = "NOT_FOUND"
	// Conflict reports an incompatible resource or local-handle state.
	Conflict ErrorCode = "CONFLICT"
	// ResourceExhausted reports a quota, capacity or bounded-buffer limit.
	ResourceExhausted ErrorCode = "RESOURCE_EXHAUSTED"
	// DeadlineExceeded reports an expired operation budget, not proof of remote cancellation.
	DeadlineExceeded ErrorCode = "DEADLINE_EXCEEDED"
	// Canceled reports local cancellation, not proof of remote termination.
	Canceled ErrorCode = "CANCELED"
	// Unavailable reports a service or transport failure.
	Unavailable ErrorCode = "UNAVAILABLE"
	// InstanceNotReady reports a sandbox whose data plane is not ready.
	InstanceNotReady ErrorCode = "INSTANCE_NOT_READY"
	// InstancePaused reports an invalidated or paused data-plane generation.
	InstancePaused ErrorCode = "INSTANCE_PAUSED"
	// Protocol reports malformed or inconsistent wire data.
	Protocol ErrorCode = "PROTOCOL"
	// Internal reports an internal SDK failure.
	Internal ErrorCode = "INTERNAL"
	// Unsupported reports a capability unavailable in the selected mode or adapter.
	Unsupported ErrorCode = "UNSUPPORTED"
)

// Error retains branchable semantics without including secrets or response bodies.
type Error struct {
	// Code is the stable category used by errors.Is comparisons.
	Code ErrorCode
	// Reason is a diagnostic identifier, Operation names the SDK operation, and RequestID is
	// optional service correlation data.
	Reason, Operation, RequestID string
	// Retryable classifies a potentially transient error; it never authorizes replay of a side
	// effect.
	Retryable bool
	// Cause preserves the wrapped error. Caller-provided causes can contain sensitive data; do
	// not log them blindly.
	Cause error
	// InstanceID is set when creation was accepted but waiting/connecting failed.
	// The instance may still exist; the SDK never assumes ownership of a replay.
	InstanceID string
	// Mutation is optional evidence for a failed update; nil does not mean NOT_SENT.
	Mutation *MutationInfo
}

// Error formats the operation, category and diagnostic reason without rendering Cause,
// InstanceID or a raw response.
func (e *Error) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("ags %s (%s): %s", e.Operation, e.Code, e.Reason)
	}
	return fmt.Sprintf("ags %s (%s)", e.Operation, e.Code)
}

// Unwrap exposes the underlying cause for errors.As and errors.Is.
func (e *Error) Unwrap() error { return e.Cause }

// Is matches another nonempty SDK error category; it does not compare operation, request ID or
// reason.
func (e *Error) Is(target error) bool {
	other, ok := target.(*Error)
	return ok && other.Code != "" && e.Code == other.Code
}
func codeError(code ErrorCode, op, reason string) error {
	return &Error{Code: code, Operation: op, Reason: reason, Retryable: code == Unavailable || code == ResourceExhausted || code == InstanceNotReady}
}
