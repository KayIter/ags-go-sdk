package dataplane

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"connectrpc.com/connect"
)

// ErrorKind identifies a private wire failure without exposing Connect types.
type ErrorKind string

const (
	ErrorRPC      ErrorKind = "RPC"
	ErrorHTTP     ErrorKind = "HTTP"
	ErrorProtocol ErrorKind = "PROTOCOL"
)

// WireError is the safe error boundary between the wire adapter and the SDK facade.
type WireError struct {
	Kind       ErrorKind
	Code       string
	Reason     string
	Detail     string
	StatusCode int
	Retryable  bool
}

func (e *WireError) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("data-plane %s failure: %s", strings.ToLower(string(e.Kind)), e.Reason)
	}
	return fmt.Sprintf("data-plane %s failure", strings.ToLower(string(e.Kind)))
}

func wrapWireError(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var wire *WireError
	if errors.As(err, &wire) {
		return err
	}
	var rpc *connect.Error
	if errors.As(err, &rpc) {
		code := strings.ToUpper(rpc.Code().String())
		return &WireError{Kind: ErrorRPC, Code: code, Reason: "REQUEST_FAILED", Retryable: rpc.Code() == connect.CodeUnavailable || rpc.Code() == connect.CodeResourceExhausted}
	}
	return err
}

func httpError(status int) error {
	return &WireError{Kind: ErrorHTTP, StatusCode: status, Reason: fmt.Sprintf("HTTP_%d", status), Retryable: status == 429 || status >= 500}
}

func protocolError(reason string) error {
	return &WireError{Kind: ErrorProtocol, Reason: reason}
}

func protocolErrorDetail(reason, detail string) error {
	return &WireError{Kind: ErrorProtocol, Reason: reason, Detail: detail}
}
