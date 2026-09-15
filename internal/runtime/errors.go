package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/dataplane"
	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/model"
)

func normalize(operation string, err error) error {
	if err == nil || err == io.EOF {
		return err
	}
	var internal *model.Error
	if errors.As(err, &internal) {
		return err
	}
	code := model.Unavailable
	switch {
	case errors.Is(err, context.Canceled):
		code = model.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		code = model.DeadlineExceeded
	default:
		var wire *dataplane.WireError
		var timeout net.Error
		var syntax *json.SyntaxError
		var shape *json.UnmarshalTypeError
		if errors.As(err, &wire) {
			code = wireCode(wire)
		} else if errors.As(err, &timeout) && timeout.Timeout() {
			code = model.DeadlineExceeded
		} else if errors.As(err, &syntax) || errors.As(err, &shape) || errors.Is(err, io.ErrUnexpectedEOF) {
			code = model.Protocol
		}
	}
	reason := "REQUEST_FAILED"
	retryable := code == model.Unavailable || code == model.ResourceExhausted
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
	return &model.Error{Code: code, Operation: operation, Reason: reason, Cause: cause, Retryable: retryable}
}

func operationError(ctx context.Context, operation string, err error) error {
	if cause := context.Cause(ctx); cause != nil {
		return normalize(operation, cause)
	}
	return normalize(operation, err)
}

func wireCode(err *dataplane.WireError) string {
	if err.Kind == dataplane.ErrorProtocol {
		return model.Protocol
	}
	if err.Kind == dataplane.ErrorHTTP {
		switch err.StatusCode {
		case 400:
			return model.InvalidArgument
		case 401:
			return model.Unauthenticated
		case 403:
			return model.PermissionDenied
		case 404:
			return model.NotFound
		case 409:
			return model.Conflict
		case 429:
			return model.ResourceExhausted
		default:
			return model.Unavailable
		}
	}
	switch err.Code {
	case "UNKNOWN", "INTERNAL":
		return model.Internal
	case "DATA_LOSS", "UNIMPLEMENTED":
		return model.Protocol
	case "ALREADY_EXISTS", "ABORTED", "FAILED_PRECONDITION":
		return model.Conflict
	case "OUT_OF_RANGE":
		return model.InvalidArgument
	case "":
		return model.Unavailable
	default:
		return err.Code
	}
}

func failure(code, operation, reason string) error {
	return &model.Error{Code: code, Operation: operation, Reason: reason}
}
