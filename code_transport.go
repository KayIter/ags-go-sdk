package ags

import (
	"context"
	"io"
)

type codeTransport interface {
	codeRequest(context.Context, string, string, []byte) (io.ReadCloser, error)
}

func (d *runtimeDataPlane) codeRequest(ctx context.Context, operation, requestPath string, body []byte) (out io.ReadCloser, err error) {
	ctx, finish, started := d.requestOperation(ctx, operation)
	defer func() {
		err = operationError(ctx, operation, err)
		if out == nil {
			finish()
		}
	}()
	response, err := d.wire.CodeRequest(ctx, requestPath, body)
	if err != nil {
		return nil, mapDataPlaneError(operation, err)
	}
	started()
	return &generationReader{ReadCloser: response, ctx: ctx, finish: finish, op: operation}, nil
}
