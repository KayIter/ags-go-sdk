package ags

import (
	"context"
	"sync"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/dataplane"
)

type codeEventStream interface {
	Recv() (dataplane.CodeEvent, error)
	Close() error
}

type codeTransport interface {
	createCodeContext(context.Context, string, string, int64) (dataplane.CodeContext, error)
	startCode(context.Context, dataplane.CodeRequest, int) (codeEventStream, error)
}

func (d *runtimeDataPlane) createCodeContext(ctx context.Context, language, cwd string, maxResponseBytes int64) (out dataplane.CodeContext, err error) {
	ctx, finish, _ := d.requestOperation(ctx, "Code.CreateContext")
	defer finish()
	defer func() { err = operationError(ctx, "Code.CreateContext", err) }()
	return d.wire.CreateCodeContext(ctx, language, cwd, maxResponseBytes)
}

func (d *runtimeDataPlane) startCode(ctx context.Context, request dataplane.CodeRequest, maxEventBytes int) (out codeEventStream, err error) {
	ctx, finish, started := d.requestOperation(ctx, "Code.Run")
	defer func() {
		if out == nil {
			err = operationError(ctx, "Code.Run", err)
			finish()
		}
	}()
	stream, err := d.wire.StartCode(ctx, request, maxEventBytes)
	if err != nil {
		return nil, err
	}
	started()
	return &generationCodeStream{stream: stream, ctx: ctx, finish: finish}, nil
}

type generationCodeStream struct {
	stream *dataplane.CodeStream
	ctx    context.Context
	finish func()
	once   sync.Once
}

func (s *generationCodeStream) Recv() (dataplane.CodeEvent, error) {
	if cause := context.Cause(s.ctx); cause != nil {
		return dataplane.CodeEvent{}, normalizeError("Code.Run", cause)
	}
	event, err := s.stream.Recv()
	return event, operationError(s.ctx, "Code.Run", err)
}

func (s *generationCodeStream) Close() error {
	var err error
	s.once.Do(func() {
		err = s.stream.Close()
		s.finish()
	})
	return normalizeError("Code.Run", err)
}
