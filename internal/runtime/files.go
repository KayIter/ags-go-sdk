package runtime

import (
	"context"
	"io"
	"sync"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/dataplane"
	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/model"
)

func (g *Generation) Read(ctx context.Context, path, user string) (out io.ReadCloser, err error) {
	ctx, finish, started := g.requestOperation(ctx)
	defer func() {
		err = operationError(ctx, "Files.Read", err)
		if out == nil {
			finish()
		}
	}()
	body, err := g.wire.ReadFile(ctx, path, user)
	if err != nil {
		return nil, err
	}
	started()
	return &reader{ReadCloser: body, ctx: ctx, finish: finish, operation: "Files.Read"}, nil
}

func (g *Generation) Write(ctx context.Context, path string, body io.Reader, user string) (out model.FileInfo, err error) {
	ctx, finish, _ := g.requestOperation(ctx)
	defer finish()
	defer func() { err = operationError(ctx, "Files.Write", err) }()
	info, err := g.wire.WriteFile(ctx, path, body, user)
	return fileInfo(info), err
}

func (g *Generation) List(ctx context.Context, path string, depth int, user string) (out []model.FileInfo, err error) {
	ctx, finish, _ := g.requestOperation(ctx)
	defer finish()
	defer func() { err = operationError(ctx, "Files.List", err) }()
	entries, err := g.wire.ListFiles(ctx, path, depth, user)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		out = append(out, fileInfo(entry))
	}
	return out, nil
}

func (g *Generation) Stat(ctx context.Context, path, user string) (out model.FileInfo, err error) {
	return g.fileOperation(ctx, "Files.Stat", func(ctx context.Context) (dataplane.FileInfo, error) { return g.wire.StatFile(ctx, path, user) })
}

func (g *Generation) MakeDir(ctx context.Context, path, user string) (out model.FileInfo, err error) {
	return g.fileOperation(ctx, "Files.MakeDir", func(ctx context.Context) (dataplane.FileInfo, error) { return g.wire.MakeDir(ctx, path, user) })
}

func (g *Generation) Move(ctx context.Context, source, destination, user string) (out model.FileInfo, err error) {
	return g.fileOperation(ctx, "Files.Move", func(ctx context.Context) (dataplane.FileInfo, error) {
		return g.wire.MoveFile(ctx, source, destination, user)
	})
}

func (g *Generation) Remove(ctx context.Context, path, user string) (err error) {
	ctx, finish, _ := g.requestOperation(ctx)
	defer finish()
	defer func() { err = operationError(ctx, "Files.Remove", err) }()
	return g.wire.RemoveFile(ctx, path, user)
}

func (g *Generation) fileOperation(ctx context.Context, operation string, call func(context.Context) (dataplane.FileInfo, error)) (out model.FileInfo, err error) {
	ctx, finish, _ := g.requestOperation(ctx)
	defer finish()
	defer func() { err = operationError(ctx, operation, err) }()
	info, err := call(ctx)
	return fileInfo(info), err
}

func fileInfo(value dataplane.FileInfo) model.FileInfo {
	kind := model.FileUnknown
	switch value.Type {
	case dataplane.FileTypeFile:
		kind = model.FileRegular
	case dataplane.FileTypeDirectory:
		kind = model.FileDirectory
	}
	return model.FileInfo{Name: value.Name, Path: value.Path, Type: kind, Size: value.Size, Mode: value.Mode, Permissions: value.Permissions, Owner: value.Owner, Group: value.Group, ModifiedAt: value.ModifiedAt, SymlinkTarget: value.SymlinkTarget}
}

type reader struct {
	io.ReadCloser
	ctx       context.Context
	finish    func()
	operation string
	once      sync.Once
}

func (r *reader) Read(p []byte) (int, error) {
	if cause := context.Cause(r.ctx); cause != nil {
		return 0, normalize(r.operation, cause)
	}
	n, err := r.ReadCloser.Read(p)
	return n, operationError(r.ctx, r.operation, err)
}

func (r *reader) Close() error {
	var err error
	r.once.Do(func() { err = r.ReadCloser.Close(); r.finish() })
	return normalize(r.operation, err)
}
