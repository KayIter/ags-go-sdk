package ags

import (
	"context"
	"errors"
	"io"
	"strings"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/model"
	internalruntime "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/runtime"
)

func (d *runtimeDataPlane) Read(ctx context.Context, path, user string) (io.ReadCloser, error) {
	reader, err := d.inner.Read(ctx, path, user)
	if err != nil {
		return nil, normalizeError("Files.Read", err)
	}
	return &facadeReader{ReadCloser: reader}, nil
}

type facadeReader struct{ io.ReadCloser }

func (r *facadeReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	return n, normalizeError("Files.Read", err)
}
func (r *facadeReader) Close() error { return normalizeError("Files.Read", r.ReadCloser.Close()) }

func (d *runtimeDataPlane) Write(ctx context.Context, path string, body io.Reader, user string) (FileInfo, error) {
	value, err := d.inner.Write(ctx, path, body, user)
	return mapRuntimeFileInfo(value), normalizeError("Files.Write", err)
}

func (d *runtimeDataPlane) List(ctx context.Context, path string, depth int, user string) ([]FileInfo, error) {
	values, err := d.inner.List(ctx, path, depth, user)
	if err != nil {
		return nil, normalizeError("Files.List", err)
	}
	out := make([]FileInfo, 0, len(values))
	for _, value := range values {
		out = append(out, mapRuntimeFileInfo(value))
	}
	return out, nil
}

func (d *runtimeDataPlane) Stat(ctx context.Context, path, user string) (FileInfo, error) {
	value, err := d.inner.Stat(ctx, path, user)
	return mapRuntimeFileInfo(value), normalizeError("Files.Stat", err)
}
func (d *runtimeDataPlane) MakeDir(ctx context.Context, path, user string) (FileInfo, error) {
	value, err := d.inner.MakeDir(ctx, path, user)
	return mapRuntimeFileInfo(value), normalizeError("Files.MakeDir", err)
}
func (d *runtimeDataPlane) Move(ctx context.Context, source, destination, user string) (FileInfo, error) {
	value, err := d.inner.Move(ctx, source, destination, user)
	return mapRuntimeFileInfo(value), normalizeError("Files.Move", err)
}
func (d *runtimeDataPlane) Remove(ctx context.Context, path, user string) error {
	return normalizeError("Files.Remove", d.inner.Remove(ctx, path, user))
}
func (d *runtimeDataPlane) Watch(ctx context.Context, root string, opts WatchOptions) (*WatchHandle, error) {
	handle, err := internalruntime.StartWatch(d.inner, ctx, root, model.WatchOptions{User: string(opts.User), Recursive: opts.Recursive, IncludeEntry: opts.IncludeEntry, Buffer: opts.Buffer}, internalruntime.WatchMapper[FileEvent]{Event: mapWatchEvent, Error: func(err error) error { return normalizeError("Files.Watch", err) }})
	if err != nil {
		return nil, normalizeError("Files.Watch", err)
	}
	return &WatchHandle{inner: handle}, nil
}

func mapRuntimeFileInfo(value model.FileInfo) FileInfo {
	kind := UnknownFileType
	switch value.Type {
	case model.FileRegular:
		kind = File
	case model.FileDirectory:
		kind = Directory
	}
	return FileInfo{Name: value.Name, Path: value.Path, Type: kind, Size: value.Size, Mode: value.Mode, Permissions: value.Permissions, Owner: value.Owner, Group: value.Group, ModifiedAt: value.ModifiedAt, SymlinkTarget: value.SymlinkTarget}
}

func mapWatchEvent(value model.FileEvent) FileEvent {
	kind := FileEventType("UNKNOWN")
	switch value.Kind {
	case model.FileEventCreate:
		kind = FileCreate
	case model.FileEventWrite:
		kind = FileWrite
	case model.FileEventRemove:
		kind = FileRemove
	case model.FileEventRename:
		kind = FileRename
	case model.FileEventChmod:
		kind = FileChmod
	}
	out := FileEvent{WatchID: value.WatchID, Sequence: value.Sequence, Type: kind, Path: value.Path, OldPath: value.OldPath}
	if value.Entry != nil {
		entry := mapRuntimeFileInfo(*value.Entry)
		out.Entry = &entry
	}
	return out
}

// StatOptions selects the remote identity for a metadata query.
type StatOptions struct {
	// User selects the logical runtime account; empty defaults to User.
	User SandboxUser
}

// MakeDirOptions selects the remote identity. Parent/existing-directory behavior is runtime-defined.
type MakeDirOptions struct {
	// User selects the logical runtime account; empty defaults to User.
	User SandboxUser
}

// MoveOptions selects the remote identity. No atomic no-overwrite guarantee is implied.
type MoveOptions struct {
	// User selects the logical runtime account; empty defaults to User.
	User SandboxUser
}

// RemoveOptions selects the remote identity. Directory removal follows the verified runtime contract.
type RemoveOptions struct {
	// User selects the logical runtime account; empty defaults to User.
	User SandboxUser
}

// ExistsOptions selects the remote identity for an existence query.
type ExistsOptions struct {
	// User selects the logical runtime account; empty defaults to User.
	User SandboxUser
}

// fileOperations keeps the additional RPC family separate from streaming reads/watch.
type fileOperations interface {
	Stat(context.Context, string, string) (FileInfo, error)
	MakeDir(context.Context, string, string) (FileInfo, error)
	Move(context.Context, string, string, string) (FileInfo, error)
	Remove(context.Context, string, string) error
}

func (f *Files) operations(op string, user SandboxUser, paths ...string) (fileOperations, error) {
	for _, p := range paths {
		if p == "" || strings.ContainsRune(p, 0) {
			return nil, codeError(InvalidArgument, op, "INVALID_PATH")
		}
	}
	if err := validateUser(user, op); err != nil {
		return nil, err
	}
	plane, err := f.sandbox.dataPlane(op)
	if err != nil {
		return nil, err
	}
	ops, ok := plane.(fileOperations)
	if !ok {
		return nil, codeError(Unsupported, op, "FILE_OPERATIONS_UNAVAILABLE")
	}
	return ops, nil
}

// Stat returns remote metadata without modifying the entry.
func (f *Files) Stat(ctx context.Context, path string, opts StatOptions) (FileInfo, error) {
	p, e := f.operations("Files.Stat", opts.User, path)
	if e != nil {
		return FileInfo{}, e
	}
	return p.Stat(ctx, path, string(opts.User))
}

// Exists reports whether a remote entry exists. A NotFound response is returned as false,
// nil; authentication, authorization, transport and protocol failures are preserved.
func (f *Files) Exists(ctx context.Context, path string, opts ExistsOptions) (bool, error) {
	_, err := f.Stat(ctx, path, StatOptions{User: opts.User})
	if err == nil {
		return true, nil
	}
	var failure *Error
	if errors.As(err, &failure) && failure.Code == NotFound {
		return false, nil
	}
	return false, err
}

// MakeDir creates a remote directory using the runtime's filesystem semantics.
func (f *Files) MakeDir(ctx context.Context, path string, opts MakeDirOptions) (FileInfo, error) {
	p, e := f.operations("Files.MakeDir", opts.User, path)
	if e != nil {
		return FileInfo{}, e
	}
	return p.MakeDir(ctx, path, string(opts.User))
}

// Move moves a remote entry. It does not emulate no-overwrite or cross-filesystem copying.
func (f *Files) Move(ctx context.Context, source, destination string, opts MoveOptions) (FileInfo, error) {
	p, e := f.operations("Files.Move", opts.User, source, destination)
	if e != nil {
		return FileInfo{}, e
	}
	return p.Move(ctx, source, destination, string(opts.User))
}

// Remove removes a remote entry. Cancellation does not establish that removal was not applied.
func (f *Files) Remove(ctx context.Context, path string, opts RemoveOptions) error {
	p, e := f.operations("Files.Remove", opts.User, path)
	if e != nil {
		return e
	}
	return p.Remove(ctx, path, string(opts.User))
}
