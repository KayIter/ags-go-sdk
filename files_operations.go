package ags

import (
	"context"
	"errors"
	"strings"
)

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
