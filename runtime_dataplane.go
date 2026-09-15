package ags

import (
	"context"
	"io"
	"time"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/dataplane"
	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/model"
	internalruntime "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/runtime"
)

// runtimeDataPlane is a temporary facade adapter. It owns no lifecycle state;
// all state and streams belong to the internal Generation.
type runtimeDataPlane struct{ inner *internalruntime.Generation }

func newRuntimeDataPlane(wire *dataplane.Client, timeout time.Duration) *runtimeDataPlane {
	return &runtimeDataPlane{inner: internalruntime.NewGeneration(wire, timeout)}
}

func (d *runtimeDataPlane) generation() *internalruntime.Generation { return d.inner }
func (d *runtimeDataPlane) Ready(ctx context.Context) error {
	return normalizeError("dataPlane.Ready", d.inner.Ready(ctx))
}
func (d *runtimeDataPlane) Close() error { return normalizeError("dataPlane.Close", d.inner.Close()) }
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

func (d *runtimeDataPlane) Run(ctx context.Context, command string, opts CommandOptions) (CommandResult, error) {
	value, err := d.inner.Run(ctx, model.ProcessConfig{Command: command, Args: opts.Args, Env: opts.Env, CWD: opts.Cwd, User: string(opts.User), MaxOutputBytes: opts.MaxOutputBytes})
	if err != nil {
		return CommandResult{}, normalizeError("Commands.Run", err)
	}
	return commandMapper().Result(value), nil
}

func (d *runtimeDataPlane) Start(ctx context.Context, command string, opts StartOptions) (*CommandHandle, error) {
	handle, err := internalruntime.StartCommand(d.inner, ctx, model.ProcessConfig{Command: command, Args: opts.Args, Env: opts.Env, CWD: opts.Cwd, User: string(opts.User), MaxOutputBytes: opts.MaxOutputBytes}, commandMapper())
	if err != nil {
		return nil, normalizeError("Commands.Start", err)
	}
	return &CommandHandle{inner: handle}, nil
}

func (d *runtimeDataPlane) ConnectCommand(ctx context.Context, pid uint32, opts ConnectCommandOptions) (*CommandHandle, error) {
	handle, err := internalruntime.ConnectCommand(d.inner, ctx, pid, string(opts.User), opts.MaxOutputBytes, commandMapper())
	if err != nil {
		return nil, normalizeError("Commands.Connect", err)
	}
	return &CommandHandle{inner: handle}, nil
}

func (d *runtimeDataPlane) ListCommands(ctx context.Context, user SandboxUser) ([]ProcessInfo, error) {
	values, err := d.inner.ListCommands(ctx, string(user))
	if err != nil {
		return nil, normalizeError("Commands.List", err)
	}
	out := make([]ProcessInfo, 0, len(values))
	for _, value := range values {
		out = append(out, ProcessInfo{PID: value.PID, Tag: value.Tag, Cmd: value.Command, Args: append([]string(nil), value.Args...), Env: cloneStringMap(value.Env), CWD: value.CWD})
	}
	return out, nil
}

func (d *runtimeDataPlane) Watch(ctx context.Context, root string, opts WatchOptions) (*WatchHandle, error) {
	handle, err := internalruntime.StartWatch(d.inner, ctx, root, model.WatchOptions{User: string(opts.User), Recursive: opts.Recursive, IncludeEntry: opts.IncludeEntry, Buffer: opts.Buffer}, internalruntime.WatchMapper[FileEvent]{Event: mapWatchEvent, Error: func(err error) error { return normalizeError("Files.Watch", err) }})
	if err != nil {
		return nil, normalizeError("Files.Watch", err)
	}
	return &WatchHandle{inner: handle}, nil
}

func (d *runtimeDataPlane) OpenPTY(ctx context.Context, opts PTYOptions) (*PTYSession, error) {
	handle, err := internalruntime.OpenPTY(d.inner, ctx, model.PTYConfig{ProcessConfig: model.ProcessConfig{Command: opts.Command, Args: append([]string(nil), opts.Args...), Env: cloneStringMap(opts.Env), CWD: opts.Cwd, User: string(opts.User)}, Cols: opts.Cols, Rows: opts.Rows}, internalruntime.PTYMapper[PTYEvent, ExitStatus]{Event: mapPTYEvent, Exit: mapExitStatus, Error: func(err error) error { return normalizeError("PTY", err) }})
	if err != nil {
		return nil, normalizeError("PTY.Open", err)
	}
	return &PTYSession{inner: handle}, nil
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

func mapPTYEvent(value model.PTYEvent) PTYEvent {
	kind := PTYStart
	switch value.Kind {
	case model.PTYOutput:
		kind = PTYOutput
	case model.PTYEnd:
		kind = PTYEnd
	}
	out := PTYEvent{Type: kind, Data: append([]byte(nil), value.Data...)}
	if value.Exit != nil {
		exit := mapExitStatus(*value.Exit)
		out.Exit = &exit
	}
	return out
}
