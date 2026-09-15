// Package model contains wire-independent values shared by private SDK layers.
// It intentionally has no dependency on the public ags package or generated code.
package model

import (
	"context"
	"fmt"
	"time"
)

// Error is a redacted internal failure translated by the public facade.
type Error struct {
	Code, Operation, Reason string
	Cause                   error
	Retryable               bool
}

func (e *Error) Error() string {
	if e.Reason == "" {
		return fmt.Sprintf("%s failed", e.Operation)
	}
	return fmt.Sprintf("%s failed: %s", e.Operation, e.Reason)
}

func (e *Error) Unwrap() error { return e.Cause }

const (
	Canceled          = "CANCELED"
	DeadlineExceeded  = "DEADLINE_EXCEEDED"
	InvalidArgument   = "INVALID_ARGUMENT"
	Unauthenticated   = "UNAUTHENTICATED"
	PermissionDenied  = "PERMISSION_DENIED"
	NotFound          = "NOT_FOUND"
	Conflict          = "CONFLICT"
	ResourceExhausted = "RESOURCE_EXHAUSTED"
	InstancePaused    = "INSTANCE_PAUSED"
	Internal          = "INTERNAL"
	Protocol          = "PROTOCOL"
	Unavailable       = "UNAVAILABLE"
)

type FileType uint8

const (
	FileUnknown FileType = iota
	FileRegular
	FileDirectory
)

type FileInfo struct {
	Name, Path, Permissions, Owner, Group string
	Type                                  FileType
	Size                                  int64
	Mode                                  uint32
	ModifiedAt                            time.Time
	SymlinkTarget                         *string
}

type WatchOptions struct {
	User                    string
	Recursive, IncludeEntry bool
	Buffer                  int
}

type FileEventKind uint8

const (
	FileEventUnknown FileEventKind = iota
	FileEventCreate
	FileEventWrite
	FileEventRemove
	FileEventRename
	FileEventChmod
)

type FileEvent struct {
	WatchID  string
	Sequence uint64
	Kind     FileEventKind
	Path     string
	OldPath  *string
	Entry    *FileInfo
}

type ProcessConfig struct {
	Command, CWD, User string
	Args               []string
	Env                map[string]string
	MaxOutputBytes     int64
}

type ProcessSignal uint8

const (
	SignalTERM ProcessSignal = iota + 1
	SignalKILL
)

type ExitStatus struct {
	Code            int
	Exited          bool
	Reason, Message string
}

type CommandEventKind uint8

const (
	CommandStdout CommandEventKind = iota + 1
	CommandStderr
	CommandExit
)

type CommandEvent struct {
	Kind CommandEventKind
	Data []byte
	Exit *ExitStatus
}

type CommandResult struct {
	ExitCode                         int
	Stdout, Stderr                   []byte
	StdoutTruncated, StderrTruncated bool
}

type ProcessInfo struct {
	PID      uint32
	Tag, CWD *string
	Command  string
	Args     []string
	Env      map[string]string
}

type PTYConfig struct {
	ProcessConfig
	Cols, Rows uint32
	Buffer     int
}

type PTYEventKind uint8

const (
	PTYStart PTYEventKind = iota + 1
	PTYOutput
	PTYEnd
)

type PTYEvent struct {
	Kind PTYEventKind
	Data []byte
	Exit *ExitStatus
}

type CodeContext struct{ ID, Language, CWD string }

type CodeRequest struct {
	Source, ContextID, Language string
	Env                         map[string]string
	MaxEventBytes               int
}

type CodeResult struct {
	Text, HTML, Markdown, SVG, PNG, JPEG, PDF, Latex, JavaScript *string
	JSON, Data, Chart, Extra                                     map[string]any
	IsMainResult                                                 bool
}

type CodeExecutionError struct{ Name, Value, Traceback string }

type CodeExecution struct {
	Results                                                             []CodeResult
	Stdout, Stderr                                                      []string
	Error                                                               *CodeExecutionError
	ExecutionCount                                                      *int
	StdoutTruncated, StderrTruncated, ResultsTruncated, EventsTruncated bool
	CallbackError                                                       *Error
}

type CodeLimits struct {
	MaxOutputBytes, MaxResultBytes int64
	MaxEvents                      int
	CallbackTimeout                time.Duration
}

type CodeCallbacks struct {
	OnStdout func(string)
	OnStderr func(string)
	OnResult func(CodeResult)
}

func ContextError(ctx context.Context, operation string) error {
	if err := ctx.Err(); err != nil {
		code := Canceled
		if err == context.DeadlineExceeded {
			code = DeadlineExceeded
		}
		return &Error{Code: code, Operation: operation, Cause: err}
	}
	return nil
}
