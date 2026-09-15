package ags

import (
	"context"
	"io"
	"time"
)

// CloudCredential is an explicit Tencent Cloud SecretID/SecretKey pair. Default formatting is
// redacted; do not serialize its exported fields into logs.
type CloudCredential struct {
	// SecretID and SecretKey authenticate Cloud control and Metrics requests; both must be nonempty.
	SecretID, SecretKey string
}

// Valid reports whether both fields are nonempty; it does not authenticate with the service.
func (c CloudCredential) Valid() bool { return c.SecretID != "" && c.SecretKey != "" }

// String and GoString deliberately omit both credential fields.
func (c CloudCredential) String() string { return "CloudCredential{REDACTED}" }

// GoString returns a redacted Go-syntax representation.
func (c CloudCredential) GoString() string { return c.String() }

// MarshalJSON prevents accidental credential disclosure by encoding only a redacted marker.
func (c CloudCredential) MarshalJSON() ([]byte, error) {
	return []byte(`"CloudCredential[REDACTED]"`), nil
}

// MarshalText prevents text-based serializers from receiving the credential fields.
func (c CloudCredential) MarshalText() ([]byte, error) {
	return []byte(c.String()), nil
}

// ToolRef selects exactly one existing sandbox tool.
type ToolRef struct {
	// ID and Name are mutually exclusive.
	ID, Name string
}

// CreateOptions configures a new remote sandbox. Do not mutate maps or pointed-to values
// during an operation; creation can incur charges.
type CreateOptions struct {
	// Tool identifies the existing runtime configuration; exactly one ID or Name is required.
	Tool ToolRef
	// Timeout is the requested sandbox lifetime, not the operation deadline. Nil preserves service
	// defaults; non-nil must be 30 seconds to 24 hours, subject to service restrictions.
	Timeout *time.Duration
	// ClientToken is optional Cloud create idempotency (at most 64 characters). Persist and reuse
	// identical parameters for recovery.
	ClientToken string
	// Metadata supplies string labels copied before submission.
	Metadata map[string]string
	// Env is submitted at creation. Runtime initialization and persistence belong to the service.
	Env map[string]string
	// MountOptions overrides storage mounts declared by the selected Tool. The slice and any
	// optional ReadOnly values are copied before the request is sent.
	MountOptions []MountOption
	// AuthMode controls access to Sandbox ports. Empty preserves the service default.
	AuthMode SandboxAuthMode
}

// String omits options that may contain credentials or sensitive environment values.
func (o CreateOptions) String() string { return "CreateOptions{REDACTED}" }

// GoString returns the same redacted representation as String.
func (o CreateOptions) GoString() string { return o.String() }

// MountOption configures one storage mount declared by the selected Tool.
type MountOption struct {
	// Name is the storage configuration name in the Tool and is required.
	Name string
	// MountPath optionally overrides the instance-local mount point.
	MountPath string
	// SubPath optionally selects a path inside the backing storage.
	SubPath string
	// ReadOnly optionally overrides the Tool's read/write setting. Nil preserves it.
	ReadOnly *bool
}

// SandboxAuthMode controls authentication for Sandbox data-plane ports. It is unrelated to
// Tencent Cloud credentials used by Client.
type SandboxAuthMode string

const (
	// SandboxAuthDefault uses the service default, currently token authentication.
	SandboxAuthDefault SandboxAuthMode = "DEFAULT"
	// SandboxAuthToken requires the Sandbox instance token on all ports.
	SandboxAuthToken SandboxAuthMode = "TOKEN"
	// SandboxAuthNone disables authentication on all Sandbox ports.
	SandboxAuthNone SandboxAuthMode = "NONE"
	// SandboxAuthPublic requires the instance token on the envd management port and exposes
	// other configured ports without it.
	SandboxAuthPublic SandboxAuthMode = "PUBLIC"
)

// SandboxListOptions selects one server-side page using Cloud offsets.
type SandboxListOptions struct {
	// ToolID filters by exact tool identity.
	ToolID string
	// Offset is a nonnegative Cloud position. Limit is 1..100; zero
	// defaults to 20.
	Offset, Limit int
	// States intersects with Metadata. Empty preserves the service default.
	States []SandboxState
	// Metadata uses exact values across keys (AND). Multi-value OR is not yet
	// available in the current service and is rejected before HTTP.
	Metadata map[string][]string
}

// SandboxPage preserves the service-reported page and total without client-side filtering or
// invented lifecycle states.
type SandboxPage struct {
	// Items contains the returned native or projected sandbox descriptions.
	Items []SandboxInfo
	// TotalCount is the server's count for the query, not merely len(Items).
	TotalCount int
	// NextOffset is the next Cloud offset, or nil when no continuation is reported.
	NextOffset *int
}

// SandboxState is the native lifecycle state.
type SandboxState string

const (
	// Creating means the instance is being provisioned.
	Creating SandboxState = "CREATING"
	// Running means the service reports the instance running; data-plane readiness is checked
	// separately.
	Running SandboxState = "RUNNING"
	// Pausing means pause is in progress.
	Pausing SandboxState = "PAUSING"
	// Paused means the service reports the instance paused.
	Paused SandboxState = "PAUSED"
	// Resuming means resume is in progress.
	Resuming SandboxState = "RESUMING"
	// Stopping means termination is in progress.
	Stopping SandboxState = "STOPPING"
	// Stopped is a terminal Native state. Explicit List filters preserve it and propagate service
	// restrictions.
	Stopped SandboxState = "STOPPED"
	// Failed is the service's failure state.
	Failed SandboxState = "FAILED"
	// Unknown preserves an unavailable or unrecognized state.
	Unknown SandboxState = "UNKNOWN"
)

// SandboxInfo is a control-plane description.
type SandboxInfo struct {
	// ID is the sandbox identity; ToolID and ToolName describe the tool when reported.
	ID, ToolID, ToolName string
	// State preserves the available native state; Unknown means no specific
	// state is established.
	State SandboxState
	// CreatedAt and ExpiresAt are reported timestamps; zero values represent unavailable timestamps.
	CreatedAt, ExpiresAt time.Time
	// Metadata contains returned labels, not creation Env or runtime environment values.
	Metadata map[string]string
}

// PauseMode selects memory or disk pause as supported by the service.
type PauseMode string

const (
	// PauseMemory requests an in-memory pause.
	PauseMemory PauseMode = "MEMORY"
	// PauseDisk requests disk-backed pause without promising ephemeral-file retention.
	PauseDisk PauseMode = "DISK"
)

// PauseOptions chooses a supported pause mode.
type PauseOptions struct {
	// Mode defaults to PauseMemory when empty. PauseDisk does not promise ephemeral-file retention.
	Mode PauseMode
}

// ResumeOptions controls remote lifetime on resume, separately from the caller's context deadline.
type ResumeOptions struct {
	// Timeout requests a resumed lifetime; nil preserves the service default. Service validation
	// and restrictions apply.
	Timeout *time.Duration
}

// SandboxUser selects a logical runtime account, not an SDK-enforced filesystem authorization
// boundary.
type SandboxUser string

const (
	// User selects the runtime's user account and is the SDK default.
	User SandboxUser = "USER"
	// Root selects the runtime's root account; service enforcement still applies.
	Root SandboxUser = "ROOT"
)

// ReadOptions selects the logical runtime user for a streamed read.
type ReadOptions struct {
	// User defaults to User when empty. Runtime enforcement, not the SDK, defines filesystem
	// authorization.
	User SandboxUser
}

// WriteOptions selects the logical runtime user for a streamed remote write.
type WriteOptions struct {
	// User defaults to User when empty; this is not an SDK-enforced authorization boundary.
	User SandboxUser
}

// FileListOptions configures a filesystem listing, not sandbox control-plane pagination.
type FileListOptions struct {
	// User defaults to User when empty.
	User SandboxUser
	// Depth is nonnegative; zero defaults to 1. Recursive traversal semantics belong to the runtime.
	Depth int
}

// WatchOptions configures a generation-bound watch with bounded local buffering.
type WatchOptions struct {
	// User defaults to User when empty.
	User SandboxUser
	// Recursive requests descendant paths. IncludeEntry performs best-effort metadata lookup on
	// non-remove events; it does not make the event and metadata atomic.
	Recursive, IncludeEntry bool
	// Buffer is the pending-event capacity; zero defaults to DefaultWatchBuffer. Negative values
	// are invalid; overflow fails with ResourceExhausted.
	Buffer int
}

// FileInfo contains runtime-reported metadata. Missing type or link information must not be
// interpreted as a regular file.
type FileInfo struct {
	// Name is the reported entry name.
	Name string
	// Path is the reported entry path.
	Path string
	// Type is File, Directory, or UnknownFileType when absent or unrecognized.
	Type FileType
	// Size is the reported byte count; a wire default does not prove field presence.
	Size int64
	// Mode contains runtime mode bits.
	Mode uint32
	// Permissions is runtime permission text and may be empty when not returned.
	Permissions string
	// Owner and Group preserve the runtime-reported account names. Empty means unavailable.
	Owner, Group string
	// ModifiedAt is the reported modification timestamp; zero means unavailable.
	ModifiedAt time.Time
	// SymlinkTarget is nil when not reported; the SDK does not resolve a missing target.
	SymlinkTarget *string
}

// FileType preserves known file/directory types and UnknownFileType for missing runtime
// information.
type FileType string

const (
	// UnknownFileType preserves omitted or unrecognized runtime file types.
	UnknownFileType FileType = "UNKNOWN"
	// File means the runtime explicitly reported a file.
	File FileType = "FILE"
	// Directory means the runtime explicitly reported a directory.
	Directory FileType = "DIRECTORY"
)

// FileEventType describes a runtime filesystem notification without guaranteeing durable
// delivery.
type FileEventType string

const (
	// FileCreate reports entry creation.
	FileCreate FileEventType = "CREATE"
	// FileWrite reports content modification.
	FileWrite FileEventType = "WRITE"
	// FileRemove reports removal; metadata may no longer be available.
	FileRemove FileEventType = "REMOVE"
	// FileRename reports a rename without guaranteeing the old path is supplied.
	FileRename FileEventType = "RENAME"
	// FileChmod reports a permission change.
	FileChmod FileEventType = "CHMOD"
)

// FileEvent is a runtime notification, not a durable or atomically enriched change-log entry.
type FileEvent struct {
	// WatchID is a stable SDK-local identity, not a server resume token.
	WatchID string
	// Sequence increases within this local watch, not across watchers or reconnections.
	Sequence uint64
	// Type identifies the reported change.
	Type FileEventType
	// Path is the affected path reported by the runtime.
	Path string
	// OldPath is nil when unavailable; the current wire contract does not supply a rename source.
	OldPath *string
	// Entry is optional best-effort metadata; remove events and failed lookups leave it nil.
	Entry *FileInfo
}

// CommandOptions configures one foreground invocation. Its environment is per-process, not
// persistent sandbox configuration.
type CommandOptions struct {
	// Args is passed directly to the runtime command; nil means no arguments.
	Args []string
	// Env supplies per-process variables; nil supplies no overrides. Do not mutate the map during
	// Run.
	Env map[string]string
	// Cwd selects the process working directory; empty preserves runtime defaults.
	Cwd string
	// User selects the runtime account; empty defaults to User.
	User SandboxUser
	// MaxOutputBytes bounds stdout and stderr independently; zero selects DefaultMaxOutputBytes
	// and negative values are invalid.
	MaxOutputBytes int64
}

// CommandResult contains a terminal exit and bounded byte output. A nonzero exit code is data,
// not a transport error.
type CommandResult struct {
	// ExitCode is the runtime-reported process exit code.
	ExitCode int
	// Stdout contains retained standard-output bytes; use the producing process's encoding to decode.
	Stdout []byte
	// Stderr contains independently retained standard-error bytes.
	Stderr []byte
	// StdoutTruncated indicates output exceeded its retention limit.
	StdoutTruncated bool
	// StderrTruncated indicates error output exceeded its independent retention limit.
	StderrTruncated bool
}

// PTYOptions configures an interactive runtime process. Callers must supply positive Cols and
// Rows.
type PTYOptions struct {
	// Command is the required runtime command.
	Command string
	// Args supplies command arguments directly; nil means none.
	Args []string
	// Env supplies per-process variables, not persistent sandbox configuration.
	Env map[string]string
	// Cwd selects the working directory; empty preserves runtime defaults.
	Cwd string
	// User defaults to User when empty.
	User SandboxUser
	// Cols is the required positive terminal width in character cells.
	Cols uint32
	// Rows is the required positive terminal height in character cells.
	Rows uint32
}

// PTYEventType identifies start, output or terminal end within one PTY stream.
type PTYEventType string

const (
	// PTYStart reports that the start barrier was observed.
	PTYStart PTYEventType = "START"
	// PTYOutput carries terminal bytes.
	PTYOutput PTYEventType = "OUTPUT"
	// PTYEnd reports terminal process status.
	PTYEnd PTYEventType = "END"
)

// PTYEvent carries one terminal notification; output is not separated into stdout and stderr.
type PTYEvent struct {
	// Type identifies start, output or terminal end.
	Type PTYEventType
	// Data contains terminal output bytes; start/end notifications may have no bytes.
	Data []byte
	// Exit is populated for PTYEnd when the runtime reports a terminal result.
	Exit *ExitStatus
}

// ExitStatus preserves runtime termination information without guessing signal numbers from
// diagnostic text.
type ExitStatus struct {
	// Code is the reported process exit code.
	Code int
	// Exited is the runtime's exited flag, not an SDK success predicate.
	Exited bool
	// Reason is the normalized reason, potentially UNKNOWN when the runtime is ambiguous.
	Reason ExitReason
	// Message is runtime status text, not a stable machine-readable error contract.
	Message string
}

// ExitReason preserves the runtime's normalized reason, including UNKNOWN when ambiguous.
type ExitReason string

// dataPlane is internal-facing: public methods never reveal frames, tokens, or domains.
type dataPlane interface {
	Read(context.Context, string, string) (io.ReadCloser, error)
	Write(context.Context, string, io.Reader, string) (FileInfo, error)
	List(context.Context, string, int, string) ([]FileInfo, error)
	Watch(context.Context, string, WatchOptions) (*WatchHandle, error)
	Run(context.Context, string, CommandOptions) (CommandResult, error)
	OpenPTY(context.Context, PTYOptions) (*PTYSession, error)
	Ready(context.Context) error
	Close() error
}
type controlPlane interface {
	Create(context.Context, CreateOptions) (SandboxInfo, error)
	Connect(context.Context, string, time.Duration) (SandboxInfo, error)
	Get(context.Context, string) (SandboxInfo, error)
	List(context.Context, SandboxListOptions) (SandboxPage, error)
	Pause(context.Context, string, PauseOptions) (SandboxInfo, error)
	Resume(context.Context, string, ResumeOptions) (SandboxInfo, error)
	Delete(context.Context, string) error
	dataPlane(context.Context, string) (dataPlane, error)
}
