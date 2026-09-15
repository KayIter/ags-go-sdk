package ags

import (
	"context"
	"strings"
	"time"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/model"
	internalruntime "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/runtime"
)

const (
	// DefaultMaxCodeEventBytes bounds one newline-delimited event from the Code service.
	DefaultMaxCodeEventBytes = 1 << 20
	// DefaultMaxCodeOutputBytes bounds retained stdout and stderr independently.
	DefaultMaxCodeOutputBytes int64 = 4 << 20
	// DefaultMaxCodeResultBytes bounds retained serialized result events.
	DefaultMaxCodeResultBytes int64 = 4 << 20
	// DefaultMaxCodeEvents bounds retained log and result event counts.
	DefaultMaxCodeEvents = 4096
	// DefaultCodeCallbackTimeout bounds observation of one user callback.
	DefaultCodeCallbackTimeout = time.Second
	codeCallbackQueueEvents    = 32
	codeCallbackQueueBytes     = 1 << 20
)

// Code executes source and manages execution contexts in one Sandbox generation.
type Code struct{ sandbox *Sandbox }

// CodeContextRef is a sealed reference accepted by RunCodeOptions. Construct one through
// CreateContext or NewExternalCodeContextRef.
type CodeContextRef interface{ codeContextRef() }

// CodeContext is a managed execution context bound to one Sandbox and generation. Its private
// identity prevents reconstruction from an ID or transfer between Sandbox handles.
type CodeContext struct {
	inner *internalruntime.CodeContext
}

func (*CodeContext) codeContextRef() {}

// ID returns the service-issued context ID.
func (c *CodeContext) ID() string { return c.inner.Value().ID }

// Language returns the runtime language reported for this context.
func (c *CodeContext) Language() string { return c.inner.Value().Language }

// CWD returns the working directory reported for this context.
func (c *CodeContext) CWD() string { return c.inner.Value().CWD }

// ExternalCodeContextRef explicitly represents a service-issued context ID whose ownership,
// existence, and generation validity can only be checked by the remote service.
type ExternalCodeContextRef struct {
	inner *internalruntime.ExternalCodeContext
}

func (*ExternalCodeContextRef) codeContextRef() {}

// NewExternalCodeContextRef validates and wraps an existing service context ID. It does not
// prove that the context exists or belongs to a Sandbox.
func NewExternalCodeContextRef(id string) (*ExternalCodeContextRef, error) {
	if id == "" || strings.TrimSpace(id) != id || len(id) > 256 || strings.ContainsRune(id, 0) {
		return nil, codeError(InvalidArgument, "NewExternalCodeContextRef", "CODE_CONTEXT_ID_INVALID")
	}
	return &ExternalCodeContextRef{inner: internalruntime.NewExternalCodeContext(id)}, nil
}

// ID returns the caller-supplied external context ID.
func (r *ExternalCodeContextRef) ID() string { return r.inner.ID() }

// CreateCodeContextOptions configures a new context. Empty values default to python and
// /home/user respectively.
type CreateCodeContextOptions struct {
	// Language selects the runtime language; empty defaults to python.
	Language string
	// CWD selects the context working directory; empty defaults to /home/user.
	CWD string
}

// RunCodeOptions configures one execution. Language defaults to python when Context is nil.
// Language and Context are mutually exclusive.
type RunCodeOptions struct {
	// Language selects the runtime language when Context is nil; empty defaults to python.
	Language string
	// Context selects a managed or explicit external execution context. It is mutually exclusive
	// with Language.
	Context CodeContextRef
	// Env supplies variables for this execution. The SDK copies the map before submission.
	Env map[string]string

	// MaxEventBytes bounds one wire event; zero selects DefaultMaxCodeEventBytes.
	MaxEventBytes int
	// MaxOutputBytes bounds retained stdout and stderr independently; zero selects the default.
	MaxOutputBytes int64
	// MaxResultBytes bounds retained serialized result data; zero selects the default.
	MaxResultBytes int64
	// MaxEvents bounds retained log and result event counts; zero selects the default.
	MaxEvents int
	// CallbackTimeout bounds one user callback; zero selects DefaultCodeCallbackTimeout.
	CallbackTimeout time.Duration
}

// CodeCallbacks observes live events without owning protocol consumption. A slow or panicking
// callback disables observation but does not stop bounded result aggregation.
type CodeCallbacks struct {
	// OnStdout observes one standard-output fragment.
	OnStdout func(string)
	// OnStderr observes one standard-error fragment.
	OnStderr func(string)
	// OnResult observes one rich result without owning internal aggregation.
	OnResult func(CodeResult)
}

// CodeExecutionError is a remote language/runtime error reported as execution data.
type CodeExecutionError struct {
	// Name is the runtime-reported error class.
	Name string
	// Value is the runtime-reported error message.
	Value string
	// Traceback is the runtime-reported diagnostic stack and may be empty.
	Traceback string
}

// CodeResult preserves the supported rich result representations.
type CodeResult struct {
	// Text contains a plain-text representation when reported.
	Text *string `json:"text,omitempty"`
	// HTML contains an HTML representation when reported.
	HTML *string `json:"html,omitempty"`
	// Markdown contains a Markdown representation when reported.
	Markdown *string `json:"markdown,omitempty"`
	// SVG contains an SVG representation when reported.
	SVG *string `json:"svg,omitempty"`
	// PNG contains a PNG representation, encoded as reported by the runtime.
	PNG *string `json:"png,omitempty"`
	// JPEG contains a JPEG representation, encoded as reported by the runtime.
	JPEG *string `json:"jpeg,omitempty"`
	// PDF contains a PDF representation, encoded as reported by the runtime.
	PDF *string `json:"pdf,omitempty"`
	// Latex contains a LaTeX representation when reported.
	Latex *string `json:"latex,omitempty"`
	// JavaScript contains a JavaScript representation when reported.
	JavaScript *string `json:"javascript,omitempty"`
	// JSON contains structured JSON data when reported.
	JSON map[string]any `json:"json,omitempty"`
	// Data contains runtime-specific structured data when reported.
	Data map[string]any `json:"data,omitempty"`
	// Chart contains runtime-specific chart data when reported.
	Chart map[string]any `json:"chart,omitempty"`
	// Extra contains additional runtime metadata when reported.
	Extra map[string]any `json:"extra,omitempty"`
	// IsMainResult reports whether the runtime marked this as the primary result.
	IsMainResult bool `json:"is_main_result"`
}

// CodeExecution contains bounded aggregates. A remote execution error appears in Error; local
// transport, cancellation, generation, and protocol failures are returned as Go errors.
type CodeExecution struct {
	// Results contains retained rich results.
	Results []CodeResult
	// Stdout contains retained standard-output fragments.
	Stdout []string
	// Stderr contains retained standard-error fragments.
	Stderr []string
	// Error contains a remote execution failure; local failures are returned as Go errors.
	Error *CodeExecutionError
	// ExecutionCount is the runtime-reported execution ordinal when available.
	ExecutionCount *int

	// StdoutTruncated reports that retained stdout reached its configured limit.
	StdoutTruncated bool
	// StderrTruncated reports that retained stderr reached its configured limit.
	StderrTruncated bool
	// ResultsTruncated reports that retained result bytes reached their configured limit.
	ResultsTruncated bool
	// EventsTruncated reports that retained event count reached its configured limit.
	EventsTruncated bool
	// CallbackError reports callback panic, timeout, or queue overflow without discarding the
	// independently aggregated execution result.
	CallbackError *Error
}

// CreateContext creates a managed context bound to the current Sandbox generation.
func (c *Code) CreateContext(ctx context.Context, opts CreateCodeContextOptions) (*CodeContext, error) {
	if opts.Language == "" {
		opts.Language = "python"
	}
	if opts.CWD == "" {
		opts.CWD = "/home/user"
	}
	if strings.ContainsRune(opts.Language, 0) || strings.ContainsRune(opts.CWD, 0) {
		return nil, codeError(InvalidArgument, "Code.CreateContext", "CODE_CONTEXT_OPTIONS_INVALID")
	}
	plane, err := c.runtime("Code.CreateContext")
	if err != nil {
		return nil, err
	}
	inner, err := plane.generation().CreateCodeContext(ctx, c, opts.Language, opts.CWD, DefaultMaxCodeEventBytes)
	if err != nil {
		return nil, normalizeError("Code.CreateContext", err)
	}
	return &CodeContext{inner: inner}, nil
}

// Run executes source once, consumes the complete local response, and returns bounded
// aggregates. Canceling ctx or invalidating the Sandbox generation stops local observation but
// does not prove that the remote execution stopped.
func (c *Code) Run(ctx context.Context, source string, opts RunCodeOptions, callbacks CodeCallbacks) (*CodeExecution, error) {
	if source == "" {
		return nil, codeError(InvalidArgument, "Code.Run", "CODE_REQUIRED")
	}
	if opts.Context != nil && opts.Language != "" {
		return nil, codeError(InvalidArgument, "Code.Run", "LANGUAGE_AND_CONTEXT_MUTUALLY_EXCLUSIVE")
	}
	applyCodeDefaults(&opts)
	if err := validateRunCodeOptions(opts); err != nil {
		return nil, err
	}
	plane, err := c.runtime("Code.Run")
	if err != nil {
		return nil, err
	}
	contextID, release, err := c.acquireContext(opts.Context, plane.generation())
	if err != nil {
		return nil, normalizeError("Code.Run", err)
	}
	defer release()
	privateCallbacks := model.CodeCallbacks{OnStdout: callbacks.OnStdout, OnStderr: callbacks.OnStderr}
	if callbacks.OnResult != nil {
		privateCallbacks.OnResult = func(value model.CodeResult) { callbacks.OnResult(mapCodeResult(value)) }
	}
	value, err := plane.generation().RunCode(ctx, model.CodeRequest{Source: source, ContextID: contextID, Language: opts.Language, Env: cloneStringMap(opts.Env), MaxEventBytes: opts.MaxEventBytes}, model.CodeLimits{MaxOutputBytes: opts.MaxOutputBytes, MaxResultBytes: opts.MaxResultBytes, MaxEvents: opts.MaxEvents, CallbackTimeout: opts.CallbackTimeout}, privateCallbacks)
	if err != nil {
		return nil, normalizeError("Code.Run", err)
	}
	return mapCodeExecution(value), nil
}

func (c *Code) runtime(operation string) (*runtimeDataPlane, error) {
	plane, err := c.sandbox.dataPlane(operation)
	if err != nil {
		return nil, err
	}
	transport, ok := plane.(*runtimeDataPlane)
	if !ok {
		return nil, codeError(Unsupported, operation, "CODE_UNAVAILABLE")
	}
	return transport, nil
}

func (c *Code) acquireContext(ref CodeContextRef, generation *internalruntime.Generation) (string, func(), error) {
	if ref == nil {
		return "", func() {}, nil
	}
	switch value := ref.(type) {
	case *CodeContext:
		if value == nil {
			return "", nil, codeError(InvalidArgument, "Code.Run", "CODE_CONTEXT_REQUIRED")
		}
		return value.inner.Acquire(c, generation)
	case *ExternalCodeContextRef:
		if value == nil {
			return "", nil, codeError(InvalidArgument, "Code.Run", "CODE_CONTEXT_REQUIRED")
		}
		return value.inner.Acquire(c)
	default:
		return "", nil, codeError(InvalidArgument, "Code.Run", "CODE_CONTEXT_INVALID")
	}
}

func applyCodeDefaults(opts *RunCodeOptions) {
	if opts.Context == nil && opts.Language == "" {
		opts.Language = "python"
	}
	if opts.MaxEventBytes == 0 {
		opts.MaxEventBytes = DefaultMaxCodeEventBytes
	}
	if opts.MaxOutputBytes == 0 {
		opts.MaxOutputBytes = DefaultMaxCodeOutputBytes
	}
	if opts.MaxResultBytes == 0 {
		opts.MaxResultBytes = DefaultMaxCodeResultBytes
	}
	if opts.MaxEvents == 0 {
		opts.MaxEvents = DefaultMaxCodeEvents
	}
	if opts.CallbackTimeout == 0 {
		opts.CallbackTimeout = DefaultCodeCallbackTimeout
	}
}

func validateRunCodeOptions(opts RunCodeOptions) error {
	if opts.MaxEventBytes < 1 || opts.MaxEventBytes > 8<<20 || opts.MaxOutputBytes < 1 || opts.MaxResultBytes < 1 || opts.MaxEvents < 1 || opts.MaxEvents > 1<<20 || opts.CallbackTimeout < 0 || opts.CallbackTimeout > 30*time.Second {
		return codeError(InvalidArgument, "Code.Run", "CODE_LIMIT_INVALID")
	}
	for key, value := range opts.Env {
		if !createEnvKey.MatchString(key) || strings.ContainsRune(value, 0) {
			return codeError(InvalidArgument, "Code.Run", "ENV_INVALID")
		}
	}
	return nil
}

func mapCodeResult(value model.CodeResult) CodeResult {
	return CodeResult{Text: value.Text, HTML: value.HTML, Markdown: value.Markdown, SVG: value.SVG, PNG: value.PNG, JPEG: value.JPEG, PDF: value.PDF, Latex: value.Latex, JavaScript: value.JavaScript, JSON: value.JSON, Data: value.Data, Chart: value.Chart, Extra: value.Extra, IsMainResult: value.IsMainResult}
}

func mapCodeExecution(value model.CodeExecution) *CodeExecution {
	out := &CodeExecution{Stdout: value.Stdout, Stderr: value.Stderr, ExecutionCount: value.ExecutionCount, StdoutTruncated: value.StdoutTruncated, StderrTruncated: value.StderrTruncated, ResultsTruncated: value.ResultsTruncated, EventsTruncated: value.EventsTruncated}
	for _, result := range value.Results {
		out.Results = append(out.Results, mapCodeResult(result))
	}
	if value.Error != nil {
		out.Error = &CodeExecutionError{Name: value.Error.Name, Value: value.Error.Value, Traceback: value.Error.Traceback}
	}
	if value.CallbackError != nil {
		mapped := normalizeError(value.CallbackError.Operation, value.CallbackError)
		if failure, ok := mapped.(*Error); ok {
			out.CallbackError = failure
		}
	}
	return out
}
