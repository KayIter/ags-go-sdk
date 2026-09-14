package ags

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
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
	id, language, cwd string
	owner             *Code
	generation        dataPlane
	mu                sync.Mutex
	active            bool
}

func (*CodeContext) codeContextRef() {}

// ID returns the service-issued context ID.
func (c *CodeContext) ID() string { return c.id }

// Language returns the runtime language reported for this context.
func (c *CodeContext) Language() string { return c.language }

// CWD returns the working directory reported for this context.
func (c *CodeContext) CWD() string { return c.cwd }

// ExternalCodeContextRef explicitly represents a service-issued context ID whose ownership,
// existence, and generation validity can only be checked by the remote service.
type ExternalCodeContextRef struct {
	id     string
	mu     sync.Mutex
	active map[*Code]bool
}

func (*ExternalCodeContextRef) codeContextRef() {}

// NewExternalCodeContextRef validates and wraps an existing service context ID. It does not
// prove that the context exists or belongs to a Sandbox.
func NewExternalCodeContextRef(id string) (*ExternalCodeContextRef, error) {
	if id == "" || strings.TrimSpace(id) != id || len(id) > 256 || strings.ContainsRune(id, 0) {
		return nil, codeError(InvalidArgument, "NewExternalCodeContextRef", "CODE_CONTEXT_ID_INVALID")
	}
	return &ExternalCodeContextRef{id: id, active: map[*Code]bool{}}, nil
}

// ID returns the caller-supplied external context ID.
func (r *ExternalCodeContextRef) ID() string { return r.id }

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

type createCodeContextRequest struct {
	Language string `json:"language,omitempty"`
	CWD      string `json:"cwd,omitempty"`
}

type createCodeContextResponse struct {
	ID       string `json:"id"`
	Language string `json:"language"`
	CWD      string `json:"cwd"`
}

type runCodeRequest struct {
	Code      string            `json:"code"`
	ContextID string            `json:"context_id,omitempty"`
	Language  string            `json:"language,omitempty"`
	Env       map[string]string `json:"env_vars,omitempty"`
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
	plane, transport, err := c.transport("Code.CreateContext")
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(createCodeContextRequest{Language: opts.Language, CWD: opts.CWD})
	if err != nil {
		return nil, normalizeError("Code.CreateContext", err)
	}
	body, err := transport.codeRequest(ctx, "Code.CreateContext", "/contexts", payload)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	limited := io.LimitReader(body, DefaultMaxCodeEventBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, normalizeError("Code.CreateContext", err)
	}
	if len(data) > DefaultMaxCodeEventBytes {
		return nil, codeError(ResourceExhausted, "Code.CreateContext", "CODE_CONTEXT_RESPONSE_TOO_LARGE")
	}
	var response createCodeContextResponse
	if err = json.Unmarshal(data, &response); err != nil {
		return nil, codeError(Protocol, "Code.CreateContext", "CODE_CONTEXT_RESPONSE_INVALID")
	}
	if response.ID == "" {
		return nil, codeError(Protocol, "Code.CreateContext", "CODE_CONTEXT_ID_MISSING")
	}
	return &CodeContext{id: response.ID, language: response.Language, cwd: response.CWD, owner: c, generation: plane}, nil
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
	plane, transport, err := c.transport("Code.Run")
	if err != nil {
		return nil, err
	}
	contextID, release, err := c.acquireContext(opts.Context, plane)
	if err != nil {
		return nil, err
	}
	defer release()
	env := cloneStringMap(opts.Env)
	payload, err := json.Marshal(runCodeRequest{Code: source, ContextID: contextID, Language: opts.Language, Env: env})
	if err != nil {
		return nil, normalizeError("Code.Run", err)
	}
	body, err := transport.codeRequest(ctx, "Code.Run", "/execute", payload)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	execution := &CodeExecution{}
	observer := newCodeObserver(callbacks, opts.CallbackTimeout)
	defer func() { execution.CallbackError = observer.finish() }()
	if err = consumeCodeEvents(body, execution, opts, observer); err != nil {
		return nil, err
	}
	return execution, nil
}

func (c *Code) transport(operation string) (dataPlane, codeTransport, error) {
	plane, err := c.sandbox.dataPlane(operation)
	if err != nil {
		return nil, nil, err
	}
	transport, ok := plane.(codeTransport)
	if !ok {
		return nil, nil, codeError(Unsupported, operation, "CODE_UNAVAILABLE")
	}
	return plane, transport, nil
}

func (c *Code) acquireContext(ref CodeContextRef, plane dataPlane) (string, func(), error) {
	if ref == nil {
		return "", func() {}, nil
	}
	switch value := ref.(type) {
	case *CodeContext:
		if value == nil {
			return "", nil, codeError(InvalidArgument, "Code.Run", "CODE_CONTEXT_REQUIRED")
		}
		value.mu.Lock()
		defer value.mu.Unlock()
		if value.owner != c {
			return "", nil, codeError(Conflict, "Code.Run", "CODE_CONTEXT_OWNER_MISMATCH")
		}
		if value.generation != plane {
			return "", nil, codeError(Conflict, "Code.Run", "CODE_CONTEXT_INVALIDATED")
		}
		if value.active {
			return "", nil, codeError(Conflict, "Code.Run", "CODE_CONTEXT_BUSY")
		}
		value.active = true
		return value.id, func() { value.mu.Lock(); value.active = false; value.mu.Unlock() }, nil
	case *ExternalCodeContextRef:
		if value == nil {
			return "", nil, codeError(InvalidArgument, "Code.Run", "CODE_CONTEXT_REQUIRED")
		}
		value.mu.Lock()
		defer value.mu.Unlock()
		if value.active[c] {
			return "", nil, codeError(Conflict, "Code.Run", "CODE_CONTEXT_BUSY")
		}
		value.active[c] = true
		return value.id, func() { value.mu.Lock(); delete(value.active, c); value.mu.Unlock() }, nil
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

func consumeCodeEvents(body io.Reader, out *CodeExecution, opts RunCodeOptions, observer *codeObserver) error {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 64<<10), opts.MaxEventBytes)
	var stdoutBytes, stderrBytes, resultBytes int64
	eventCount := 0
	ended := false
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		var envelope struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(line, &envelope); err != nil {
			return &Error{Code: Protocol, Operation: "Code.Run", Reason: "CODE_EVENT_INVALID", Cause: err}
		}
		if envelope.Type == "" {
			return codeError(Protocol, "Code.Run", "CODE_EVENT_TYPE_MISSING")
		}
		if ended {
			return codeError(Protocol, "Code.Run", "CODE_EVENT_AFTER_END")
		}
		eventCount++
		if eventCount > opts.MaxEvents {
			out.EventsTruncated = true
		}
		switch envelope.Type {
		case "keepalive":
		case "end_of_execution":
			ended = true
		case "stdout", "stderr":
			var event struct {
				Text *string `json:"text"`
			}
			if err := json.Unmarshal(line, &event); err != nil || event.Text == nil {
				return codeError(Protocol, "Code.Run", "CODE_LOG_EVENT_INVALID")
			}
			retained, truncated := retainCodeText(*event.Text, opts.MaxOutputBytes, map[bool]*int64{true: &stdoutBytes, false: &stderrBytes}[envelope.Type == "stdout"])
			if envelope.Type == "stdout" {
				out.StdoutTruncated = out.StdoutTruncated || truncated || eventCount > opts.MaxEvents
				if eventCount <= opts.MaxEvents && retained != "" {
					out.Stdout = append(out.Stdout, retained)
				}
				observer.enqueue(codeCallbackEvent{kind: "stdout", text: *event.Text, size: len(line)})
			} else {
				out.StderrTruncated = out.StderrTruncated || truncated || eventCount > opts.MaxEvents
				if eventCount <= opts.MaxEvents && retained != "" {
					out.Stderr = append(out.Stderr, retained)
				}
				observer.enqueue(codeCallbackEvent{kind: "stderr", text: *event.Text, size: len(line)})
			}
		case "result":
			var result CodeResult
			if err := json.Unmarshal(line, &result); err != nil {
				return codeError(Protocol, "Code.Run", "CODE_RESULT_INVALID")
			}
			if eventCount > opts.MaxEvents || resultBytes+int64(len(line)) > opts.MaxResultBytes {
				out.ResultsTruncated = true
			} else {
				resultBytes += int64(len(line))
				out.Results = append(out.Results, result)
			}
			observer.enqueue(codeCallbackEvent{kind: "result", result: result, size: len(line)})
		case "error":
			var event struct {
				Name      string `json:"name"`
				Value     string `json:"value"`
				Traceback string `json:"traceback"`
			}
			if err := json.Unmarshal(line, &event); err != nil {
				return codeError(Protocol, "Code.Run", "CODE_ERROR_EVENT_INVALID")
			}
			out.Error = &CodeExecutionError{Name: event.Name, Value: event.Value, Traceback: event.Traceback}
		case "number_of_executions":
			var event struct {
				ExecutionCount *int `json:"execution_count"`
			}
			if err := json.Unmarshal(line, &event); err != nil || event.ExecutionCount == nil {
				return codeError(Protocol, "Code.Run", "CODE_EXECUTION_COUNT_INVALID")
			}
			count := *event.ExecutionCount
			out.ExecutionCount = &count
		default:
			return &Error{Code: Protocol, Operation: "Code.Run", Reason: "CODE_EVENT_UNKNOWN", Cause: fmt.Errorf("unknown Code event type %q", envelope.Type)}
		}
	}
	if err := scanner.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) || strings.Contains(err.Error(), "token too long") {
			return codeError(ResourceExhausted, "Code.Run", "CODE_EVENT_TOO_LARGE")
		}
		return normalizeError("Code.Run", err)
	}
	return nil
}

func retainCodeText(value string, limit int64, used *int64) (string, bool) {
	remaining := limit - *used
	if remaining <= 0 {
		return "", value != ""
	}
	if int64(len(value)) <= remaining {
		*used += int64(len(value))
		return value, false
	}
	*used = limit
	return value[:remaining], true
}

type codeCallbackEvent struct {
	kind   string
	text   string
	result CodeResult
	size   int
}

type codeObserver struct {
	callbacks CodeCallbacks
	timeout   time.Duration
	queue     chan codeCallbackEvent
	stop      chan struct{}
	done      chan struct{}
	once      sync.Once
	mu        sync.Mutex
	bytes     int
	enabled   bool
	err       *Error
}

func newCodeObserver(callbacks CodeCallbacks, timeout time.Duration) *codeObserver {
	o := &codeObserver{callbacks: callbacks, timeout: timeout, queue: make(chan codeCallbackEvent, codeCallbackQueueEvents), stop: make(chan struct{}), done: make(chan struct{}), enabled: true}
	go o.dispatch()
	return o
}

func (o *codeObserver) enqueue(event codeCallbackEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.enabled || !o.hasCallback(event.kind) {
		return
	}
	if len(o.queue) >= codeCallbackQueueEvents || o.bytes+event.size > codeCallbackQueueBytes {
		o.failLocked("CODE_CALLBACK_BUFFER_FULL")
		return
	}
	select {
	case o.queue <- event:
		o.bytes += event.size
	default:
		o.failLocked("CODE_CALLBACK_BUFFER_FULL")
	}
}

func (o *codeObserver) hasCallback(kind string) bool {
	return kind == "stdout" && o.callbacks.OnStdout != nil || kind == "stderr" && o.callbacks.OnStderr != nil || kind == "result" && o.callbacks.OnResult != nil
}

func (o *codeObserver) failLocked(reason string) {
	if !o.enabled {
		return
	}
	o.enabled = false
	o.err = &Error{Code: ResourceExhausted, Operation: "Code.callbacks", Reason: reason}
	o.once.Do(func() { close(o.stop) })
}

func (o *codeObserver) dispatch() {
	defer close(o.done)
	for {
		select {
		case <-o.stop:
			return
		case event, ok := <-o.queue:
			if !ok {
				return
			}
			o.mu.Lock()
			o.bytes -= event.size
			o.mu.Unlock()
			result := make(chan string, 1)
			go func() {
				reason := ""
				defer func() {
					if recover() != nil {
						reason = "CODE_CALLBACK_PANIC"
					}
					result <- reason
				}()
				switch event.kind {
				case "stdout":
					o.callbacks.OnStdout(event.text)
				case "stderr":
					o.callbacks.OnStderr(event.text)
				case "result":
					o.callbacks.OnResult(event.result)
				}
			}()
			select {
			case reason := <-result:
				if reason != "" {
					o.mu.Lock()
					o.failLocked(reason)
					o.mu.Unlock()
					return
				}
			case <-time.After(o.timeout):
				o.mu.Lock()
				o.failLocked("CODE_CALLBACK_TIMEOUT")
				o.mu.Unlock()
				return
			case <-o.stop:
				return
			}
		}
	}
}

func (o *codeObserver) finish() *Error {
	close(o.queue)
	<-o.done
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.err
}
