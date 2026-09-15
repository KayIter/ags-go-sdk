package runtime

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/dataplane"
	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/model"
)

const (
	codeCallbackQueueEvents = 32
	codeCallbackQueueBytes  = 1 << 20
)

// CodeContext owns managed context identity, generation, and busy state.
type CodeContext struct {
	value      model.CodeContext
	owner      any
	generation *Generation
	mu         sync.Mutex
	active     bool
}

func (c *CodeContext) Value() model.CodeContext { return c.value }

func (c *CodeContext) Acquire(owner any, generation *Generation) (string, func(), error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.owner != owner {
		return "", nil, failure(model.Conflict, "Code.Run", "CODE_CONTEXT_OWNER_MISMATCH")
	}
	if c.generation != generation {
		return "", nil, failure(model.Conflict, "Code.Run", "CODE_CONTEXT_INVALIDATED")
	}
	if c.active {
		return "", nil, failure(model.Conflict, "Code.Run", "CODE_CONTEXT_BUSY")
	}
	c.active = true
	return c.value.ID, func() { c.mu.Lock(); c.active = false; c.mu.Unlock() }, nil
}

// ExternalCodeContext owns only local concurrency; the server verifies identity.
type ExternalCodeContext struct {
	id     string
	mu     sync.Mutex
	active map[any]bool
}

func NewExternalCodeContext(id string) *ExternalCodeContext {
	return &ExternalCodeContext{id: id, active: map[any]bool{}}
}
func (c *ExternalCodeContext) ID() string { return c.id }
func (c *ExternalCodeContext) Acquire(owner any) (string, func(), error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active[owner] {
		return "", nil, failure(model.Conflict, "Code.Run", "CODE_CONTEXT_BUSY")
	}
	c.active[owner] = true
	return c.id, func() { c.mu.Lock(); delete(c.active, owner); c.mu.Unlock() }, nil
}

func (g *Generation) CreateCodeContext(ctx context.Context, owner any, language, cwd string, maxResponseBytes int64) (out *CodeContext, err error) {
	ctx, finish, _ := g.requestOperation(ctx)
	defer finish()
	defer func() { err = operationError(ctx, "Code.CreateContext", err) }()
	value, err := g.wire.CreateCodeContext(ctx, language, cwd, maxResponseBytes)
	if err != nil {
		return nil, err
	}
	return &CodeContext{value: model.CodeContext{ID: value.ID, Language: value.Language, CWD: value.CWD}, owner: owner, generation: g}, nil
}

func (g *Generation) RunCode(ctx context.Context, request model.CodeRequest, limits model.CodeLimits, callbacks model.CodeCallbacks) (out model.CodeExecution, err error) {
	ctx, finish, started := g.requestOperation(ctx)
	stream, err := g.wire.StartCode(ctx, dataplane.CodeRequest{Source: request.Source, ContextID: request.ContextID, Language: request.Language, Env: request.Env}, request.MaxEventBytes)
	if err != nil {
		finish()
		return out, operationError(ctx, "Code.Run", err)
	}
	started()
	defer func() {
		closeErr := stream.Close()
		finish()
		if err == nil && closeErr != nil {
			err = normalize("Code.Run", closeErr)
		}
	}()
	observer := newCodeObserver(callbacks, limits.CallbackTimeout)
	defer func() { out.CallbackError = observer.finish() }()
	err = consumeCodeEvents(ctx, stream, &out, limits, observer)
	return out, err
}

func consumeCodeEvents(ctx context.Context, stream *dataplane.CodeStream, out *model.CodeExecution, limits model.CodeLimits, observer *codeObserver) error {
	var stdoutBytes, stderrBytes, resultBytes int64
	eventCount := 0
	for {
		if cause := context.Cause(ctx); cause != nil {
			return normalize("Code.Run", cause)
		}
		event, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return operationError(ctx, "Code.Run", err)
		}
		eventCount++
		if eventCount > limits.MaxEvents {
			out.EventsTruncated = true
		}
		switch event.Kind {
		case dataplane.CodeKeepalive, dataplane.CodeEnd:
		case dataplane.CodeStdout, dataplane.CodeStderr:
			used := &stderrBytes
			if event.Kind == dataplane.CodeStdout {
				used = &stdoutBytes
			}
			retained, truncated := retainText(event.Text, limits.MaxOutputBytes, used)
			if event.Kind == dataplane.CodeStdout {
				out.StdoutTruncated = out.StdoutTruncated || truncated || eventCount > limits.MaxEvents
				if eventCount <= limits.MaxEvents && retained != "" {
					out.Stdout = append(out.Stdout, retained)
				}
				observer.enqueue(callbackEvent{kind: "stdout", text: event.Text, size: event.WireBytes})
			} else {
				out.StderrTruncated = out.StderrTruncated || truncated || eventCount > limits.MaxEvents
				if eventCount <= limits.MaxEvents && retained != "" {
					out.Stderr = append(out.Stderr, retained)
				}
				observer.enqueue(callbackEvent{kind: "stderr", text: event.Text, size: event.WireBytes})
			}
		case dataplane.CodeResultEvent:
			result := codeResult(event.Result)
			if eventCount > limits.MaxEvents || resultBytes+int64(event.WireBytes) > limits.MaxResultBytes {
				out.ResultsTruncated = true
			} else {
				resultBytes += int64(event.WireBytes)
				out.Results = append(out.Results, result)
			}
			observer.enqueue(callbackEvent{kind: "result", result: result, size: event.WireBytes})
		case dataplane.CodeErrorEvent:
			out.Error = &model.CodeExecutionError{Name: event.ExecutionError.Name, Value: event.ExecutionError.Value, Traceback: event.ExecutionError.Traceback}
		case dataplane.CodeExecutionCount:
			count := *event.ExecutionCount
			out.ExecutionCount = &count
		}
	}
}

func codeResult(value dataplane.CodeResult) model.CodeResult {
	return model.CodeResult{Text: value.Text, HTML: value.HTML, Markdown: value.Markdown, SVG: value.SVG, PNG: value.PNG, JPEG: value.JPEG, PDF: value.PDF, Latex: value.Latex, JavaScript: value.JavaScript, JSON: value.JSON, Data: value.Data, Chart: value.Chart, Extra: value.Extra, IsMainResult: value.IsMainResult}
}
func retainText(value string, limit int64, used *int64) (string, bool) {
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

type callbackEvent struct {
	kind, text string
	result     model.CodeResult
	size       int
}
type codeObserver struct {
	callbacks  model.CodeCallbacks
	timeout    time.Duration
	queue      chan callbackEvent
	stop, done chan struct{}
	once       sync.Once
	mu         sync.Mutex
	bytes      int
	enabled    bool
	err        *model.Error
}

func newCodeObserver(callbacks model.CodeCallbacks, timeout time.Duration) *codeObserver {
	o := &codeObserver{callbacks: callbacks, timeout: timeout, queue: make(chan callbackEvent, codeCallbackQueueEvents), stop: make(chan struct{}), done: make(chan struct{}), enabled: true}
	go o.dispatch()
	return o
}
func (o *codeObserver) enqueue(event callbackEvent) {
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
	o.err = &model.Error{Code: model.ResourceExhausted, Operation: "Code.callbacks", Reason: reason}
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
func (o *codeObserver) finish() *model.Error {
	close(o.queue)
	<-o.done
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.err
}
