package ags

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func codeFixture(t *testing.T, handler http.Handler) (*Sandbox, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(handler)
	sandbox := newSandbox(nil, "sandbox-code", newDataPlane(server.URL, "synthetic-instance-token", server.Client()))
	t.Cleanup(func() {
		_ = sandbox.Close()
		server.Close()
	})
	return sandbox, server
}

func TestCodeManagedContextRequestAndAggregation(t *testing.T) {
	var calls atomic.Int32
	sandbox, _ := codeFixture(t, http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if request.Header.Get("X-Access-Token") != "synthetic-instance-token" || strings.Contains(request.URL.String(), "synthetic-instance-token") {
			t.Error("instance token was not confined to the data-plane header")
		}
		w.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/contexts":
			var input createCodeContextRequest
			if err := json.NewDecoder(request.Body).Decode(&input); err != nil || input.Language != "python" || input.CWD != "/workspace" {
				t.Errorf("unexpected context input: %+v err=%v", input, err)
			}
			_, _ = fmt.Fprint(w, `{"id":"context-1","language":"python","cwd":"/workspace"}`)
		case "/execute":
			var input runCodeRequest
			if err := json.NewDecoder(request.Body).Decode(&input); err != nil || input.ContextID != "context-1" || input.Language != "" || input.Env["FOO"] != "bar" {
				t.Errorf("unexpected run input: %+v err=%v", input, err)
			}
			_, _ = fmt.Fprintln(w, `{"type":"stdout","text":"hello"}`)
			_, _ = fmt.Fprintln(w, `{"type":"result","text":"42","is_main_result":true}`)
			_, _ = fmt.Fprintln(w, `{"type":"number_of_executions","execution_count":2}`)
			_, _ = fmt.Fprintln(w, `{"type":"end_of_execution"}`)
		default:
			t.Errorf("unexpected path %s", request.URL.Path)
		}
	}))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	managed, err := sandbox.Code().CreateContext(ctx, CreateCodeContextOptions{CWD: "/workspace"})
	if err != nil {
		t.Fatal(err)
	}
	if managed.ID() != "context-1" || managed.Language() != "python" || managed.CWD() != "/workspace" {
		t.Fatalf("unexpected managed context: %q %q %q", managed.ID(), managed.Language(), managed.CWD())
	}
	result, err := sandbox.Code().Run(ctx, "print(42)", RunCodeOptions{Context: managed, Env: map[string]string{"FOO": "bar"}}, CodeCallbacks{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Stdout) != 1 || result.Stdout[0] != "hello" || len(result.Results) != 1 || result.Results[0].Text == nil || *result.Results[0].Text != "42" || !result.Results[0].IsMainResult || result.ExecutionCount == nil || *result.ExecutionCount != 2 {
		t.Fatalf("unexpected execution: %+v", result)
	}
	if calls.Load() != 2 {
		t.Fatalf("unexpected request count %d", calls.Load())
	}
}

func TestCodeContextOwnerGenerationAndBusyRejectBeforeHTTP(t *testing.T) {
	var executeCalls atomic.Int32
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if request.URL.Path == "/contexts" {
			_, _ = fmt.Fprint(w, `{"id":"context-1","language":"python","cwd":"/home/user"}`)
			return
		}
		executeCalls.Add(1)
		started <- struct{}{}
		<-release
		_, _ = fmt.Fprintln(w, `{"type":"stdout","text":"done"}`)
	})
	sandbox, server := codeFixture(t, handler)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	managed, err := sandbox.Code().CreateContext(ctx, CreateCodeContextOptions{})
	if err != nil {
		t.Fatal(err)
	}

	other := newSandbox(nil, "other", newDataPlane(server.URL, "synthetic-instance-token", server.Client()))
	defer other.Close()
	_, err = other.Code().Run(ctx, "1", RunCodeOptions{Context: managed}, CodeCallbacks{})
	assertCodeReason(t, err, "CODE_CONTEXT_OWNER_MISMATCH")
	if executeCalls.Load() != 0 {
		t.Fatal("owner mismatch sent HTTP")
	}

	finished := make(chan error, 1)
	go func() {
		_, runErr := sandbox.Code().Run(ctx, "1", RunCodeOptions{Context: managed}, CodeCallbacks{})
		finished <- runErr
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("first execution did not start")
	}
	_, err = sandbox.Code().Run(ctx, "2", RunCodeOptions{Context: managed}, CodeCallbacks{})
	assertCodeReason(t, err, "CODE_CONTEXT_BUSY")
	if executeCalls.Load() != 1 {
		t.Fatal("busy context sent a second request")
	}
	close(release)
	if err = <-finished; err != nil {
		t.Fatal(err)
	}

	newGeneration := newDataPlane(server.URL, "new-synthetic-instance-token", server.Client())
	sandbox.mu.Lock()
	oldGeneration := sandbox.plane
	sandbox.plane = newGeneration
	sandbox.mu.Unlock()
	_ = oldGeneration.Close()
	_, err = sandbox.Code().Run(ctx, "3", RunCodeOptions{Context: managed}, CodeCallbacks{})
	assertCodeReason(t, err, "CODE_CONTEXT_INVALIDATED")
	if executeCalls.Load() != 1 {
		t.Fatal("invalidated context sent HTTP")
	}
}

func TestCodeExternalContextAndStrictProtocol(t *testing.T) {
	var input runCodeRequest
	sandbox, _ := codeFixture(t, http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		_ = json.NewDecoder(request.Body).Decode(&input)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"type": "future_event", "value": 1})
	}))
	external, err := NewExternalCodeContextRef("external-1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = sandbox.Code().Run(context.Background(), "1", RunCodeOptions{Context: external}, CodeCallbacks{})
	assertCodeReason(t, err, "CODE_EVENT_UNKNOWN")
	var unknown *Error
	if !errors.As(err, &unknown) || unknown.Cause == nil || !strings.Contains(unknown.Cause.Error(), "future_event") {
		t.Fatalf("unknown event diagnostic = %#v", unknown)
	}
	if input.ContextID != "external-1" || input.Language != "" {
		t.Fatalf("external context mapping changed: %+v", input)
	}
	if _, err = NewExternalCodeContextRef(" bad "); err == nil {
		t.Fatal("invalid external context ID accepted")
	}
}

func TestCodeRejectsEventsAfterExecutionEnd(t *testing.T) {
	sandbox, _ := codeFixture(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, `{"type":"end_of_execution"}`)
		_, _ = fmt.Fprintln(w, `{"type":"stdout","text":"late"}`)
	}))
	_, err := sandbox.Code().Run(context.Background(), "1", RunCodeOptions{}, CodeCallbacks{})
	assertCodeReason(t, err, "CODE_EVENT_AFTER_END")
}

func TestCodeCallbackFailureDoesNotBreakAggregation(t *testing.T) {
	const eventCount = 80
	sandbox, _ := codeFixture(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		for i := 0; i < eventCount; i++ {
			_, _ = fmt.Fprintf(w, "{\"type\":\"stdout\",\"text\":%q}\n", strings.Repeat("x", 20<<10))
		}
	}))
	release := make(chan struct{})
	defer close(release)
	result, err := sandbox.Code().Run(context.Background(), "1", RunCodeOptions{MaxOutputBytes: 2 << 20, CallbackTimeout: 20 * time.Millisecond}, CodeCallbacks{OnStdout: func(string) { <-release }})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Stdout) != eventCount || result.StdoutTruncated || result.CallbackError == nil {
		t.Fatalf("aggregation or callback isolation failed: logs=%d truncated=%v callback=%v", len(result.Stdout), result.StdoutTruncated, result.CallbackError)
	}
	if result.CallbackError.Reason != "CODE_CALLBACK_BUFFER_FULL" && result.CallbackError.Reason != "CODE_CALLBACK_TIMEOUT" {
		t.Fatalf("unexpected callback error: %v", result.CallbackError)
	}
}

func TestCodeCallbackPanicDoesNotBreakAggregation(t *testing.T) {
	sandbox, _ := codeFixture(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintln(w, `{"type":"stdout","text":"retained"}`)
	}))
	result, err := sandbox.Code().Run(context.Background(), "1", RunCodeOptions{}, CodeCallbacks{
		OnStdout: func(string) { panic("consumer callback") },
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Stdout) != 1 || result.Stdout[0] != "retained" {
		t.Fatalf("aggregate = %#v", result.Stdout)
	}
	if result.CallbackError == nil || result.CallbackError.Reason != "CODE_CALLBACK_PANIC" {
		t.Fatalf("callback error = %v", result.CallbackError)
	}
}

func TestCodeCancellationReturnsStableLocalError(t *testing.T) {
	sandbox, _ := codeFixture(t, http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintln(w, `{"type":"stdout","text":"started"}`)
		if flush, ok := w.(http.Flusher); ok {
			flush.Flush()
		}
		<-request.Context().Done()
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := sandbox.Code().Run(ctx, "1", RunCodeOptions{}, CodeCallbacks{})
	var failure *Error
	if !errors.As(err, &failure) || failure.Code != DeadlineExceeded {
		t.Fatalf("unexpected cancellation error: %v", err)
	}
}

func assertCodeReason(t *testing.T, err error, reason string) {
	t.Helper()
	var failure *Error
	if !errors.As(err, &failure) || failure.Reason != reason {
		var cause error
		if failure != nil {
			cause = failure.Cause
		}
		t.Fatalf("error=%v cause=%v, want reason %s", err, cause, reason)
	}
}
