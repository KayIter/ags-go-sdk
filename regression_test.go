package ags

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/cloudapi"
	fsproto "github.com/TencentCloudAgentRuntime/ags-go-sdk/pb/filesystem"
	fsconnect "github.com/TencentCloudAgentRuntime/ags-go-sdk/pb/filesystem/filesystemconnect"
	pp "github.com/TencentCloudAgentRuntime/ags-go-sdk/pb/process"
	pc "github.com/TencentCloudAgentRuntime/ags-go-sdk/pb/process/processconnect"
)

func TestReviewExplicitUploadUsers(t *testing.T) {
	for _, user := range []SandboxUser{User, Root} {
		t.Run(string(user), func(t *testing.T) {
			seen := make(chan string, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				actual, _, _ := r.BasicAuth()
				seen <- actual
				_, _ = io.Copy(io.Discard, r.Body)
				if actual != dataPlaneUser(string(user)) {
					w.WriteHeader(401)
					return
				}
				_, _ = io.WriteString(w, `[{"name":"x","path":"/tmp/x","type":"file"}]`)
			}))
			defer server.Close()
			s := newSandbox(nil, "review-instance", newDataPlane(server.URL, "fixture", server.Client()))
			_, err := s.Files().Write(context.Background(), "/tmp/x", strings.NewReader("x"), WriteOptions{User: user})
			if err != nil {
				t.Fatalf("explicit SDK user %s emitted Basic username %q; error=%v", user, <-seen, err)
			}
		})
	}
}

func TestReviewFileReadInvalidatedWithPlane(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "A")
		w.(http.Flusher).Flush()
		<-release
		_, _ = io.WriteString(w, "B")
	}))
	defer server.Close()
	plane := newDataPlane(server.URL, "fixture", server.Client())
	s := newSandbox(nil, "review-instance", plane)
	reader, err := s.Files().Read(context.Background(), "/tmp/x", ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	first := make([]byte, 1)
	_, _ = io.ReadFull(reader, first)
	_ = s.invalidate()
	close(release)
	after, err := io.ReadAll(reader)
	if err == nil {
		t.Fatalf("old file reader remains usable after pause invalidation: got %q", after)
	}
}

type reviewDeniedFS struct{ fsconnect.FilesystemHandler }

func (reviewDeniedFS) ListDir(context.Context, *connect.Request[fsproto.ListDirRequest]) (*connect.Response[fsproto.ListDirResponse], error) {
	return nil, connect.NewError(connect.CodePermissionDenied, errors.New("backend-only-details"))
}
func TestReviewDataPlaneStableErrors(t *testing.T) {
	path, handler := fsconnect.NewFilesystemHandler(reviewDeniedFS{})
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	server := httptest.NewServer(mux)
	defer server.Close()
	s := newSandbox(nil, "review-instance", newDataPlane(server.URL, "fixture", server.Client()))
	_, err := s.Files().List(context.Background(), "/denied", FileListOptions{})
	var sdk *Error
	if !errors.As(err, &sdk) || sdk.Code != PermissionDenied {
		t.Fatalf("public Files.List exposes %T instead of SDK error: %v", err, err)
	}
}

type reviewBurstProcess struct{ pc.ProcessHandler }

func (reviewBurstProcess) Start(_ context.Context, _ *connect.Request[pp.StartRequest], stream *connect.ServerStream[pp.StartResponse]) error {
	if err := stream.Send(startResponse(42)); err != nil {
		return err
	}
	for i := 0; i < 40; i++ {
		if err := stream.Send(dataResponse(nil, nil, []byte("x"))); err != nil {
			return err
		}
	}
	return stream.Send(endResponse(0))
}
func (reviewBurstProcess) SendSignal(context.Context, *connect.Request[pp.SendSignalRequest]) (*connect.Response[pp.SendSignalResponse], error) {
	return connect.NewResponse(&pp.SendSignalResponse{}), nil
}
func TestReviewPtyWaitWithoutEventConsumer(t *testing.T) {
	path, handler := pc.NewProcessHandler(reviewBurstProcess{})
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	server := httptest.NewServer(mux)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s := newSandbox(nil, "review-instance", newDataPlane(server.URL, "fixture", server.Client()))
	pty, err := s.PTY().Open(ctx, PTYOptions{Command: "sh", Cols: 80, Rows: 24})
	if err != nil {
		t.Fatal(err)
	}
	wait, end := context.WithTimeout(ctx, 100*time.Millisecond)
	defer end()
	_, err = pty.Wait(wait)
	// Release the stuck pump so this review probe does not leak a goroutine.
	for range pty.Events() {
	}
	var sdk *Error
	if !errors.As(err, &sdk) || sdk.Code != ResourceExhausted {
		t.Fatalf("unconsumed bounded PTY must fail promptly with RESOURCE_EXHAUSTED: %v", err)
	}
}

func TestReviewCloudInvalidParameterIsNotRetryable(t *testing.T) {
	err := mapCloudError(&cloudapi.Error{Code: "InvalidParameterValue.Timeout", RequestID: "fixture-request"}, "create")
	var sdk *Error
	if !errors.As(err, &sdk) || sdk.Code != InvalidArgument || sdk.Retryable {
		t.Fatalf("cloud HTTP-200 business rejection mapped incorrectly: %+v", sdk)
	}
}

type reviewCancelMonitor struct{ entered chan struct{} }

func (m *reviewCancelMonitor) Query(ctx context.Context, q monitorRequest) (monitorResponse, error) {
	m.entered <- struct{}{}
	<-ctx.Done()
	return monitorResponse{}, &Error{Code: Canceled, Cause: ctx.Err()}
}
func TestReviewMetricsCancellation(t *testing.T) {
	monitor := &reviewCancelMonitor{entered: make(chan struct{}, 10)}
	client, _ := NewClient(WithRegion("test"), WithCredential(CloudCredential{"id", "key"}), withControlPlane(metricControl{SandboxInfo{ID: "fixture", ToolID: "tool"}}), withMonitorTransport(monitor))
	s := newSandbox(client, "fixture", nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		for i := 0; i < 4; i++ {
			<-monitor.entered
		}
		cancel()
	}()
	_, err := s.Metrics().Get(ctx, MetricsQuery{})
	var sdk *Error
	if !errors.As(err, &sdk) || sdk.Code != Canceled {
		t.Fatalf("canceled Metrics.Get returned %v", err)
	}
}
