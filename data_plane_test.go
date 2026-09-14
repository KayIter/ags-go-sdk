package ags

import (
	"bytes"
	"context"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	fsproto "github.com/TencentCloudAgentRuntime/ags-go-sdk/pb/filesystem"
	fsconnect "github.com/TencentCloudAgentRuntime/ags-go-sdk/pb/filesystem/filesystemconnect"
	processproto "github.com/TencentCloudAgentRuntime/ags-go-sdk/pb/process"
	processconnect "github.com/TencentCloudAgentRuntime/ags-go-sdk/pb/process/processconnect"
)

type filesystemFixture struct{ fsconnect.FilesystemHandler }

type rootOnlyFilesystem struct {
	fsconnect.FilesystemHandler
	mu    sync.Mutex
	users []string
}

func (f *rootOnlyFilesystem) ListDir(_ context.Context, req *connect.Request[fsproto.ListDirRequest]) (*connect.Response[fsproto.ListDirResponse], error) {
	auth := req.Header().Get("Authorization")
	f.mu.Lock()
	f.users = append(f.users, auth)
	f.mu.Unlock()
	if auth == "Basic dXNlcjo=" {
		return nil, connect.NewError(connect.CodeUnauthenticated, nil)
	}
	return connect.NewResponse(&fsproto.ListDirResponse{}), nil
}

func (filesystemFixture) Stat(_ context.Context, req *connect.Request[fsproto.StatRequest]) (*connect.Response[fsproto.StatResponse], error) {
	return connect.NewResponse(&fsproto.StatResponse{Entry: &fsproto.EntryInfo{Name: "event.txt", Path: req.Msg.Path, Type: fsproto.FileType_FILE_TYPE_FILE, Size: 7}}), nil
}
func (filesystemFixture) ListDir(_ context.Context, req *connect.Request[fsproto.ListDirRequest]) (*connect.Response[fsproto.ListDirResponse], error) {
	return connect.NewResponse(&fsproto.ListDirResponse{Entries: []*fsproto.EntryInfo{{Name: "a.txt", Path: req.Msg.Path + "/a.txt", Type: fsproto.FileType_FILE_TYPE_FILE, Size: 3, Mode: 0644, Permissions: "-rw-r--r--"}}}), nil
}
func (filesystemFixture) WatchDir(_ context.Context, _ *connect.Request[fsproto.WatchDirRequest], stream *connect.ServerStream[fsproto.WatchDirResponse]) error {
	if err := stream.Send(&fsproto.WatchDirResponse{Event: &fsproto.WatchDirResponse_Start{Start: &fsproto.WatchDirResponse_StartEvent{}}}); err != nil {
		return err
	}
	for _, kind := range []fsproto.EventType{fsproto.EventType_EVENT_TYPE_CREATE, fsproto.EventType_EVENT_TYPE_WRITE} {
		if err := stream.Send(&fsproto.WatchDirResponse{Event: &fsproto.WatchDirResponse_Filesystem{Filesystem: &fsproto.FilesystemEvent{Name: "event.txt", Type: kind}}}); err != nil {
			return err
		}
	}
	return nil
}

type processFixture struct {
	processconnect.ProcessHandler
	input  chan []byte
	resize chan [2]uint32
	signal chan processproto.Signal
}

func (p *processFixture) Start(_ context.Context, req *connect.Request[processproto.StartRequest], stream *connect.ServerStream[processproto.StartResponse]) error {
	if err := stream.Send(startResponse(42)); err != nil {
		return err
	}
	if req.Msg.Pty == nil {
		_ = stream.Send(dataResponse([]byte("0123456789"), nil, nil))
		_ = stream.Send(dataResponse(nil, []byte("err"), nil))
		return stream.Send(endResponse(7))
	}
	data := <-p.input
	if err := stream.Send(dataResponse(nil, nil, data)); err != nil {
		return err
	}
	return stream.Send(endResponse(0))
}
func (p *processFixture) SendInput(_ context.Context, req *connect.Request[processproto.SendInputRequest]) (*connect.Response[processproto.SendInputResponse], error) {
	p.input <- req.Msg.GetInput().GetPty()
	return connect.NewResponse(&processproto.SendInputResponse{}), nil
}
func (p *processFixture) Update(_ context.Context, req *connect.Request[processproto.UpdateRequest]) (*connect.Response[processproto.UpdateResponse], error) {
	p.resize <- [2]uint32{req.Msg.GetPty().GetSize().GetCols(), req.Msg.GetPty().GetSize().GetRows()}
	return connect.NewResponse(&processproto.UpdateResponse{}), nil
}
func (p *processFixture) SendSignal(_ context.Context, req *connect.Request[processproto.SendSignalRequest]) (*connect.Response[processproto.SendSignalResponse], error) {
	p.signal <- req.Msg.GetSignal()
	return connect.NewResponse(&processproto.SendSignalResponse{}), nil
}
func startResponse(pid uint32) *processproto.StartResponse {
	return &processproto.StartResponse{Event: &processproto.ProcessEvent{Event: &processproto.ProcessEvent_Start{Start: &processproto.ProcessEvent_StartEvent{Pid: pid}}}}
}
func dataResponse(stdout, stderr, pty []byte) *processproto.StartResponse {
	data := &processproto.ProcessEvent_DataEvent{}
	switch {
	case stdout != nil:
		data.Output = &processproto.ProcessEvent_DataEvent_Stdout{Stdout: stdout}
	case stderr != nil:
		data.Output = &processproto.ProcessEvent_DataEvent_Stderr{Stderr: stderr}
	default:
		data.Output = &processproto.ProcessEvent_DataEvent_Pty{Pty: pty}
	}
	return &processproto.StartResponse{Event: &processproto.ProcessEvent{Event: &processproto.ProcessEvent_Data{Data: data}}}
}
func endResponse(code int32) *processproto.StartResponse {
	return &processproto.StartResponse{Event: &processproto.ProcessEvent{Event: &processproto.ProcessEvent_End{End: &processproto.ProcessEvent_EndEvent{ExitCode: code, Exited: true, Status: "exited"}}}}
}

func fixtureServer(t *testing.T, files http.Handler, process *processFixture) (*httptest.Server, *legacyDataPlane) {
	t.Helper()
	mux := http.NewServeMux()
	fsPath, fsHandler := fsconnect.NewFilesystemHandler(filesystemFixture{})
	mux.Handle(fsPath, fsHandler)
	processPath, processHandler := processconnect.NewProcessHandler(process)
	mux.Handle(processPath, processHandler)
	mux.Handle("/files", files)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server, newDataPlane(server.URL, "token", server.Client())
}

type gatedReader struct {
	release <-chan struct{}
	once    sync.Once
	rest    *bytes.Reader
}

func (r *gatedReader) Read(p []byte) (int, error) {
	first := false
	r.once.Do(func() { first = true })
	if first {
		p[0] = 'x'
		return 1, nil
	}
	<-r.release
	return r.rest.Read(p)
}

func TestDataPlaneFileWriteIsStreaming(t *testing.T) {
	release := make(chan struct{})
	firstSeen := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Access-Token") != "token" || !strings.HasPrefix(r.Header.Get("Authorization"), "Basic ") {
			t.Error("missing data-plane auth headers")
		}
		reader, err := r.MultipartReader()
		if err != nil {
			t.Error(err)
			return
		}
		part, err := reader.NextPart()
		if err != nil {
			t.Error(err)
			return
		}
		one := make([]byte, 1)
		if _, err = io.ReadFull(part, one); err != nil {
			t.Error(err)
			return
		}
		close(firstSeen)
		close(release)
		content, err := io.ReadAll(part)
		if err != nil {
			t.Error(err)
		}
		if string(append(one, content...)) != "x-rest" {
			t.Errorf("content mismatch")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[{"name":"job.bin","type":"file","path":"/tmp/job.bin"}]`)
	})
	process := &processFixture{input: make(chan []byte, 1), resize: make(chan [2]uint32, 1), signal: make(chan processproto.Signal, 1)}
	_, plane := fixtureServer(t, handler, process)
	done := make(chan error, 1)
	go func() {
		_, err := plane.Write(context.Background(), "/tmp/job.bin", &gatedReader{release: release, rest: bytes.NewReader([]byte("-rest"))}, "user")
		done <- err
	}()
	select {
	case <-firstSeen:
	case <-time.After(time.Second):
		t.Fatal("server did not receive first byte before reader completed")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestGeneratedDataPlaneCommandWatchAndPTY(t *testing.T) {
	files := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_, _ = io.WriteString(w, "ok")
			return
		}
		_ = multipart.ErrMessageTooLarge
	})
	process := &processFixture{input: make(chan []byte, 1), resize: make(chan [2]uint32, 1), signal: make(chan processproto.Signal, 1)}
	_, plane := fixtureServer(t, files, process)
	list, err := plane.List(context.Background(), "/tmp", 1, "user")
	if err != nil || len(list) != 1 || list[0].Name != "a.txt" || list[0].Mode != 0644 {
		t.Fatalf("list=%+v err=%v", list, err)
	}
	result, err := plane.Run(context.Background(), "sh", CommandOptions{Args: []string{"-lc", "exit 7"}, MaxOutputBytes: 4})
	if err != nil || result.ExitCode != 7 || string(result.Stdout) != "0123" || !result.StdoutTruncated || string(result.Stderr) != "err" {
		t.Fatalf("command=%+v err=%v", result, err)
	}
	stream, err := plane.Watch(context.Background(), "/tmp", WatchOptions{IncludeEntry: true})
	if err != nil {
		t.Fatal(err)
	}
	first, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	second, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if first.WatchID == "" || first.WatchID != second.WatchID || first.Sequence != 1 || second.Sequence != 2 || first.Entry == nil {
		t.Fatalf("events=%+v %+v", first, second)
	}
	pty, err := plane.OpenPTY(context.Background(), PTYOptions{Command: "sh", Cols: 80, Rows: 24})
	if err != nil {
		t.Fatal(err)
	}
	if event, err := pty.Recv(); err != nil || event.Type != PTYStart || pty.ID() == "" {
		t.Fatalf("start=%+v err=%v", event, err)
	}
	if err := pty.Resize(context.Background(), 100, 40); err != nil {
		t.Fatal(err)
	}
	if got := <-process.resize; got != [2]uint32{100, 40} {
		t.Fatalf("resize=%v", got)
	}
	if err := pty.Input(context.Background(), []byte("echo ok\n")); err != nil {
		t.Fatal(err)
	}
	output, _ := pty.Recv()
	end, _ := pty.Recv()
	if string(output.Data) != "echo ok\n" || end.Exit == nil || end.Exit.Code != 0 {
		t.Fatalf("pty output=%+v end=%+v", output, end)
	}
}

func TestDataPlaneReadinessFallsBackToRoot(t *testing.T) {
	files := &rootOnlyFilesystem{}
	mux := http.NewServeMux()
	path, handler := fsconnect.NewFilesystemHandler(files)
	mux.Handle(path, handler)
	server := httptest.NewServer(mux)
	defer server.Close()
	plane := newDataPlane(server.URL, "token", server.Client())
	if err := plane.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	files.mu.Lock()
	defer files.mu.Unlock()
	if len(files.users) != 2 || files.users[1] != "Basic cm9vdDo=" {
		t.Fatalf("readiness auth sequence=%v", files.users)
	}
}
