package dataplane

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	process "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/gen/process"
	processconnect "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/gen/process/processconnect"
)

type semanticProcess struct {
	processconnect.UnimplementedProcessHandler
	ptyData []byte
}

func (semanticProcess) List(context.Context, *connect.Request[process.ListRequest]) (*connect.Response[process.ListResponse], error) {
	tag, cwd := "job", "/work"
	return connect.NewResponse(&process.ListResponse{Processes: []*process.ProcessInfo{{Pid: 42, Tag: &tag, Config: &process.ProcessConfig{Cmd: "sh", Args: []string{"-lc", "echo"}, Envs: map[string]string{"A": "B"}, Cwd: &cwd}}}}), nil
}

func (semanticProcess) Connect(_ context.Context, request *connect.Request[process.ConnectRequest], stream *connect.ServerStream[process.ConnectResponse]) error {
	pid := request.Msg.GetProcess().GetPid()
	if err := stream.Send(&process.ConnectResponse{Event: startProcessEvent(pid)}); err != nil {
		return err
	}
	return stream.Send(&process.ConnectResponse{Event: endProcessEvent(0)})
}

func (s semanticProcess) Start(_ context.Context, request *connect.Request[process.StartRequest], stream *connect.ServerStream[process.StartResponse]) error {
	if err := stream.Send(&process.StartResponse{Event: keepaliveProcessEvent()}); err != nil {
		return err
	}
	if err := stream.Send(&process.StartResponse{Event: startProcessEvent(42)}); err != nil {
		return err
	}
	data := &process.ProcessEvent_DataEvent{Output: &process.ProcessEvent_DataEvent_Stdout{Stdout: []byte("out")}}
	if request.Msg.Pty != nil {
		payload := s.ptyData
		if payload == nil {
			payload = []byte("pty")
		}
		data.Output = &process.ProcessEvent_DataEvent_Pty{Pty: payload}
	}
	if err := stream.Send(&process.StartResponse{Event: &process.ProcessEvent{Event: &process.ProcessEvent_Data{Data: data}}}); err != nil {
		return err
	}
	return stream.Send(&process.StartResponse{Event: endProcessEvent(7)})
}

func TestPTYDoesNotApplyCommandFrameLimit(t *testing.T) {
	mux := http.NewServeMux()
	path, handler := processconnect.NewProcessHandler(semanticProcess{ptyData: bytes.Repeat([]byte("x"), CommandFrameLimit+1)})
	mux.Handle(path, handler)
	server := httptest.NewServer(mux)
	defer server.Close()

	stream, err := New(server.URL, "token", server.Client()).StartPTY(context.Background(), PTYConfig{Command: "sh", Size: PTYSize{Cols: 80, Rows: 24}}, "USER")
	if err != nil {
		t.Fatal(err)
	}
	event, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if event.Kind != ProcessPTY || len(event.Data) != CommandFrameLimit+1 {
		t.Fatalf("pty event kind=%v bytes=%d", event.Kind, len(event.Data))
	}
}

func (semanticProcess) SendInput(context.Context, *connect.Request[process.SendInputRequest]) (*connect.Response[process.SendInputResponse], error) {
	return connect.NewResponse(&process.SendInputResponse{}), nil
}

func (semanticProcess) Update(context.Context, *connect.Request[process.UpdateRequest]) (*connect.Response[process.UpdateResponse], error) {
	return connect.NewResponse(&process.UpdateResponse{}), nil
}

func (semanticProcess) SendSignal(context.Context, *connect.Request[process.SendSignalRequest]) (*connect.Response[process.SendSignalResponse], error) {
	return connect.NewResponse(&process.SendSignalResponse{}), nil
}

func startProcessEvent(pid uint32) *process.ProcessEvent {
	return &process.ProcessEvent{Event: &process.ProcessEvent_Start{Start: &process.ProcessEvent_StartEvent{Pid: pid}}}
}

func keepaliveProcessEvent() *process.ProcessEvent {
	return &process.ProcessEvent{Event: &process.ProcessEvent_Keepalive{Keepalive: &process.ProcessEvent_KeepAlive{}}}
}

func endProcessEvent(code int32) *process.ProcessEvent {
	return &process.ProcessEvent{Event: &process.ProcessEvent_End{End: &process.ProcessEvent_EndEvent{ExitCode: code, Exited: true, Status: "exited"}}}
}

func TestSemanticProcessAndPTYAdapters(t *testing.T) {
	mux := http.NewServeMux()
	path, handler := processconnect.NewProcessHandler(semanticProcess{})
	mux.Handle(path, handler)
	server := httptest.NewServer(mux)
	defer server.Close()
	client := New(server.URL, "token", server.Client())
	ctx := context.Background()

	stream, err := client.StartProcess(ctx, ProcessConfig{Command: "sh", Args: []string{"-lc", "echo"}, Env: map[string]string{"A": "B"}, CWD: "/work"}, "USER")
	if err != nil || stream.PID != 42 {
		t.Fatalf("start=%+v err=%v", stream, err)
	}
	event, err := stream.Recv()
	if err != nil || event.Kind != ProcessStdout || string(event.Data) != "out" {
		t.Fatalf("stdout=%+v err=%v", event, err)
	}
	event, err = stream.Recv()
	if err != nil || event.Kind != ProcessEnd || event.Exit.Code != 7 {
		t.Fatalf("end=%+v err=%v", event, err)
	}
	if _, err = stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("stream eof=%v", err)
	}
	if err = stream.Close(); err != nil {
		t.Fatal(err)
	}

	connected, err := client.ConnectProcess(ctx, 42, "ROOT")
	if err != nil || connected.PID != 42 {
		t.Fatalf("connect=%+v err=%v", connected, err)
	}
	if event, err = connected.Recv(); err != nil || event.Kind != ProcessEnd {
		t.Fatalf("connect event=%+v err=%v", event, err)
	}
	if err = connected.Invalidate(); err != nil {
		t.Fatal(err)
	}

	items, err := client.ListProcesses(ctx, "USER")
	if err != nil || len(items) != 1 || items[0].Tag == nil || items[0].CWD == nil || items[0].Env["A"] != "B" {
		t.Fatalf("processes=%+v err=%v", items, err)
	}
	if err = client.SendProcessInput(ctx, 42, []byte("input"), "USER"); err != nil {
		t.Fatal(err)
	}
	if err = client.SendProcessSignal(ctx, 42, SignalKILL, "USER"); err != nil {
		t.Fatal(err)
	}

	ptyStream, err := client.StartPTY(ctx, PTYConfig{Command: "sh", Size: PTYSize{Cols: 80, Rows: 24}}, "USER")
	if err != nil {
		t.Fatal(err)
	}
	if event, err = ptyStream.Recv(); err != nil || event.Kind != ProcessPTY || string(event.Data) != "pty" {
		t.Fatalf("pty=%+v err=%v", event, err)
	}
	if err = client.SendPTYInput(ctx, ptyStream.PID, []byte("input"), "USER"); err != nil {
		t.Fatal(err)
	}
	if err = client.ResizePTY(ctx, ptyStream.PID, PTYSize{Cols: 100, Rows: 40}, "USER"); err != nil {
		t.Fatal(err)
	}
	if err = ptyStream.Invalidate(); err != nil {
		t.Fatal(err)
	}
}
