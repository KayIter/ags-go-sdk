package ags

import (
	"context"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	process "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/gen/process"
	processconnect "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/gen/process/processconnect"
)

type commandConnectFixture struct {
	processconnect.UnimplementedProcessHandler
	t *testing.T
}

func (f commandConnectFixture) List(_ context.Context, request *connect.Request[process.ListRequest]) (*connect.Response[process.ListResponse], error) {
	if request.Header().Get("X-Access-Token") != "synthetic-instance-token" {
		f.t.Error("list token missing")
	}
	if request.Header().Get("Authorization") != "Basic cm9vdDo=" {
		f.t.Errorf("list user = %q", request.Header().Get("Authorization"))
	}
	cwd, tag := "/workspace", "worker"
	return connect.NewResponse(&process.ListResponse{Processes: []*process.ProcessInfo{{
		Pid:    23,
		Tag:    &tag,
		Config: &process.ProcessConfig{Cmd: "python", Args: []string{"job.py"}, Envs: map[string]string{"FOO": "bar"}, Cwd: &cwd},
	}}}), nil
}

func (f commandConnectFixture) Connect(_ context.Context, request *connect.Request[process.ConnectRequest], stream *connect.ServerStream[process.ConnectResponse]) error {
	if request.Msg.GetProcess().GetPid() != 23 || request.Header().Get("X-Access-Token") != "synthetic-instance-token" {
		f.t.Error("connect identity or token changed")
	}
	if err := stream.Send(&process.ConnectResponse{Event: &process.ProcessEvent{Event: &process.ProcessEvent_Start{Start: &process.ProcessEvent_StartEvent{Pid: 23}}}}); err != nil {
		return err
	}
	if err := stream.Send(&process.ConnectResponse{Event: &process.ProcessEvent{Event: &process.ProcessEvent_Data{Data: &process.ProcessEvent_DataEvent{Output: &process.ProcessEvent_DataEvent_Stdout{Stdout: []byte("connected")}}}}}); err != nil {
		return err
	}
	return stream.Send(&process.ConnectResponse{Event: &process.ProcessEvent{Event: &process.ProcessEvent_End{End: &process.ProcessEvent_EndEvent{ExitCode: 0, Exited: true, Status: "exited"}}}})
}

func TestCommandsListAndConnectUseCurrentGeneration(t *testing.T) {
	path, handler := processconnect.NewProcessHandler(commandConnectFixture{t: t})
	server := httptest.NewServer(handler)
	defer server.Close()
	if path == "" {
		t.Fatal("process handler path missing")
	}
	sandbox := newSandbox(nil, "sandbox-command", newDataPlane(server.URL, "synthetic-instance-token", server.Client()))
	defer sandbox.Close()

	processes, err := sandbox.Commands().List(context.Background(), CommandListOptions{User: Root})
	if err != nil {
		t.Fatal(err)
	}
	if len(processes) != 1 || processes[0].PID != 23 || processes[0].Cmd != "python" || processes[0].Tag == nil || *processes[0].Tag != "worker" || processes[0].CWD == nil || *processes[0].CWD != "/workspace" || processes[0].Env["FOO"] != "bar" {
		t.Fatalf("unexpected process list: %+v", processes)
	}
	handle, err := sandbox.Commands().Connect(context.Background(), 23, ConnectCommandOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	result, err := handle.Wait(context.Background())
	if err != nil || result.ExitCode != 0 || string(result.Stdout) != "connected" {
		t.Fatalf("unexpected connected result: %+v err=%v", result, err)
	}
}

func TestCommandsListRejectsInvalidOptionsBeforeDataPlane(t *testing.T) {
	sandbox := newSandbox(nil, "sandbox-command", nil)
	_, err := sandbox.Commands().List(context.Background(), CommandListOptions{}, CommandListOptions{})
	assertCodeReason(t, err, "SINGLE_OPTIONS_REQUIRED")
	_, err = sandbox.Commands().List(context.Background(), CommandListOptions{User: "admin"})
	assertCodeReason(t, err, "INVALID_SANDBOX_USER")
}

func TestCommandsConnectRejectsZeroPIDBeforeDataPlane(t *testing.T) {
	sandbox := newSandbox(nil, "sandbox-command", nil)
	_, err := sandbox.Commands().Connect(context.Background(), 0, ConnectCommandOptions{})
	assertCodeReason(t, err, "PID_REQUIRED")
}
