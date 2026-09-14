package ags

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	processproto "github.com/TencentCloudAgentRuntime/ags-go-sdk/pb/process"
	processconnect "github.com/TencentCloudAgentRuntime/ags-go-sdk/pb/process/processconnect"
)

type endBeforeAckProcess struct {
	processconnect.ProcessHandler
	input chan struct{}
	ack   chan struct{}
}

func (p *endBeforeAckProcess) Start(ctx context.Context, _ *connect.Request[processproto.StartRequest], stream *connect.ServerStream[processproto.StartResponse]) error {
	if err := stream.Send(startResponse(42)); err != nil {
		return err
	}
	select {
	case <-p.input:
	case <-ctx.Done():
		return ctx.Err()
	}
	return stream.Send(endResponse(0))
}
func (p *endBeforeAckProcess) SendInput(ctx context.Context, _ *connect.Request[processproto.SendInputRequest]) (*connect.Response[processproto.SendInputResponse], error) {
	close(p.input)
	select {
	case <-p.ack:
		return connect.NewResponse(&processproto.SendInputResponse{}), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestPtyEndDoesNotCancelAcceptedInput(t *testing.T) {
	process := &endBeforeAckProcess{input: make(chan struct{}), ack: make(chan struct{})}
	path, handler := processconnect.NewProcessHandler(process)
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	server := httptest.NewServer(mux)
	defer server.Close()
	plane := newDataPlane(server.URL, "token", server.Client())
	defer plane.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pty, err := plane.OpenPTY(ctx, PTYOptions{Command: "sh", Cols: 80, Rows: 24})
	if err != nil {
		t.Fatal(err)
	}
	defer pty.Close()
	if _, err = pty.Recv(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- pty.Input(ctx, []byte("exit 0\n")) }()
	event, err := pty.Recv()
	if err != nil || event.Type != PTYEnd {
		close(process.ack)
		t.Fatalf("end: %v %v", event, err)
	}
	select {
	case err := <-done:
		close(process.ack)
		t.Fatalf("input ended before acknowledgment: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(process.ack)
	if err := <-done; err != nil {
		t.Fatalf("accepted input canceled by natural End: %v", err)
	}
}

func TestPtyEndStillAllowsExplicitCancellation(t *testing.T) {
	for _, mode := range []string{"close", "invalidate", "caller"} {
		t.Run(mode, func(t *testing.T) {
			process := &endBeforeAckProcess{input: make(chan struct{}), ack: make(chan struct{})}
			path, handler := processconnect.NewProcessHandler(process)
			mux := http.NewServeMux()
			mux.Handle(path, handler)
			server := httptest.NewServer(mux)
			defer server.Close()
			plane := newDataPlane(server.URL, "token", server.Client())
			defer plane.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			pty, err := plane.OpenPTY(ctx, PTYOptions{Command: "sh", Cols: 80, Rows: 24})
			if err != nil {
				t.Fatal(err)
			}
			defer pty.Close()
			if _, err = pty.Recv(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- pty.Input(ctx, []byte("exit 0\n")) }()
			event, err := pty.Recv()
			if err != nil || event.Type != PTYEnd {
				t.Fatalf("end: %v %v", event, err)
			}
			switch mode {
			case "close":
				_ = pty.Close()
			case "invalidate":
				_ = plane.Close()
			case "caller":
				cancel()
			}
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("pending input was not canceled")
				}
			case <-time.After(time.Second):
				t.Fatal("cancellation did not release pending input")
			}
		})
	}
}
