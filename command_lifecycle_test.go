package ags

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

type commandServer struct {
	server                  *httptest.Server
	emit                    chan []byte
	signals, inputs, starts atomic.Int32
}

func newCommandServer(t *testing.T) *commandServer {
	t.Helper()
	f := &commandServer{emit: make(chan []byte, 64)}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, e := io.ReadAll(r.Body)
		if e != nil {
			return
		}
		var input map[string]any
		if r.URL.Path == "/process.Process/Start" {
			f.starts.Add(1)
			if len(body) < 5 {
				t.Error("missing request frame")
				return
			}
			if e = json.Unmarshal(body[5:], &input); e != nil {
				t.Error(e)
				return
			}
			w.Header().Set("Content-Type", "application/connect+json")
			writeCommandFrame(w, 0, []byte(`{"event":{"start":{"pid":42}}}`))
			if input["process"].(map[string]any)["cmd"] == "foreground" {
				writeCommandFrame(w, 0, []byte(`{"event":{"end":{"exitCode":0,"exited":true}}}`))
				return
			}
			for {
				select {
				case frame := <-f.emit:
					if frame == nil {
						var header [5]byte
						binary.BigEndian.PutUint32(header[1:], 3<<20)
						_, _ = w.Write(header[:])
						w.(http.Flusher).Flush()
						continue
					}
					writeCommandFrame(w, 0, frame)
				case <-r.Context().Done():
					return
				}
			}
		}
		if e = json.Unmarshal(body, &input); e != nil {
			t.Error(e)
			return
		}
		if input["process"].(map[string]any)["pid"] != float64(42) {
			t.Error("wrong PID")
		}
		switch r.URL.Path {
		case "/process.Process/SendInput":
			f.inputs.Add(1)
			if input["input"].(map[string]any)["stdin"] != base64.StdEncoding.EncodeToString([]byte("hi")) {
				t.Error("incorrect input")
			}
		case "/process.Process/SendSignal":
			f.signals.Add(1)
			if input["signal"] == "SIGNAL_SIGKILL" {
				f.emit <- []byte(`{"event":{"end":{"exitCode":7,"exited":true}}}`)
			}
		default:
			t.Error("unexpected request")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	return f
}
func commandError(t *testing.T, err error, code ErrorCode) {
	t.Helper()
	var sdk *Error
	if !errors.As(err, &sdk) || sdk.Code != code {
		t.Fatalf("expected %s, got %v", code, err)
	}
}
func TestConformanceCommandControls(t *testing.T) {
	f := newCommandServer(t)
	defer f.server.Close()
	sb := newSandbox(nil, "synthetic", newDataPlane(f.server.URL, "synthetic", f.server.Client()))
	defer sb.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	h, e := sb.Commands().Start(ctx, "background", StartOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Close()
	wait, stop := context.WithTimeout(ctx, 5*time.Millisecond)
	_, e = h.Wait(wait)
	stop()
	commandError(t, e, DeadlineExceeded)
	if f.signals.Load() != 0 {
		t.Fatal("Wait timeout sent signal")
	}
	if _, e = sb.Commands().Run(ctx, "foreground", CommandOptions{}); e != nil {
		t.Fatal(e)
	}
	if e = h.Write(ctx, []byte("hi")); e != nil {
		t.Fatal(e)
	}
	events := h.Events()
	if events != h.Events() {
		t.Fatal("events channel changed")
	}
	f.emit <- []byte(`{"event":{"data":{"stdout":"YWJj"}}}`)
	select {
	case event := <-events:
		if event.Type != CommandStdout || string(event.Data) != "abc" {
			t.Fatal("output mismatch")
		}
		event.Data[0] = '!'
	case <-ctx.Done():
		t.Fatal("output missing")
	}
	if e = h.Signal(ctx, SignalTERM); e != nil {
		t.Fatal(e)
	}
	// End may race a signal ACK. Regardless of that ACK's outcome, End is authoritative.
	if err := h.Signal(ctx, SignalKILL); err != nil {
		commandError(t, err, Canceled)
	}
	result, e := h.Wait(ctx)
	if e != nil || result.ExitCode != 7 || string(result.Stdout) != "abc" {
		t.Fatalf("result=%+v error=%v", result, e)
	}
	select {
	case event := <-events:
		if event.Type != CommandExit || event.Exit.Code != 7 {
			t.Fatal("exit event missing")
		}
	case <-ctx.Done():
		t.Fatal("no exit")
	}
	if _, ok := <-events; ok {
		t.Fatal("events not closed")
	}
	if e = h.Write(ctx, []byte("hi")); e == nil {
		t.Fatal("write after End accepted")
	}
	if e = h.Signal(ctx, SignalTERM); e == nil {
		t.Fatal("signal after End accepted")
	}
	_ = h.Close()
	if f.inputs.Load() != 1 || f.signals.Load() != 2 || f.starts.Load() != 2 {
		t.Fatal("unexpected calls or retry")
	}
}
func TestConformanceCommandLocalTermination(t *testing.T) {
	for _, mode := range []string{"close", "caller", "generation"} {
		t.Run(mode, func(t *testing.T) {
			f := newCommandServer(t)
			defer f.server.Close()
			plane := newDataPlane(f.server.URL, "synthetic", f.server.Client())
			sb := newSandbox(nil, "synthetic", plane)
			defer sb.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			h, e := sb.Commands().Start(ctx, "background", StartOptions{})
			if e != nil {
				t.Fatal(e)
			}
			defer h.Close()
			code := Canceled
			switch mode {
			case "close":
				_ = h.Close()
			case "caller":
				cancel()
			case "generation":
				_ = plane.Close()
				code = InstancePaused
			}
			wait, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			_, e = h.Wait(wait)
			commandError(t, e, code)
			if h.Err() == nil || f.signals.Load() != 0 {
				t.Fatal("local termination sent kill or lost error")
			}
		})
	}
}
func TestConformanceCommandBackpressure(t *testing.T) {
	for _, size := range []int{1, 64 << 10} {
		name := "event-count"
		if size > 1 {
			name = "pending-bytes"
		}
		t.Run(name, func(t *testing.T) {
			f := newCommandServer(t)
			defer f.server.Close()
			sb := newSandbox(nil, "synthetic", newDataPlane(f.server.URL, "synthetic", f.server.Client()))
			defer sb.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			h, e := sb.Commands().Start(ctx, "background", StartOptions{})
			if e != nil {
				t.Fatal(e)
			}
			defer h.Close()
			h.Events()
			frame, _ := json.Marshal(map[string]any{"event": map[string]any{"data": map[string]any{"stdout": base64.StdEncoding.EncodeToString(make([]byte, size))}}})
			for i := 0; i < 33; i++ {
				f.emit <- frame
			}
			_, e = h.Wait(ctx)
			commandError(t, e, ResourceExhausted)
			var sdk *Error
			errors.As(e, &sdk)
			if sdk.Reason != "COMMAND_BUFFER_FULL" {
				t.Fatal(e)
			}
			if f.signals.Load() != 0 {
				t.Fatal("overflow sent kill")
			}
		})
	}
}
