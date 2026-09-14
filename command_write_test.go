package ags

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestConformanceCommandWriteSerialization(t *testing.T) {
	entered := make(chan string, 3)
	release := make(chan struct{})
	var inputs, signals atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		switch r.URL.Path {
		case "/process.Process/Start":
			w.Header().Set("Content-Type", "application/connect+json")
			writeCommandFrame(w, 0, []byte(`{"event":{"start":{"pid":42}}}`))
			<-r.Context().Done()
		case "/process.Process/SendInput":
			var request struct{ Input struct{ Stdin string } }
			if err := json.Unmarshal(body, &request); err != nil {
				t.Error(err)
				return
			}
			value, err := base64.StdEncoding.DecodeString(request.Input.Stdin)
			if err != nil {
				t.Error(err)
				return
			}
			entered <- string(value)
			if inputs.Add(1) == 1 {
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte("{}"))
		default:
			signals.Add(1)
		}
	}))
	defer server.Close()
	sb := newSandbox(nil, "synthetic", newDataPlane(server.URL, "synthetic", server.Client()))
	defer sb.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	h, err := sb.Commands().Start(ctx, "background", StartOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	first := make(chan error, 1)
	go func() { first <- h.Write(ctx, []byte("first")) }()
	select {
	case value := <-entered:
		if value != "first" {
			t.Fatal("incorrect first input")
		}
	case <-ctx.Done():
		t.Fatal("first input missing")
	}
	second, stop := context.WithTimeout(ctx, 30*time.Millisecond)
	defer stop()
	commandError(t, h.Write(second, []byte("canceled")), DeadlineExceeded)
	if inputs.Load() != 1 {
		t.Fatal("concurrent write bypassed pending ACK")
	}
	close(release)
	select {
	case err = <-first:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("first ACK missing")
	}
	if err = h.Write(ctx, []byte("third")); err != nil {
		t.Fatal(err)
	}
	select {
	case value := <-entered:
		if value != "third" {
			t.Fatal("canceled input was replayed")
		}
	case <-ctx.Done():
		t.Fatal("third input missing")
	}
	if inputs.Load() != 2 || signals.Load() != 0 {
		t.Fatal("unexpected request or signal")
	}
}
