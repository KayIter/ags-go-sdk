package ags

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestConformanceCommandEndBeforeInputACK(t *testing.T) {
	seen, release, end := make(chan struct{}), make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		switch r.URL.Path {
		case "/process.Process/Start":
			w.Header().Set("Content-Type", "application/connect+json")
			writeCommandFrame(w, 0, []byte(`{"event":{"start":{"pid":42}}}`))
			select {
			case <-end:
				writeCommandFrame(w, 0, []byte(`{"event":{"end":{"exitCode":7,"exited":true}}}`))
			case <-r.Context().Done():
			}
		case "/process.Process/SendInput":
			close(seen)
			select {
			case <-release:
			case <-r.Context().Done():
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{}`))
		default:
			t.Error("unexpected request")
		}
	}))
	defer server.Close()
	defer close(release)
	sb := newSandbox(nil, "synthetic", newDataPlane(server.URL, "synthetic", server.Client()))
	defer sb.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	h, e := sb.Commands().Start(ctx, "background", StartOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer h.Close()
	ack := make(chan error, 1)
	go func() { ack <- h.Write(ctx, []byte("hi")) }()
	select {
	case <-seen:
	case <-ctx.Done():
		t.Fatal("input request missing")
	}
	close(end)
	r, e := h.Wait(ctx)
	if e != nil || r.ExitCode != 7 {
		t.Fatalf("exit=%+v err=%v", r, e)
	}
	select {
	case e := <-ack:
		commandError(t, e, Canceled)
	case <-ctx.Done():
		t.Fatal("pending input did not stop")
	}
	r, e = h.Wait(ctx)
	if e != nil || r.ExitCode != 7 || h.Err() != nil {
		t.Fatal("late input failure overwrote End")
	}
}
func TestConformanceCommandFrameBound(t *testing.T) {
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
	f.emit <- nil
	_, e = h.Wait(ctx)
	commandError(t, e, ResourceExhausted)
	if f.signals.Load() != 0 {
		t.Fatal("oversized frame sent kill")
	}
}
