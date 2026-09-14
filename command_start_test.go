package ags

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestConformanceCommandBeforePID(t *testing.T) {
	for _, mode := range []string{"canceled-before-send", "cancel-stream", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			var calls, signals atomic.Int32
			seen, closed := make(chan struct{}), make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Path != "/process.Process/Start" {
					signals.Add(1)
					return
				}
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "application/connect+json")
				writeCommandFrame(w, 0, []byte(`{"event":{"keepalive":{}}}`))
				close(seen)
				<-r.Context().Done()
				close(closed)
			}))
			defer server.Close()
			sb := newSandbox(nil, "synthetic", newDataPlane(server.URL, "synthetic", server.Client()))
			defer sb.Close()
			timeout := 3 * time.Second
			if mode == "deadline" {
				timeout = 150 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			if mode == "canceled-before-send" {
				cancel()
			}
			result := make(chan error, 1)
			go func() {
				h, err := sb.Commands().Start(ctx, "background", StartOptions{})
				if h != nil {
					_ = h.Close()
					t.Error("delivered handle without PID")
				}
				result <- err
			}()
			if mode != "canceled-before-send" {
				select {
				case <-seen:
				case <-time.After(2 * time.Second):
					t.Fatal("request not observed")
				}
				if mode == "cancel-stream" {
					cancel()
				}
			}
			var err error
			select {
			case err = <-result:
			case <-time.After(3 * time.Second):
				t.Fatal("Start did not terminate")
			}
			code := Canceled
			if mode == "deadline" {
				code = DeadlineExceeded
			}
			commandError(t, err, code)
			expected := int32(0)
			if mode != "canceled-before-send" {
				expected = 1
				select {
				case <-closed:
				case <-time.After(2 * time.Second):
					t.Fatal("HTTP request not closed")
				}
			}
			if calls.Load() != expected || signals.Load() != 0 {
				t.Fatal("unexpected request, retry or signal")
			}
		})
	}
}
