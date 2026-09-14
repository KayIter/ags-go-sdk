package ags

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestConformanceFilesInflightCancellation(t *testing.T) {
	for _, mode := range []string{"caller", "generation"} {
		for _, operation := range []string{"Stat", "MakeDir", "Move", "Remove"} {
			t.Run(mode+"/"+operation, func(t *testing.T) {
				var calls atomic.Int32
				accepted, disconnected := make(chan struct{}, 1), make(chan struct{}, 1)
				release := make(chan struct{})
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					_, _ = io.Copy(io.Discard, r.Body)
					if r.URL.Path != "/filesystem.Filesystem/"+operation {
						t.Error("incorrect file procedure")
					}
					accepted <- struct{}{}
					select {
					case <-r.Context().Done():
						disconnected <- struct{}{}
					case <-release:
					}
				}))
				defer server.Close()
				defer close(release)
				plane := newDataPlane(server.URL, "synthetic", server.Client())
				sandbox := newSandbox(nil, "synthetic", plane)
				defer sandbox.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				result := make(chan error, 1)
				go func() {
					var e error
					switch operation {
					case "Stat":
						_, e = sandbox.Files().Stat(ctx, "/owned/source", StatOptions{})
					case "MakeDir":
						_, e = sandbox.Files().MakeDir(ctx, "/owned/source", MakeDirOptions{})
					case "Move":
						_, e = sandbox.Files().Move(ctx, "/owned/source", "/owned/destination", MoveOptions{})
					case "Remove":
						e = sandbox.Files().Remove(ctx, "/owned/source", RemoveOptions{})
					}
					result <- e
				}()
				select {
				case <-accepted:
				case <-ctx.Done():
					t.Fatal("request did not reach server")
				}
				expected := Canceled
				if mode == "caller" {
					cancel()
				} else {
					expected = InstancePaused
					if e := plane.Close(); e != nil {
						t.Fatal(e)
					}
				}
				select {
				case e := <-result:
					var sdk *Error
					if !errors.As(e, &sdk) || sdk.Code != expected {
						t.Fatalf("expected %s, got %v", expected, e)
					}
				case <-time.After(time.Second):
					t.Fatal("in-flight request did not stop")
				}
				select {
				case <-disconnected:
				case <-time.After(time.Second):
					t.Fatal("HTTP request not canceled")
				}
				if calls.Load() != 1 {
					t.Fatalf("mutation retried: %d", calls.Load())
				}
			})
		}
	}
}
