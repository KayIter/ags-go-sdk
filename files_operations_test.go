package ags

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestConformanceFilesSharedWire(t *testing.T) {
	var fixture struct {
		Cases []struct {
			ID           string          `json:"id"`
			Operation    string          `json:"operation"`
			Request      map[string]any  `json:"request"`
			Status       int             `json:"status"`
			Response     json.RawMessage `json:"response"`
			ExpectedCode ErrorCode       `json:"expectedCode"`
			User         SandboxUser     `json:"user"`
		}
	}
	body, err := os.ReadFile("contracts/filesystem-cases.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(body, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Cases) == 0 {
		t.Fatal("empty filesystem fixture")
	}
	for _, c := range fixture.Cases {
		t.Run(c.ID, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if r.Method != "POST" || r.URL.Path != "/filesystem.Filesystem/"+c.Operation {
					t.Error("wrong procedure")
				}
				if r.Header.Get("X-Access-Token") != "synthetic-instance-token" {
					t.Error("wrong credential")
				}
				expectedAuth := "Basic dXNlcjo="
				if c.User == Root {
					expectedAuth = "Basic cm9vdDo="
				}
				if r.Header.Get("Authorization") != expectedAuth {
					t.Error("wrong remote identity")
				}
				var request map[string]any
				if e := json.NewDecoder(r.Body).Decode(&request); e != nil {
					t.Error(e)
				}
				if !reflect.DeepEqual(request, c.Request) {
					t.Errorf("wire request mismatch: %v", request)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(c.Status)
				_, _ = w.Write(c.Response)
			}))
			defer server.Close()
			plane := newDataPlane(server.URL, "synthetic-instance-token", server.Client())
			sandbox := newSandbox(nil, "synthetic", plane)
			defer sandbox.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			var info FileInfo
			var callErr error
			path, _ := c.Request["path"].(string)
			switch c.Operation {
			case "Stat":
				info, callErr = sandbox.Files().Stat(ctx, path, StatOptions{User: c.User})
			case "MakeDir":
				info, callErr = sandbox.Files().MakeDir(ctx, path, MakeDirOptions{User: c.User})
			case "Move":
				info, callErr = sandbox.Files().Move(ctx, c.Request["source"].(string), c.Request["destination"].(string), MoveOptions{User: c.User})
			case "Remove":
				callErr = sandbox.Files().Remove(ctx, path, RemoveOptions{User: c.User})
			default:
				t.Fatal("unknown operation")
			}
			if c.ExpectedCode != "" {
				var sdk *Error
				if !errors.As(callErr, &sdk) || sdk.Code != c.ExpectedCode {
					t.Fatalf("error mismatch: %v", callErr)
				}
				if strings.Contains(callErr.Error(), "private response") {
					t.Fatal("response leaked")
				}
			} else {
				if callErr != nil {
					t.Fatal(callErr)
				}
				if c.Operation != "Remove" && info.Path == "" {
					t.Fatal("entry missing")
				}
			}
			if requests.Load() != 1 {
				t.Fatalf("expected exactly one request, got %d", requests.Load())
			}
		})
	}
}

func TestConformanceFilesPreflightAndGeneration(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"entry":{"path":"/fixture/item"}}`))
	}))
	defer server.Close()
	plane := newDataPlane(server.URL, "synthetic", server.Client())
	sandbox := newSandbox(nil, "synthetic", plane)
	ctx := context.Background()
	for _, path := range []string{"", "bad\x00path"} {
		_, err := sandbox.Files().Stat(ctx, path, StatOptions{})
		var sdk *Error
		if !errors.As(err, &sdk) || sdk.Code != InvalidArgument {
			t.Fatal("invalid path accepted")
		}
	}
	if _, err := sandbox.Files().MakeDir(ctx, "/fixture", MakeDirOptions{User: "invalid"}); err == nil {
		t.Fatal("invalid user accepted")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := sandbox.Files().Stat(canceled, "/fixture", StatOptions{}); err == nil {
		t.Fatal("cancellation ignored")
	}
	if err := plane.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := sandbox.Files().Stat(ctx, "/fixture", StatOptions{}); err == nil {
		t.Fatal("invalidated generation used")
	}
	_ = sandbox.Close()
	if err := sandbox.Files().Remove(ctx, "/fixture", RemoveOptions{}); err == nil {
		t.Fatal("closed sandbox used")
	}
	if requests.Load() != 0 {
		t.Fatal("preflight/cancellation sent request")
	}
}
