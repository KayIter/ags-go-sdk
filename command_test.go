package ags

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func writeCommandFrame(w http.ResponseWriter, flags byte, body []byte) {
	var header [5]byte
	header[0] = flags
	binary.BigEndian.PutUint32(header[1:], uint32(len(body)))
	_, _ = w.Write(header[:])
	_, _ = w.Write(body)
	w.(http.Flusher).Flush()
}
func TestConformanceCommandShared(t *testing.T) {
	var fixture struct {
		Cases []struct {
			ID              string            `json:"id"`
			Limit           int64             `json:"limit"`
			Frames          []json.RawMessage `json:"frames"`
			Stage           string            `json:"stage"`
			Code            ErrorCode         `json:"code"`
			Reason          string            `json:"reason"`
			Stdout          string            `json:"stdout"`
			Stderr          string            `json:"stderr"`
			ExitCode        int               `json:"exitCode"`
			StdoutTruncated bool              `json:"stdoutTruncated"`
			StderrTruncated bool              `json:"stderrTruncated"`
		}
	}
	b, err := os.ReadFile("contracts/command-cases.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(b, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Cases) == 0 {
		t.Fatal("empty cases")
	}
	for _, c := range fixture.Cases {
		t.Run(c.ID, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Path != "/process.Process/Start" || r.Method != "POST" {
					t.Error("unexpected mutation")
				}
				if r.Header.Get("X-Access-Token") != "synthetic" || r.Header.Get("Authorization") != "Basic cm9vdDo=" {
					t.Error("identity mismatch")
				}
				body, e := io.ReadAll(r.Body)
				if e != nil || len(body) < 5 {
					t.Error("bad envelope")
					return
				}
				var request map[string]any
				if e = json.Unmarshal(body[5:], &request); e != nil {
					t.Error(e)
					return
				}
				expected := map[string]any{"process": map[string]any{"cmd": "sh", "args": []any{"-c", "example"}, "envs": map[string]any{"KEY": "value"}, "cwd": "/tmp"}}
				if !reflect.DeepEqual(request, expected) {
					t.Errorf("request=%v", request)
				}
				w.Header().Set("Content-Type", "application/connect+json")
				w.WriteHeader(200)
				for _, frame := range c.Frames {
					writeCommandFrame(w, 0, frame)
				}
				writeCommandFrame(w, 2, []byte(`{}`))
			}))
			defer server.Close()
			sb := newSandbox(nil, "synthetic", newDataPlane(server.URL, "synthetic", server.Client()))
			defer sb.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			h, e := sb.Commands().Start(ctx, "sh", StartOptions{Args: []string{"-c", "example"}, Env: map[string]string{"KEY": "value"}, Cwd: "/tmp", User: Root, MaxOutputBytes: c.Limit})
			if c.Stage != "start" {
				if e != nil {
					t.Fatal(e)
				}
				defer h.Close()
				if h.PID() != 42 {
					t.Fatal("PID barrier failed")
				}
				var result CommandResult
				result, e = h.Wait(ctx)
				if c.Code == "" {
					if e != nil {
						t.Fatal(e)
					}
					if string(result.Stdout) != c.Stdout || string(result.Stderr) != c.Stderr || result.ExitCode != c.ExitCode || result.StdoutTruncated != c.StdoutTruncated || result.StderrTruncated != c.StderrTruncated {
						t.Fatalf("result=%+v", result)
					}
					if len(result.Stdout) > 0 {
						result.Stdout[0] = '!'
					}
					_ = h.Close()
					again, e := h.Wait(ctx)
					if e != nil || string(again.Stdout) != c.Stdout || h.Err() != nil {
						t.Fatal("completed result not preserved")
					}
				}
			}
			if c.Code != "" {
				var sdk *Error
				if !errors.As(e, &sdk) || sdk.Code != c.Code || sdk.Reason != c.Reason {
					t.Fatalf("expected %s/%s got %v", c.Code, c.Reason, e)
				}
			}
			if calls.Load() != 1 {
				t.Fatal("unexpected retry or signal")
			}
		})
	}
}
