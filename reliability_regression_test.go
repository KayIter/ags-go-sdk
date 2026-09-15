package ags

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/controlplane"
)

func TestCloudPollingCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_, _ = fmt.Fprint(w, `{"Response":{"InstanceSet":[{"InstanceId":"synthetic","Status":"RUNNING"}],"TotalCount":1}}`)
		time.AfterFunc(30*time.Millisecond, cancel)
	}))
	defer server.Close()
	control := newTencentControlPlane("ap-test", CloudCredential{"synthetic", "synthetic"}, server.URL, server.Client(), time.Second)
	_, err := control.waitFor(ctx, "synthetic", Paused)
	var failure *Error
	if !errors.As(err, &failure) || failure.Code != Canceled || !errors.Is(err, context.Canceled) {
		t.Fatalf("manual cancellation = %#v; want CANCELED with context.Canceled", err)
	}
}

func TestCloudStateContract(t *testing.T) {
	raw, err := os.ReadFile("contracts/sandbox-state-cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Cases []struct {
			Wire, State     string
			TerminalFailure bool `json:"terminal_failure"`
		} `json:"cases"`
	}
	if err = json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	for _, test := range fixture.Cases {
		t.Run(test.Wire, func(t *testing.T) {
			got := instanceFrom(controlplane.Instance{ID: "synthetic", Status: test.Wire})
			if string(got.State) != test.State || (got.State == Failed) != test.TerminalFailure {
				t.Fatalf("state %s = %s; terminal=%v", test.Wire, got.State, got.State == Failed)
			}
		})
	}
}

type timeoutRewriteTransport struct {
	base     http.RoundTripper
	endpoint string
}

func (transport timeoutRewriteTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	clone := request.Clone(request.Context())
	u := *request.URL
	clone.URL = &u
	clone.URL.Scheme = "http"
	clone.URL.Host = strings.TrimPrefix(transport.endpoint, "http://")
	return transport.base.RoundTrip(clone)
}

func TestDataPlaneRequestTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Header.Get("X-TC-Action") == "AcquireSandboxInstanceToken":
			_, _ = fmt.Fprint(w, `{"Response":{"Token":"synthetic","TrafficToken":"synthetic-business"}}`)
		case r.URL.Path == "/filesystem.Filesystem/Stat":
			time.Sleep(200 * time.Millisecond)
			_, _ = fmt.Fprint(w, `{"entry":{"name":"synthetic","path":"/synthetic","type":"FILE_TYPE_FILE"}}`)
		default:
			t.Errorf("unexpected synthetic request: %s", r.URL.Path)
		}
	}))
	defer server.Close()
	client := &http.Client{Transport: timeoutRewriteTransport{base: server.Client().Transport, endpoint: server.URL}}
	sdk, err := NewClient(WithRegion("ap-test"), WithCredential(CloudCredential{"synthetic", "synthetic"}), WithHTTPClient(client), WithRequestTimeout(40*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	plane, err := sdk.cfg.control.dataPlane(ctx, "synthetic")
	if err != nil {
		t.Fatal(err)
	}
	sandbox := newSandbox(sdk, "synthetic", plane)
	defer sandbox.Close()
	started := time.Now()
	_, err = sandbox.Files().Stat(ctx, "/synthetic", StatOptions{})
	if !errors.Is(err, &Error{Code: DeadlineExceeded}) {
		t.Fatalf("Stat error = %v; want DEADLINE_EXCEEDED", err)
	}
	if elapsed := time.Since(started); elapsed >= 150*time.Millisecond {
		t.Fatalf("request deadline returned too late: %s", elapsed)
	}
}

func TestFileReadStartTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	plane := newDataPlaneWithTimeout(server.URL, "synthetic", server.Client(), 40*time.Millisecond)
	defer plane.Close()
	started := time.Now()
	_, err := plane.Read(context.Background(), "/synthetic", "")
	if !errors.Is(err, &Error{Code: DeadlineExceeded}) {
		t.Fatalf("Read error = %v; want DEADLINE_EXCEEDED", err)
	}
	if elapsed := time.Since(started); elapsed >= 150*time.Millisecond {
		t.Fatalf("read start deadline returned too late: %s", elapsed)
	}
}

func TestDeliveredFileReadHasNoIdleTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher := w.(http.Flusher)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("a"))
		flusher.Flush()
		time.Sleep(120 * time.Millisecond)
		_, _ = w.Write([]byte("b"))
	}))
	defer server.Close()
	plane := newDataPlaneWithTimeout(server.URL, "synthetic", server.Client(), 40*time.Millisecond)
	defer plane.Close()
	reader, err := plane.Read(context.Background(), "/synthetic", "")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("delivered reader acquired an SDK idle timeout: %v", err)
	}
	if got := string(content); got != "ab" {
		t.Fatalf("read content = %q; want ab", got)
	}
}
