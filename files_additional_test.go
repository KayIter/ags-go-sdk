package ags

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFilesExistsAndAttributes(t *testing.T) {
	cases := []struct {
		name       string
		status     int
		response   map[string]any
		wantExists bool
		wantOwner  string
		wantGroup  string
		wantCode   ErrorCode
	}{
		{
			name:       "exists-preserves-owner-group",
			status:     http.StatusOK,
			response:   map[string]any{"entry": map[string]any{"path": "/fixture", "type": "FILE_TYPE_FILE", "owner": "alice", "group": "agents"}},
			wantExists: true,
			wantOwner:  "alice",
			wantGroup:  "agents",
		},
		{name: "not-found", status: http.StatusNotFound, response: map[string]any{"code": "not_found"}},
		{name: "permission-preserved", status: http.StatusForbidden, response: map[string]any{"code": "permission_denied"}, wantCode: PermissionDenied},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/filesystem.Filesystem/Stat" {
					t.Errorf("path = %s", r.URL.Path)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(test.status)
				_ = json.NewEncoder(w).Encode(test.response)
			}))
			defer server.Close()
			sandbox := newSandbox(nil, "fixture", newDataPlane(server.URL, "synthetic", server.Client()))
			defer sandbox.Close()
			exists, err := sandbox.Files().Exists(context.Background(), "/fixture", ExistsOptions{})
			if test.wantCode != "" {
				var failure *Error
				if !errors.As(err, &failure) || failure.Code != test.wantCode || exists {
					t.Fatalf("Exists = %v, %v", exists, err)
				}
				return
			}
			if err != nil || exists != test.wantExists {
				t.Fatalf("Exists = %v, %v; want %v", exists, err, test.wantExists)
			}
			if exists {
				info, err := sandbox.Files().Stat(context.Background(), "/fixture", StatOptions{})
				if err != nil || info.Owner != test.wantOwner || info.Group != test.wantGroup {
					t.Fatalf("Stat = %+v, %v", info, err)
				}
			}
		})
	}
}

func TestSandboxGetHost(t *testing.T) {
	client := timeoutTestClient(t, metricControl{})
	sandbox := newSandbox(client, "sb-123", nil)
	cases := []struct {
		name string
		port int
		want string
	}{
		{"minimum", 1, "1-sb-123.test.tencentags.com"},
		{"envd", 49983, "49983-sb-123.test.tencentags.com"},
		{"maximum", 65535, "65535-sb-123.test.tencentags.com"},
		{"zero", 0, ""},
		{"above-maximum", 65536, ""},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			got, err := sandbox.GetHost(test.port)
			if test.want == "" {
				var failure *Error
				if !errors.As(err, &failure) || failure.Code != InvalidArgument || failure.Reason != "PORT_OUT_OF_RANGE" {
					t.Fatalf("GetHost = %q, %v", got, err)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("GetHost = %q, %v; want %q", got, err, test.want)
			}
		})
	}
}
