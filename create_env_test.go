package ags

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type createEnvironmentCase struct {
	ID    string            `json:"id"`
	Env   map[string]string `json:"env"`
	Valid bool              `json:"valid"`
	Code  ErrorCode         `json:"publicCode"`
	Calls int32             `json:"calls"`
	Wire  any               `json:"wire"`
}

func loadCreateEnvironmentCases(t *testing.T) []createEnvironmentCase {
	t.Helper()
	data, err := os.ReadFile("contracts/create-env-cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Cases []createEnvironmentCase `json:"cases"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Cases) != 9 {
		t.Fatalf("Create Env case count = %d; want 9", len(fixture.Cases))
	}
	return fixture.Cases
}

func TestCloudCreateEnvironmentContract(t *testing.T) {
	for _, test := range loadCreateEnvironmentCases(t) {
		t.Run(test.ID, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Header.Get("X-TC-Action") != "StartSandboxInstance" || r.Header.Get("Authorization") == "" {
					t.Error("Create did not use the signed typed Cloud action")
				}
				var request map[string]any
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
				}
				if test.Wire == nil {
					if _, exists := request["CustomConfiguration"]; exists {
						t.Error("empty Env emitted CustomConfiguration")
					}
				} else {
					configuration, ok := request["CustomConfiguration"].(map[string]any)
					if !ok || !reflect.DeepEqual(configuration["Env"], test.Wire) || len(configuration) != 1 {
						t.Errorf("Env wire mapping = %#v; want %#v", configuration, test.Wire)
					}
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"Response": map[string]any{"RequestId": "create-fixture", "Error": map[string]any{"Code": "ResourceNotFound", "Message": "synthetic"}}})
			}))
			defer server.Close()

			client, err := NewClient(WithRegion("test"), WithCredential(CloudCredential{"id", "key"}), WithControlPlaneEndpoint(server.URL), WithHTTPClient(server.Client()))
			if err != nil {
				t.Fatal(err)
			}
			opts := CreateOptions{Tool: ToolRef{Name: "fixture"}, Env: test.Env}
			_, err = client.Sandboxes().Create(context.Background(), opts)
			var failure *Error
			if !errors.As(err, &failure) || failure.Code != test.Code {
				t.Fatalf("error = %v; want %s", err, test.Code)
			}
			if calls.Load() != test.Calls {
				t.Fatalf("HTTP calls = %d; want %d", calls.Load(), test.Calls)
			}
			if !test.Valid && failure.Reason != "ENV_INVALID" {
				t.Fatalf("reason = %s; want ENV_INVALID", failure.Reason)
			}
			for _, value := range []string{fmt.Sprintf("%v", opts), fmt.Sprintf("%+v", opts), fmt.Sprintf("%#v", opts), err.Error()} {
				if strings.Contains(value, "fixture-sensitive-value") {
					t.Fatal("Create Env value leaked through formatting or error")
				}
			}
		})
	}
}

func TestCloudCreateCapturedRequest(t *testing.T) {
	readOnly := false
	timeout := 30 * time.Second
	var request map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-TC-Action") != "StartSandboxInstance" {
			t.Errorf("action = %q", r.Header.Get("X-TC-Action"))
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"Response": map[string]any{"Instance": map[string]any{"InstanceId": "fixture", "ToolId": "tool-id", "Status": "RUNNING"}}})
	}))
	defer server.Close()
	client, err := NewClient(WithRegion("test"), WithCredential(CloudCredential{"id", "key"}), WithControlPlaneEndpoint(server.URL), WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatal(err)
	}
	info, err := client.cfg.control.Create(context.Background(), CreateOptions{
		Tool:        ToolRef{ID: "tool-id"},
		Timeout:     &timeout,
		ClientToken: "idempotency-token",
		Env:         map[string]string{"Z": "last", "A": ""},
		Metadata:    map[string]string{"z": "last", "a": ""},
		MountOptions: []MountOption{{
			Name: "workspace", MountPath: "/workspace", SubPath: "job", ReadOnly: &readOnly,
		}},
		AuthMode: SandboxAuthPublic,
	})
	if err != nil || info.ID != "fixture" {
		t.Fatalf("Create = %+v, %v", info, err)
	}
	want := map[string]any{
		"ToolId":      "tool-id",
		"Timeout":     "30s",
		"ClientToken": "idempotency-token",
		"CustomConfiguration": map[string]any{"Env": []any{
			map[string]any{"Name": "A", "Value": ""},
			map[string]any{"Name": "Z", "Value": "last"},
		}},
		"MountOptions": []any{map[string]any{"Name": "workspace", "MountPath": "/workspace", "SubPath": "job", "ReadOnly": false}},
		"AuthMode":     "PUBLIC",
		"Metadata": []any{
			map[string]any{"Name": "a", "Value": ""},
			map[string]any{"Name": "z", "Value": "last"},
		},
	}
	if !reflect.DeepEqual(request, want) {
		t.Fatalf("captured Create request = %#v; want %#v", request, want)
	}
}
