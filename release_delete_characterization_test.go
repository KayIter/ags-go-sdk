package ags

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"
)

// This locks the desired repeated-Delete behavior without treating arbitrary
// service failures as success.
func TestReleaseDeleteCharacterization(t *testing.T) {
	for _, tc := range []struct {
		name, describeError, stopError string
		state                          SandboxState
		want                           ErrorCode
		actions                        []string
	}{
		{"stopped_is_success", "", "UnsupportedOperation.SandboxInstance", Stopped, "", []string{"DescribeSandboxInstanceList"}},
		{"failed_still_stops", "", "", Failed, "", []string{"DescribeSandboxInstanceList", "StopSandboxInstance"}},
		{"missing_is_success", "ResourceNotFound.SandboxInstance", "", Unknown, "", []string{"DescribeSandboxInstanceList"}},
		{"permission_is_preserved", "UnauthorizedOperation", "", Unknown, PermissionDenied, []string{"DescribeSandboxInstanceList"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var actions []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				action := r.Header.Get("X-TC-Action")
				mu.Lock()
				actions = append(actions, action)
				mu.Unlock()
				response := map[string]any{"RequestId": "synthetic-request"}
				code := tc.stopError
				if action == "DescribeSandboxInstanceList" {
					code = tc.describeError
					response["InstanceSet"] = []any{map[string]any{"InstanceId": "synthetic-instance", "Status": tc.state}}
				}
				if code != "" {
					response["Error"] = map[string]any{"Code": code, "Message": "synthetic rejection"}
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"Response": response})
			}))
			defer server.Close()
			client, err := NewClient(WithRegion("test"), WithCredential(CloudCredential{"synthetic", "synthetic"}), WithControlPlaneEndpoint(server.URL))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			err = client.Sandboxes().Delete(ctx, "synthetic-instance")
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				var failure *Error
				if !errors.As(err, &failure) || failure.Code != tc.want || failure.RequestID != "synthetic-request" {
					t.Fatalf("unexpected stable error: %v", err)
				}
			}
			mu.Lock()
			defer mu.Unlock()
			if !reflect.DeepEqual(actions, tc.actions) {
				t.Fatalf("request sequence %v, want %v", actions, tc.actions)
			}
		})
	}
}
