package ags

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

type timeoutValidationControl struct {
	metricControl
	createCalls  atomic.Int32
	resumeCalls  atomic.Int32
	connectCalls atomic.Int32
	updateCalls  atomic.Int32
}

func (c *timeoutValidationControl) Create(context.Context, CreateOptions) (SandboxInfo, error) {
	c.createCalls.Add(1)
	return SandboxInfo{}, codeError(Unavailable, "StartSandboxInstance", "SYNTHETIC_STOP")
}

func (c *timeoutValidationControl) Resume(context.Context, string, ResumeOptions) (SandboxInfo, error) {
	c.resumeCalls.Add(1)
	return SandboxInfo{}, codeError(Unavailable, "ResumeSandboxInstance", "SYNTHETIC_STOP")
}

func (c *timeoutValidationControl) Connect(context.Context, string, time.Duration) (SandboxInfo, error) {
	c.connectCalls.Add(1)
	return SandboxInfo{}, codeError(Unavailable, "Sandboxes.Connect", "SYNTHETIC_STOP")
}

func (c *timeoutValidationControl) Update(context.Context, string, UpdateOptions) error {
	c.updateCalls.Add(1)
	return codeError(Unavailable, "UpdateSandboxInstance", "SYNTHETIC_STOP")
}

func timeoutTestClient(t *testing.T, control controlPlane) *Client {
	t.Helper()
	client, err := NewClient(
		WithRegion("test"),
		WithCredential(CloudCredential{"id", "key"}),
		withControlPlane(control),
		withMonitorTransport(&metricTransport{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestTimeoutValidationBeforeMutation(t *testing.T) {
	cases := []struct {
		name      string
		seconds   time.Duration
		operation string
		wantCalls int32
	}{
		{"create-29", 29 * time.Second, "create", 0},
		{"create-30", 30 * time.Second, "create", 1},
		{"create-299", 299 * time.Second, "create", 1},
		{"create-300", 300 * time.Second, "create", 1},
		{"create-fractional", 300*time.Second + time.Millisecond, "create", 0},
		{"resume-29", 29 * time.Second, "resume", 0},
		{"resume-30", 30 * time.Second, "resume", 1},
		{"resume-299", 299 * time.Second, "resume", 1},
		{"resume-300", 300 * time.Second, "resume", 1},
		{"resume-fractional", 300*time.Second + time.Millisecond, "resume", 0},
		{"update-299", 299 * time.Second, "update", 0},
		{"update-300", 300 * time.Second, "update", 1},
		{"update-fractional", 300*time.Second + time.Millisecond, "update", 0},
		{"connect-29", 29 * time.Second, "connect", 0},
		{"connect-30", 30 * time.Second, "connect", 1},
		{"connect-299", 299 * time.Second, "connect", 1},
		{"connect-300", 300 * time.Second, "connect", 1},
		{"connect-fractional", 300*time.Second + time.Millisecond, "connect", 0},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			control := &timeoutValidationControl{metricControl: metricControl{info: SandboxInfo{ID: "fixture", State: Running}}}
			client := timeoutTestClient(t, control)
			var err error
			switch test.operation {
			case "create":
				_, err = client.Sandboxes().Create(context.Background(), CreateOptions{Tool: ToolRef{Name: "fixture"}, Timeout: &test.seconds})
				if control.createCalls.Load() != test.wantCalls {
					t.Fatalf("Create calls = %d; want %d", control.createCalls.Load(), test.wantCalls)
				}
			case "resume":
				_, err = newSandbox(client, "fixture", nil).Resume(context.Background(), ResumeOptions{Timeout: &test.seconds})
				if control.resumeCalls.Load() != test.wantCalls {
					t.Fatalf("Resume calls = %d; want %d", control.resumeCalls.Load(), test.wantCalls)
				}
			case "update":
				_, err = newSandbox(client, "fixture", nil).Update(context.Background(), UpdateOptions{Timeout: &test.seconds})
				if control.updateCalls.Load() != test.wantCalls {
					t.Fatalf("Update calls = %d; want %d", control.updateCalls.Load(), test.wantCalls)
				}
			case "connect":
				_, err = client.Sandboxes().Connect(context.Background(), "fixture", ConnectOptions{Timeout: &test.seconds})
				if control.connectCalls.Load() != test.wantCalls {
					t.Fatalf("Connect calls = %d; want %d", control.connectCalls.Load(), test.wantCalls)
				}
			default:
				t.Fatal("unknown operation")
			}
			if err == nil {
				t.Fatal("synthetic operation unexpectedly succeeded")
			}
			if test.wantCalls == 0 {
				var failure *Error
				if !errors.As(err, &failure) || failure.Code != InvalidArgument || failure.Reason != "TIMEOUT_OUT_OF_RANGE" {
					t.Fatalf("validation error = %v", err)
				}
			}
		})
	}
}

func TestTimeoutTypedRequestPreservesSeconds(t *testing.T) {
	cases := []struct {
		name, action string
		seconds      time.Duration
	}{
		{"create-30", "StartSandboxInstance", 30 * time.Second},
		{"create-299", "StartSandboxInstance", 299 * time.Second},
		{"create-300", "StartSandboxInstance", 300 * time.Second},
		{"resume-30", "ResumeSandboxInstance", 30 * time.Second},
		{"resume-299", "ResumeSandboxInstance", 299 * time.Second},
		{"resume-300", "ResumeSandboxInstance", 300 * time.Second},
		{"update-300", "UpdateSandboxInstance", 300 * time.Second},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var captured string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request map[string]any
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
				}
				action := r.Header.Get("X-TC-Action")
				if action == test.action {
					value, ok := request["Timeout"].(string)
					if !ok {
						t.Errorf("%s Timeout = %#v", action, request["Timeout"])
					}
					captured = value
				}
				response := map[string]any{"RequestId": "timeout-fixture"}
				switch action {
				case "StartSandboxInstance":
					response["Instance"] = instanceJSON("RUNNING")
				case "DescribeSandboxInstanceList":
					response["InstanceSet"], response["TotalCount"] = []any{instanceJSON("RUNNING")}, 1
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"Response": response})
			}))
			defer server.Close()
			control := newTencentControlPlane("test", CloudCredential{"id", "key"}, server.URL, server.Client(), time.Second)
			var err error
			switch test.action {
			case "StartSandboxInstance":
				_, err = control.Create(context.Background(), CreateOptions{Tool: ToolRef{Name: "fixture"}, Timeout: &test.seconds})
			case "ResumeSandboxInstance":
				_, err = control.Resume(context.Background(), "sb-test", ResumeOptions{Timeout: &test.seconds})
			case "UpdateSandboxInstance":
				err = control.Update(context.Background(), "sb-test", UpdateOptions{Timeout: &test.seconds})
			}
			if err != nil {
				t.Fatal(err)
			}
			if captured != test.seconds.String() {
				t.Fatalf("captured Timeout = %q; want %q", captured, test.seconds.String())
			}
		})
	}
}

func TestConnectTimeoutUsesStateSpecificAction(t *testing.T) {
	cases := []struct {
		name         string
		state        SandboxState
		seconds      time.Duration
		wantMutation string
		wantCode     ErrorCode
	}{
		{"paused-30-resume", Paused, 30 * time.Second, "ResumeSandboxInstance", ""},
		{"paused-299-resume", Paused, 299 * time.Second, "ResumeSandboxInstance", ""},
		{"running-299-reject", Running, 299 * time.Second, "", InvalidArgument},
		{"running-300-update", Running, 300 * time.Second, "UpdateSandboxInstance", ""},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			state := test.state
			var mutations []string
			var captured string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request map[string]any
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
				}
				action := r.Header.Get("X-TC-Action")
				response := map[string]any{"RequestId": "connect-fixture"}
				switch action {
				case "DescribeSandboxInstanceList":
					instance := instanceJSON(string(state))
					instance["ExpiresAt"] = time.Now().Add(time.Minute).UTC().Format(time.RFC3339)
					response["InstanceSet"], response["TotalCount"] = []any{instance}, 1
				case "ResumeSandboxInstance", "UpdateSandboxInstance":
					mutations = append(mutations, action)
					captured, _ = request["Timeout"].(string)
					state = Running
				default:
					t.Errorf("unexpected action %q", action)
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"Response": response})
			}))
			defer server.Close()
			control := newTencentControlPlane("test", CloudCredential{"id", "key"}, server.URL, server.Client(), time.Second)
			_, err := control.Connect(context.Background(), "sb-test", test.seconds)
			if test.wantCode != "" {
				var failure *Error
				if !errors.As(err, &failure) || failure.Code != test.wantCode || len(mutations) != 0 {
					t.Fatalf("Connect error = %v; mutations = %v", err, mutations)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(mutations) != 1 || mutations[0] != test.wantMutation {
				t.Fatalf("mutations = %v; want [%s]", mutations, test.wantMutation)
			}
			if captured != test.seconds.String() {
				t.Fatalf("captured Timeout = %q; want %q", captured, test.seconds.String())
			}
		})
	}
}
