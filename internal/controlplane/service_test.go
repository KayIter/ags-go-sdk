package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/model"
)

func TestServiceOwnsControlAndMonitorOrchestration(t *testing.T) {
	state := "RUNNING"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		action := r.Header.Get("X-Tc-Action")
		response := map[string]any{"RequestId": "request-fixture"}
		switch action {
		case "StartSandboxInstance":
			response["Instance"] = serviceInstance(state)
		case "DescribeSandboxInstanceList":
			response["InstanceSet"] = []any{serviceInstance(state)}
			response["TotalCount"] = 1
		case "PauseSandboxInstance":
			state = "PAUSED"
		case "ResumeSandboxInstance":
			state = "RUNNING"
		case "UpdateSandboxInstance", "StopSandboxInstance":
		case "GetMonitorData":
			response["Period"] = 60
			response["DataPoints"] = []any{map[string]any{"Timestamps": []int64{1}, "Values": []float64{2.5}}}
		default:
			t.Fatalf("unexpected action %q", action)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"Response": response})
	}))
	defer server.Close()

	service := NewService(ServiceConfig{Region: "ap-test", ControlEndpoint: server.URL, MonitorEndpoint: server.URL, Timeout: time.Second, HTTPClient: server.Client(), Credential: func(context.Context) (Credential, error) {
		return Credential{SecretID: "synthetic-id", SecretKey: "synthetic-key"}, nil
	}})
	timeout := 5 * time.Minute
	created, err := service.Create(context.Background(), CreateRequest{ToolName: "tool", Timeout: &timeout, Env: map[string]string{"A": "B"}})
	if err != nil || created.ID != "sandbox-fixture" || created.State != StateRunning {
		t.Fatalf("Create = %+v, %v", created, err)
	}
	got, err := service.Get(context.Background(), created.ID)
	if err != nil || got.ToolID != "tool-fixture" {
		t.Fatalf("Get = %+v, %v", got, err)
	}
	page, err := service.List(context.Background(), ListRequest{Limit: 20, States: []State{StateCreating}})
	if err != nil || page.TotalCount != 1 || len(page.Items) != 1 {
		t.Fatalf("List = %+v, %v", page, err)
	}
	connected, err := service.Connect(context.Background(), created.ID, timeout)
	if err != nil || connected.State != StateRunning {
		t.Fatalf("Connect = %+v, %v", connected, err)
	}
	paused, err := service.Pause(context.Background(), created.ID, false)
	if err != nil || paused.State != StatePaused {
		t.Fatalf("Pause = %+v, %v", paused, err)
	}
	resumed, err := service.Resume(context.Background(), created.ID, &timeout)
	if err != nil || resumed.State != StateRunning {
		t.Fatalf("Resume = %+v, %v", resumed, err)
	}
	if err = service.Update(context.Background(), created.ID, &timeout, map[string]string{"job": "fixture"}); err != nil {
		t.Fatal(err)
	}
	metric, err := service.QueryMetric(context.Background(), MetricRequest{Name: "SandboxMemoryUsedBytes", InstanceID: created.ID, ToolID: created.ToolID, Start: time.Unix(0, 0), End: time.Unix(60, 0), Period: time.Minute})
	if err != nil || metric.Resolution != time.Minute || len(metric.Points) != 1 || metric.RequestID != "request-fixture" {
		t.Fatalf("QueryMetric = %+v, %v", metric, err)
	}
	if err = service.Delete(context.Background(), created.ID); err != nil {
		t.Fatal(err)
	}
}

func serviceInstance(state string) map[string]any {
	return map[string]any{"InstanceId": "sandbox-fixture", "ToolId": "tool-fixture", "ToolName": "tool", "Status": state, "CreateTime": "2026-09-01T00:00:00Z", "ExpiresAt": time.Now().Add(time.Hour).UTC().Format(time.RFC3339), "Metadata": []any{map[string]any{"Name": "job", "Value": "old"}}}
}

func TestCloudErrorClassification(t *testing.T) {
	for _, test := range []struct{ cloud, want string }{
		{"AuthFailure.SignatureFailure", model.Unauthenticated},
		{"UnauthorizedOperation", model.PermissionDenied},
		{"ResourceNotFound.Instance", model.NotFound},
		{"ResourceInUse.Conflict", model.Conflict},
		{"RequestLimitExceeded", model.ResourceExhausted},
		{"InvalidParameterValue.Timeout", model.InvalidArgument},
		{"ClientError.MalformedResponse", model.Protocol},
	} {
		t.Run(test.cloud, func(t *testing.T) {
			err := cloudError(&Error{Code: test.cloud, RequestID: "request-fixture"}, "fixture")
			var typed *model.Error
			if !errors.As(err, &typed) || typed.Code != test.want || typed.RequestID != "request-fixture" {
				t.Fatalf("cloudError = %#v", err)
			}
		})
	}
}

func TestContextErrorPreservesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := contextError(ctx, "fixture"); !errors.Is(err, context.Canceled) {
		t.Fatalf("contextError = %v", err)
	}
}
