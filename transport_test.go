package ags

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSharedControlPlaneUsesFrozenActionsAndFields(t *testing.T) {
	var mu sync.Mutex
	state := "RUNNING"
	var startBody map[string]any
	seen := map[string]bool{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			t.Error("missing TC3 authorization")
		}
		if r.Header.Get("X-Tc-Token") != "" {
			t.Error("long-lived credential unexpectedly sent a temporary token")
		}
		var input map[string]any
		_ = json.NewDecoder(r.Body).Decode(&input)
		action := r.Header.Get("X-Tc-Action")
		checkFrozenControlRequest(t, action, input)
		response := map[string]any{"RequestId": "request-test"}
		mu.Lock()
		defer mu.Unlock()
		seen[action] = true
		switch action {
		case "StartSandboxInstance":
			startBody = input
			response["Instance"] = instanceJSON(state)
		case "DescribeSandboxInstanceList":
			response["InstanceSet"] = []any{instanceJSON(state)}
			response["TotalCount"] = 1
		case "PauseSandboxInstance":
			if input["Memory"] != false {
				t.Errorf("disk pause did not map Memory=false: %#v", input)
			}
			state = "PAUSED"
		case "ResumeSandboxInstance":
			state = "RUNNING"
		case "StopSandboxInstance":
			state = "STOPPED"
		case "AcquireSandboxInstanceToken":
			response["Token"] = "instance-token"
		case "UpdateSandboxInstance":
		default:
			t.Errorf("unexpected action %q", action)
		}
		checkFrozenControlResponse(t, action, response)
		_ = json.NewEncoder(w).Encode(map[string]any{"Response": response})
	}))
	defer server.Close()
	cp := newTencentControlPlane("ap-test", CloudCredential{"id", "key"}, server.URL, server.Client(), time.Second)
	timeout := time.Minute
	created, err := cp.Create(context.Background(), CreateOptions{Tool: ToolRef{Name: "tool"}, Timeout: &timeout, ClientToken: "token", Metadata: map[string]string{"job": "e2e"}})
	if err != nil || created.ID != "sb-test" {
		t.Fatalf("create=%+v err=%v", created, err)
	}
	if startBody["ToolName"] != "tool" || startBody["ClientToken"] != "token" {
		t.Fatalf("start body=%#v", startBody)
	}
	metadata, ok := startBody["Metadata"].([]any)
	if !ok || len(metadata) != 1 {
		t.Fatalf("metadata=%#v", startBody["Metadata"])
	}
	if _, err = cp.Pause(context.Background(), "sb-test", PauseOptions{Mode: PauseDisk}); err != nil {
		t.Fatal(err)
	}
	if _, err = cp.Resume(context.Background(), "sb-test", ResumeOptions{}); err != nil {
		t.Fatal(err)
	}
	page, err := cp.List(context.Background(), SandboxListOptions{Limit: 20})
	if err != nil || page.TotalCount != 1 || page.Items[0].Metadata["job"] != "e2e" {
		t.Fatalf("page=%+v err=%v", page, err)
	}
	updateTimeout := 5 * time.Minute
	if err = cp.Update(context.Background(), "sb-test", UpdateOptions{Timeout: &updateTimeout}); err != nil {
		t.Fatal(err)
	}
	generation, err := cp.service.Runtime(context.Background(), "sb-test")
	if err != nil {
		t.Fatal(err)
	}
	if err = generation.Close(); err != nil {
		t.Fatal(err)
	}
	if err = cp.Delete(context.Background(), "sb-test"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if len(seen) != 7 {
		t.Errorf("captured %d control-plane actions, want all 7: %v", len(seen), seen)
	}
	mu.Unlock()
}

func checkFrozenControlRequest(t *testing.T, action string, input map[string]any) {
	t.Helper()
	contract := readControlPlaneContract(t)
	actionShape, ok := contract.Actions[action]
	if !ok {
		t.Errorf("action %q is outside the SDK control-plane allowlist", action)
		return
	}
	shape, ok := contract.Objects[actionShape.Input]
	if !ok {
		t.Errorf("action %q input object %q is missing", action, actionShape.Input)
		return
	}
	fields := map[string]controlPlaneMember{}
	for _, member := range shape.Members {
		fields[member.Name] = member
	}
	for key := range input {
		if _, allowed := fields[key]; !allowed {
			t.Errorf("unexpected %s request field %q", action, key)
		}
	}
	for name, member := range fields {
		if member.Required {
			if _, present := input[name]; !present {
				t.Errorf("required %s request field %q is missing", action, name)
			}
		}
	}
}

func checkFrozenControlResponse(t *testing.T, action string, output map[string]any) {
	t.Helper()
	contract := readControlPlaneContract(t)
	actionShape, ok := contract.Actions[action]
	if !ok {
		t.Errorf("action %q is outside the SDK control-plane allowlist", action)
		return
	}
	shape, ok := contract.Objects[actionShape.Output]
	if !ok {
		t.Errorf("action %q output object %q is missing", action, actionShape.Output)
		return
	}
	fields := map[string]controlPlaneMember{}
	for _, member := range shape.Members {
		fields[member.Name] = member
	}
	for key := range output {
		if _, allowed := fields[key]; !allowed {
			t.Errorf("unexpected %s response field %q", action, key)
		}
	}
	for name, member := range fields {
		if member.OutputRequired {
			if _, present := output[name]; !present {
				t.Errorf("required %s response field %q is missing", action, name)
			}
		}
	}
}

type controlPlaneContract struct {
	Actions map[string]struct {
		Input  string `json:"input"`
		Output string `json:"output"`
	} `json:"actions"`
	Objects map[string]struct {
		Members []controlPlaneMember `json:"members"`
	} `json:"objects"`
}

type controlPlaneMember struct {
	Name           string `json:"name"`
	Required       bool   `json:"required"`
	OutputRequired bool   `json:"output_required"`
}

func readControlPlaneContract(t *testing.T) controlPlaneContract {
	t.Helper()
	return readContract[controlPlaneContract](t, "contracts/controlplane/ags/v20250920/effective.json")
}

func instanceJSON(state string) map[string]any {
	return map[string]any{"InstanceId": "sb-test", "ToolId": "tool-id", "ToolName": "tool", "Status": state, "CreateTime": "2026-09-04T00:00:00Z", "ExpiresAt": "2026-09-04T01:00:00Z", "Metadata": []any{map[string]any{"Name": "job", "Value": "e2e"}}}
}

func TestTencentMonitorCallsPublicGetMonitorData(t *testing.T) {
	var input map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Tc-Action") != "GetMonitorData" || r.Header.Get("X-Tc-Version") != "2018-07-24" {
			t.Errorf("headers=%v", r.Header)
		}
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &input)
		_ = json.NewEncoder(w).Encode(map[string]any{"Response": map[string]any{"Period": 60, "RequestId": "monitor-request", "DataPoints": []any{map[string]any{"Timestamps": []int64{2, 1}, "Values": []float64{20, 10}}}}})
	}))
	defer server.Close()
	monitor := newTencentMonitorWithConfig("ap-test", CloudCredential{"id", "key"}, server.URL, server.Client(), 30*time.Second)
	response, err := monitor.Query(context.Background(), monitorRequest{Metric: SandboxMemoryUsedBytes, InstanceID: "sb", ToolID: "tool", Start: time.Unix(0, 0), End: time.Unix(120, 0), Period: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if input["Namespace"] != "QCE/AGS" || input["MetricName"] != "SandboxMemoryUsedBytes" {
		t.Fatalf("payload=%#v", input)
	}
	instances := input["Instances"].([]any)
	dimensions := instances[0].(map[string]any)["Dimensions"].([]any)
	if len(dimensions) != 2 {
		t.Fatalf("dimensions=%#v", dimensions)
	}
	if response.Resolution != time.Minute || response.RequestID != "monitor-request" || len(response.Points) != 2 {
		t.Fatalf("response=%+v", response)
	}
}

func TestTemporaryCloudCredentialsReachOfficialControlAndMonitor(t *testing.T) {
	var tokens []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokens = append(tokens, r.Header.Get("X-Tc-Token"))
		if r.Header.Get("X-Tc-Action") == "GetMonitorData" {
			_ = json.NewEncoder(w).Encode(map[string]any{"Response": map[string]any{"Period": 60, "DataPoints": []any{}}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"Response": map[string]any{"InstanceSet": []any{instanceJSON("RUNNING")}, "TotalCount": 1}})
	}))
	defer server.Close()
	credential := TemporaryCloudCredential{SecretID: "id", SecretKey: "key", Token: "session-token"}
	control := newTencentControlPlane("ap-test", credential, server.URL, server.Client(), time.Second)
	if _, err := control.Get(context.Background(), "sb-test"); err != nil {
		t.Fatal(err)
	}
	monitor := newTencentMonitorWithConfig("ap-test", credential, server.URL, server.Client(), time.Second)
	if _, err := monitor.Query(context.Background(), monitorRequest{Metric: SandboxCPUUsagePercent, InstanceID: "sb-test", Start: time.Unix(0, 0), End: time.Unix(60, 0), Period: time.Minute}); err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 2 || tokens[0] != "session-token" || tokens[1] != "session-token" {
		t.Fatalf("temporary token headers=%q", tokens)
	}
}

func TestCloudErrorsMapWithoutLeakingResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"Response": map[string]any{"RequestId": "r", "Error": map[string]any{"Code": "ResourceNotFound.Instance", "Message": "sensitive body"}}})
	}))
	defer server.Close()
	cp := newTencentControlPlane("ap-test", CloudCredential{"id", "key"}, server.URL, server.Client(), time.Second)
	_, err := cp.Get(context.Background(), "missing")
	typed, ok := err.(*Error)
	if !ok || typed.Code != NotFound || typed.RequestID != "r" {
		t.Fatalf("error=%#v", err)
	}
	if strings.Contains(err.Error(), "sensitive body") {
		t.Fatal("error leaked response message")
	}
}
