package ags

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/controlplane"
)

func readContract[T any](t *testing.T, path string) T {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var value T
	if err = json.Unmarshal(data, &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func TestControlPlaneActionRegistryMatchesContract(t *testing.T) {
	contract := readContract[struct {
		Actions []struct {
			Name  string `json:"name"`
			Route string `json:"route"`
		} `json:"actions"`
	}](t, "contracts/control-plane.json")
	expected := map[string]controlplane.ActionRoute{}
	for _, action := range contract.Actions {
		if _, exists := expected[action.Name]; exists {
			t.Fatalf("duplicate action %s", action.Name)
		}
		expected[action.Name] = controlplane.ActionRoute(action.Route)
	}
	if !reflect.DeepEqual(controlplane.ActionRoutes(), expected) {
		t.Fatalf("action routing drift: got=%v want=%v", controlplane.ActionRoutes(), expected)
	}
}

func TestMetricsRegistryMatchesContract(t *testing.T) {
	contract := readContract[struct {
		Metrics []struct {
			Name string     `json:"name"`
			Unit MetricUnit `json:"unit"`
		} `json:"metrics"`
	}](t, "contracts/metrics.json")
	expected := map[SandboxMetricName]MetricUnit{}
	for _, metric := range contract.Metrics {
		expected[SandboxMetricName(metric.Name)] = metric.Unit
	}
	if len(expected) != 10 || !reflect.DeepEqual(expected, metricUnits) {
		t.Fatalf("metrics contract drift: got=%v want=%v", metricUnits, expected)
	}
}

func TestErrorCodesMatchContract(t *testing.T) {
	contract := readContract[struct {
		Codes []ErrorCode `json:"codes"`
	}](t, "contracts/errors.json")
	actual := []ErrorCode{InvalidArgument, Unauthenticated, PermissionDenied, NotFound, Conflict, ResourceExhausted, DeadlineExceeded, Canceled, Unavailable, InstanceNotReady, InstancePaused, Protocol, Internal, Unsupported}
	if !reflect.DeepEqual(actual, contract.Codes) {
		t.Fatalf("error-code contract drift: got=%v want=%v", actual, contract.Codes)
	}
}

func TestProtoHashesMatchContract(t *testing.T) {
	contract := readContract[struct {
		Files []struct {
			Path   string `json:"path"`
			SHA256 string `json:"sha256"`
		} `json:"files"`
	}](t, "contracts/proto.json")
	if len(contract.Files) != 2 {
		t.Fatalf("unexpected proto inventory: %d", len(contract.Files))
	}
	for _, file := range contract.Files {
		data, err := os.ReadFile(file.Path)
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(data)
		if got := hex.EncodeToString(digest[:]); got != file.SHA256 {
			t.Fatalf("proto hash drift for %s: got=%s want=%s", file.Path, got, file.SHA256)
		}
	}
}
