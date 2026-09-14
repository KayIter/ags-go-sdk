package e2e_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	ags "github.com/TencentCloudAgentRuntime/ags-go-sdk"
)

type cloudEnvironment struct {
	region     string
	toolID     string
	codeToolID string
}

func requireCloudEnvironment(t *testing.T) cloudEnvironment {
	t.Helper()
	if os.Getenv("AGS_E2E") != "1" {
		t.Fatal("real Cloud E2E requires AGS_E2E=1; use make test-e2e")
	}
	required := []string{"TENCENTCLOUD_SECRET_ID", "TENCENTCLOUD_SECRET_KEY", "AGS_E2E_REGION", "AGS_E2E_TOOL_ID", "AGS_E2E_CODE_TOOL_ID"}
	for _, name := range required {
		if os.Getenv(name) == "" {
			t.Fatalf("real Cloud E2E requires %s", name)
		}
	}
	return cloudEnvironment{region: os.Getenv("AGS_E2E_REGION"), toolID: os.Getenv("AGS_E2E_TOOL_ID"), codeToolID: os.Getenv("AGS_E2E_CODE_TOOL_ID")}
}

func newCloudClient(t *testing.T, environment cloudEnvironment) *ags.Client {
	t.Helper()
	client, err := ags.NewClient(ags.WithRegion(environment.region))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func deleteAndConfirm(t *testing.T, manager *ags.SandboxManager, sandbox *ags.Sandbox) {
	t.Helper()
	_ = sandbox.Close()
	deleteIDAndConfirm(t, manager, sandbox.ID())
}

func cleanupAcceptedCreate(t *testing.T, manager *ags.SandboxManager, err error) {
	t.Helper()
	var failure *ags.Error
	if errors.As(err, &failure) && failure.InstanceID != "" {
		deleteIDAndConfirm(t, manager, failure.InstanceID)
	}
}

func deleteIDAndConfirm(t *testing.T, manager *ags.SandboxManager, id string) {
	t.Helper()
	cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := manager.Delete(cleanup, id); err != nil {
		t.Errorf("delete sandbox: %v", err)
		return
	}
	deadline := time.NewTicker(2 * time.Second)
	defer deadline.Stop()
	for {
		info, err := manager.Get(cleanup, id)
		if err != nil {
			var failure *ags.Error
			if errors.As(err, &failure) && failure.Code == ags.NotFound {
				return
			}
			t.Errorf("confirm sandbox cleanup: %v", err)
			return
		}
		if info.State == ags.Stopped {
			return
		}
		select {
		case <-cleanup.Done():
			t.Errorf("confirm sandbox cleanup: %v", cleanup.Err())
			return
		case <-deadline.C:
		}
	}
}
