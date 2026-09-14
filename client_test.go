package ags

import (
	"context"
	"errors"
	"testing"
)

type createCleanupControl struct {
	metricControl
	deleted bool
}

func (c *createCleanupControl) Delete(context.Context, string) error {
	c.deleted = true
	return nil
}

func (c *createCleanupControl) dataPlane(context.Context, string) (dataPlane, error) {
	return nil, codeError(Unavailable, "AcquireSandboxInstanceToken", "TEST_FAILURE")
}

func TestCreatePreservesRemoteInstanceWhenConnectionFails(t *testing.T) {
	control := &createCleanupControl{metricControl: metricControl{info: SandboxInfo{ID: "created", State: Running}}}
	client, err := NewClient(
		WithRegion("ap-test"),
		WithCredential(CloudCredential{SecretID: "id", SecretKey: "key"}),
		withControlPlane(control),
		withMonitorTransport(&metricTransport{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Sandboxes().Create(context.Background(), CreateOptions{Tool: ToolRef{Name: "tool"}})
	var failure *Error
	if !errors.As(err, &failure) || failure.InstanceID != "created" || control.deleted {
		t.Fatalf("error=%v deleted=%v", err, control.deleted)
	}
	if err = client.Sandboxes().Delete(context.Background(), failure.InstanceID); err != nil || !control.deleted {
		t.Fatalf("explicit cleanup must work without data-plane readiness: err=%v deleted=%v", err, control.deleted)
	}
}
