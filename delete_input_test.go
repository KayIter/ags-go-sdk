package ags

import (
	"context"
	"errors"
	"testing"
)

func TestDeleteRejectsEmptyIdentityBeforeControlPlane(t *testing.T) {
	control := &pausedDeleteControl{}
	client, err := NewClient(WithRegion("test"), withControlPlane(control))
	if err != nil {
		t.Fatal(err)
	}
	err = client.Sandboxes().Delete(context.Background(), "")
	var sdk *Error
	if !errors.As(err, &sdk) || sdk.Code != InvalidArgument || sdk.Reason != "INSTANCE_ID_REQUIRED" {
		t.Fatalf("empty identity error = %#v", err)
	}
	if control.gets.Load() != 0 || control.resumes.Load() != 0 || control.deletes.Load() != 0 {
		t.Fatal("invalid delete reached the control plane")
	}
}
