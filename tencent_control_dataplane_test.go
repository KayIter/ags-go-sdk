package ags

import (
	"context"
	"errors"
	"testing"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/dataplane"
)

type emptyAccessTokenSource struct{}

func (emptyAccessTokenSource) AcquireToken(context.Context, string) (string, error) {
	return "", nil
}

func TestTencentDataPlanePreservesMissingTokenError(t *testing.T) {
	control := &tencentControlPlane{
		runtime: dataplane.NewCloudConnector("ap-guangzhou", emptyAccessTokenSource{}, nil),
	}
	_, err := control.dataPlane(context.Background(), "sandbox-id")
	var failure *Error
	if !errors.As(err, &failure) || failure.Code != Protocol || failure.Operation != "AcquireSandboxInstanceToken" || failure.Reason != "TOKEN_MISSING" {
		t.Fatalf("missing token error = %#v", err)
	}
}
