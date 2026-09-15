package ags

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/controlplane"
)

type emptyAccessTokenSource struct{}

func (emptyAccessTokenSource) AcquireToken(context.Context, string) (string, error) {
	return "", nil
}

func TestTencentDataPlanePreservesMissingTokenError(t *testing.T) {
	control := &controlAdapter{service: controlplane.NewService(controlplane.ServiceConfig{Region: "ap-guangzhou", ControlEndpoint: "ags.tencentcloudapi.com", MonitorEndpoint: "monitor.tencentcloudapi.com", Timeout: time.Second, Credential: func(context.Context) (controlplane.Credential, error) { return controlplane.Credential{}, nil }, RuntimeSource: emptyAccessTokenSource{}})}
	_, err := control.dataPlane(context.Background(), "sandbox-id")
	var failure *Error
	if !errors.As(err, &failure) || failure.Code != Protocol || failure.Operation != "AcquireSandboxInstanceToken" || failure.Reason != "TOKEN_MISSING" {
		t.Fatalf("missing token error = %#v", err)
	}
}
