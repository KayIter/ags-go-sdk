package ags

import (
	"context"
	"errors"
	"testing"
)

func TestMetricsAllUnavailableReturnsStableAggregateError(t *testing.T) {
	monitor := &metricTransport{fail: map[SandboxMetricName]error{
		SandboxCPUUsagePercent: codeError(PermissionDenied, "Monitor.Get", "DENIED"),
		SandboxCPUUsedCores:    codeError(Unavailable, "Monitor.Get", "TEMPORARY"),
	}}
	result, err := testMetrics(CloudCredential{"id", "key"}, monitor).Get(
		context.Background(),
		MetricsQuery{Names: []SandboxMetricName{SandboxCPUUsagePercent, SandboxCPUUsedCores}},
	)
	var sdk *Error
	if !errors.As(err, &sdk) || sdk.Code != Unavailable || sdk.Reason != "ALL_METRICS_UNAVAILABLE" {
		t.Fatalf("aggregate error = %#v", err)
	}
	if len(result.Series) != 0 || len(result.Unavailable) != 0 {
		t.Fatalf("all-failure result must not imply partial success: %#v", result)
	}
}
