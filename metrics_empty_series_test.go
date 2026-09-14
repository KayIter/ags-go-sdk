package ags

import (
	"context"
	"testing"
)

type emptyMetricTransport struct{}

func (emptyMetricTransport) Query(context.Context, monitorRequest) (monitorResponse, error) {
	return monitorResponse{}, nil
}

func TestMetricsSuccessfulEmptySeriesDoesNotFabricateZero(t *testing.T) {
	client, err := NewClient(
		WithRegion("test"),
		WithCredential(CloudCredential{"id", "key"}),
		withControlPlane(metricControl{SandboxInfo{ID: "sb", ToolID: "tool", State: Running}}),
		withMonitorTransport(emptyMetricTransport{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := newSandbox(client, "sb", nil).Metrics().Get(
		context.Background(), MetricsQuery{Names: []SandboxMetricName{SandboxFSUsedBytes}},
	)
	if err != nil || len(result.Series) != 1 || len(result.Series[0].Points) != 0 || len(result.Unavailable) != 0 {
		t.Fatalf("empty success was changed: result=%#v err=%v", result, err)
	}
}
