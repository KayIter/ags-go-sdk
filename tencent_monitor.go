package ags

import (
	"context"
	"net/http"
	"time"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/cloudapi"
)

// tencentMonitor is a thin, signed adapter for the public Cloud Monitor API.
type tencentMonitor struct {
	api *cloudapi.Monitor
}

func newTencentMonitor(region string, credential CredentialProvider, client *http.Client) *tencentMonitor {
	return newTencentMonitorWithConfig(region, credential, "monitor.tencentcloudapi.com", client, 30*time.Second)
}
func newTencentMonitorWithConfig(region string, credential CredentialProvider, endpoint string, client *http.Client, timeout time.Duration) *tencentMonitor {
	if client == nil {
		client = &http.Client{Timeout: timeout}
	}
	source := func(ctx context.Context) (cloudapi.Credential, error) {
		value, err := retrieveCloudCredential(ctx, credential)
		return cloudapi.Credential{SecretID: value.SecretID, SecretKey: value.SecretKey, Token: value.Token}, err
	}
	return &tencentMonitor{api: cloudapi.NewMonitor(cloudapi.Config{Region: region, Endpoint: endpoint, Timeout: timeout, Transport: client.Transport}, source)}
}
func (m *tencentMonitor) Query(ctx context.Context, q monitorRequest) (monitorResponse, error) {
	response, err := m.api.Query(ctx, cloudapi.MonitorInput{Namespace: "QCE/AGS", MetricName: string(q.Metric), InstanceID: q.InstanceID, ToolID: q.ToolID, Period: q.Period, Start: q.Start, End: q.End})
	if err != nil {
		return monitorResponse{}, mapCloudError(err, "Metrics.Get")
	}
	points := make([]MetricPoint, 0, len(response.Points))
	for _, point := range response.Points {
		points = append(points, MetricPoint{Timestamp: point.Timestamp, Value: point.Value})
	}
	resolution := q.Period
	if response.Period > 0 {
		resolution = response.Period
	}
	truncated := len(points) >= 1440 && q.End.Sub(q.Start) > resolution*time.Duration(len(points))
	return monitorResponse{Points: points, Resolution: resolution, Truncated: truncated, RequestID: response.RequestID}, nil
}
