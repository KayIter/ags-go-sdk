package ags

import (
	"context"
	"sync"
	"testing"
	"time"
)

type metricControl struct{ info SandboxInfo }

func (c metricControl) Connect(context.Context, string, time.Duration) (SandboxInfo, error) {
	return c.info, nil
}

func (c metricControl) Create(context.Context, CreateOptions) (SandboxInfo, error) {
	return c.info, nil
}
func (c metricControl) Get(context.Context, string) (SandboxInfo, error) { return c.info, nil }
func (c metricControl) List(context.Context, SandboxListOptions) (SandboxPage, error) {
	return SandboxPage{}, nil
}
func (c metricControl) Pause(context.Context, string, PauseOptions) (SandboxInfo, error) {
	return c.info, nil
}
func (c metricControl) Resume(context.Context, string, ResumeOptions) (SandboxInfo, error) {
	return c.info, nil
}
func (c metricControl) Delete(context.Context, string) error { return nil }
func (c metricControl) dataPlane(context.Context, string) (dataPlane, error) {
	return nil, codeError(Unavailable, "test", "unused")
}

type metricTransport struct {
	mu                sync.Mutex
	got               []monitorRequest
	fail              map[SandboxMetricName]error
	active, maxActive int
	delay             time.Duration
}

func (m *metricTransport) Query(_ context.Context, q monitorRequest) (monitorResponse, error) {
	m.mu.Lock()
	m.got = append(m.got, q)
	m.active++
	if m.active > m.maxActive {
		m.maxActive = m.active
	}
	err := m.fail[q.Metric]
	m.mu.Unlock()
	if m.delay > 0 {
		time.Sleep(m.delay)
	}
	m.mu.Lock()
	m.active--
	m.mu.Unlock()
	if err != nil {
		return monitorResponse{}, err
	}
	return monitorResponse{Resolution: q.Period, Points: []MetricPoint{{Timestamp: q.End, Value: 2}, {Timestamp: q.Start, Value: 1}}}, nil
}
func testMetrics(credential CloudCredential, monitor *metricTransport) *Metrics {
	c, err := NewClient(WithRegion("test"), WithCredential(credential), withControlPlane(metricControl{SandboxInfo{ID: "sb", ToolID: "tool", State: Running}}), withMonitorTransport(monitor))
	if err != nil {
		panic(err)
	}
	s := newSandbox(c, "sb", nil)
	return s.Metrics()
}
func TestMetricsDefaultQueriesAllNamesAndSortsPoints(t *testing.T) {
	m := &metricTransport{}
	result, err := testMetrics(CloudCredential{"id", "key"}, m).Get(context.Background(), MetricsQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(m.got) != 10 || len(result.Series) != 10 {
		t.Fatalf("calls=%d series=%d, want 10", len(m.got), len(result.Series))
	}
	for _, series := range result.Series {
		if len(series.Points) != 2 || series.Points[0].Timestamp.After(series.Points[1].Timestamp) {
			t.Fatalf("points were not sorted: %#v", series.Points)
		}
		if series.Unit == "" {
			t.Fatalf("missing unit for %s", series.Name)
		}
	}
	for _, q := range m.got {
		if q.InstanceID != "sb" || q.ToolID != "tool" || q.Period != 5*time.Minute {
			t.Fatalf("bad public monitor query: %#v", q)
		}
	}
}
func TestMetricsRejectsMissingCloudCredentialsBeforeTransport(t *testing.T) {
	m := &metricTransport{}
	_, err := testMetrics(CloudCredential{}, m).Get(context.Background(), MetricsQuery{})
	sdk, ok := err.(*Error)
	if !ok || sdk.Code != PermissionDenied || sdk.Reason != "METRICS_CLOUD_CREDENTIALS_REQUIRED" {
		t.Fatalf("error=%#v", err)
	}
	if len(m.got) != 0 {
		t.Fatalf("transport called with missing credentials")
	}
}
func TestMetricsReturnsPartialAvailability(t *testing.T) {
	m := &metricTransport{fail: map[SandboxMetricName]error{SandboxCPUUsedCores: codeError(PermissionDenied, "monitor", "denied")}}
	result, err := testMetrics(CloudCredential{"id", "key"}, m).Get(context.Background(), MetricsQuery{Names: []SandboxMetricName{SandboxCPUUsagePercent, SandboxCPUUsedCores}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Series) != 1 || len(result.Unavailable) != 1 || result.Unavailable[0].Code != PermissionDenied {
		t.Fatalf("result=%#v", result)
	}
}
func TestMetricsRejectsInvalidRange(t *testing.T) {
	m := &metricTransport{}
	a := time.Now()
	b := a.Add(-time.Second)
	_, err := testMetrics(CloudCredential{"id", "key"}, m).Get(context.Background(), MetricsQuery{Start: &a, End: &b})
	if e, ok := err.(*Error); !ok || e.Code != InvalidArgument {
		t.Fatalf("error=%v", err)
	}
}

func TestMetricsConcurrencyIsBoundedAndEmptyNamesRejected(t *testing.T) {
	m := &metricTransport{delay: 10 * time.Millisecond}
	if _, err := testMetrics(CloudCredential{"id", "key"}, m).Get(context.Background(), MetricsQuery{}); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	max := m.maxActive
	m.mu.Unlock()
	if max < 2 || max > 4 {
		t.Fatalf("max concurrency=%d, want 2..4", max)
	}
	empty := []SandboxMetricName{}
	_, err := testMetrics(CloudCredential{"id", "key"}, &metricTransport{}).Get(context.Background(), MetricsQuery{Names: empty})
	if typed, ok := err.(*Error); !ok || typed.Code != InvalidArgument || typed.Reason != "METRIC_NAMES_EMPTY" {
		t.Fatalf("error=%v", err)
	}
}
