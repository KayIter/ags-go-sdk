package ags

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

// SandboxMetricName identifies one of the ten supported public Cloud Monitor metrics.
type SandboxMetricName string

const (
	// SandboxCPUUsagePercent measures CPU utilization in percent.
	SandboxCPUUsagePercent SandboxMetricName = "SandboxCpuUsagePercent"
	// SandboxCPUUsedCores measures consumed CPU cores.
	SandboxCPUUsedCores SandboxMetricName = "SandboxCpuUsedCores"
	// SandboxDiskReadBytesPerSecond measures disk-read throughput.
	SandboxDiskReadBytesPerSecond SandboxMetricName = "SandboxDiskReadBytesPerSecond"
	// SandboxDiskWriteBytesPerSecond measures disk-write throughput.
	SandboxDiskWriteBytesPerSecond SandboxMetricName = "SandboxDiskWriteBytesPerSecond"
	// SandboxFSUsagePercent measures filesystem utilization in percent.
	SandboxFSUsagePercent SandboxMetricName = "SandboxFsUsagePercent"
	// SandboxFSUsedBytes measures filesystem usage in bytes.
	SandboxFSUsedBytes SandboxMetricName = "SandboxFsUsedBytes"
	// SandboxMemoryUsagePercent measures memory utilization in percent.
	SandboxMemoryUsagePercent SandboxMetricName = "SandboxMemoryUsagePercent"
	// SandboxMemoryUsedBytes measures memory usage in bytes.
	SandboxMemoryUsedBytes SandboxMetricName = "SandboxMemoryUsedBytes"
	// SandboxNetworkRxBytesPerSecond measures received network throughput.
	SandboxNetworkRxBytesPerSecond SandboxMetricName = "SandboxNetworkRxBytesPerSecond"
	// SandboxNetworkTxBytesPerSecond measures transmitted network throughput.
	SandboxNetworkTxBytesPerSecond SandboxMetricName = "SandboxNetworkTxBytesPerSecond"
)

// MetricUnit is the native SDK's display unit; Cores is dimensionless on the wire.
type MetricUnit string

const (
	// Percent is a percentage unit.
	Percent MetricUnit = "PERCENT"
	// Cores is CPU-core usage; the service wire unit is dimensionless.
	Cores MetricUnit = "CORES"
	// Bytes is a byte-count unit.
	Bytes MetricUnit = "BYTES"
	// BytesPerSecond is a byte-throughput unit.
	BytesPerSecond MetricUnit = "BYTES_PER_SECOND"
)

var metricUnits = map[SandboxMetricName]MetricUnit{SandboxCPUUsagePercent: Percent, SandboxCPUUsedCores: Cores, SandboxDiskReadBytesPerSecond: BytesPerSecond, SandboxDiskWriteBytesPerSecond: BytesPerSecond, SandboxFSUsagePercent: Percent, SandboxFSUsedBytes: Bytes, SandboxMemoryUsagePercent: Percent, SandboxMemoryUsedBytes: Bytes, SandboxNetworkRxBytesPerSecond: BytesPerSecond, SandboxNetworkTxBytesPerSecond: BytesPerSecond}

// MetricsQuery selects Cloud Monitor series and their time window.
type MetricsQuery struct {
	// Start and End bound the query in UTC at whole-second wire precision. Nil End means now; nil
	// Start means 48 hours before End. Start must not be after End.
	Start, End *time.Time
	// Names selects unique supported metrics. Nil selects all ten; an explicitly empty slice is
	// invalid.
	Names []SandboxMetricName
	// Period requests whole-second resolution, at least one second. Nil selects five minutes;
	// actual series resolution may differ.
	Period *time.Duration
}

// MetricPoint is one real Monitor sample; absent samples are not synthesized as zero.
type MetricPoint struct {
	// Timestamp is the sample time.
	Timestamp time.Time
	// Value uses the containing series' Unit.
	Value float64
}

// MetricSeries preserves one successful series, including a successful response with no
// points.
type MetricSeries struct {
	// Name is the native metric identifier.
	Name SandboxMetricName
	// Unit is the SDK unit for this metric.
	Unit MetricUnit
	// Points is ordered by ascending timestamp; empty is not evidence of telemetry collection.
	Points []MetricPoint
	// Resolution is the returned sampling period, or the requested period when the service omits
	// it.
	Resolution time.Duration
}

// UnavailableMetric describes one failed series in a partial Metrics result.
type UnavailableMetric struct {
	// Name identifies the failed query.
	Name SandboxMetricName
	// Code is the stable SDK failure category.
	Code ErrorCode
	// RequestID is the service request identifier when available; empty means absent.
	RequestID string
}

// MetricsResult contains successful series and explicit per-series failures. All-series
// failure returns an error instead.
type MetricsResult struct {
	// Series contains successful series ordered by name, possibly with zero points.
	Series []MetricSeries
	// Unavailable lists failed series ordered by name; inspect it even when Get returns no error.
	Unavailable []UnavailableMetric
	// Resolution is the shared series resolution, or zero when successful series disagree.
	Resolution time.Duration
	// Truncated indicates at least one response reported truncation.
	Truncated bool
}
type monitorTransport interface {
	Query(context.Context, monitorRequest) (monitorResponse, error)
}
type monitorRequest struct {
	Metric             SandboxMetricName
	InstanceID, ToolID string
	Start, End         time.Time
	Period             time.Duration
}
type monitorResponse struct {
	Points     []MetricPoint
	Resolution time.Duration
	Truncated  bool
	RequestID  string
}

// Metrics queries public Cloud Monitor by sandbox identity without connecting envd.
type Metrics struct{ sandbox *Sandbox }

// Get queries this sandbox's Metrics using Cloud credentials, without opening envd. Context
// cancellation takes precedence over partial results. Successful empty series contain no
// fabricated points; inspect Unavailable for partial failures.
func (m *Metrics) Get(ctx context.Context, q MetricsQuery) (MetricsResult, error) {
	return m.sandbox.client.Sandboxes().GetMetrics(ctx, m.sandbox.ID(), q)
}

// GetMetrics queries Cloud Monitor without connecting the sandbox data plane.
func (m *SandboxManager) GetMetrics(ctx context.Context, id string, q MetricsQuery) (MetricsResult, error) {
	const op = "Metrics.Get"
	if id == "" {
		return MetricsResult{}, codeError(InvalidArgument, op, "INSTANCE_ID_REQUIRED")
	}
	credential, credentialErr := m.client.cfg.credential.Retrieve(ctx)
	if credentialErr != nil {
		return MetricsResult{}, normalizeError(op, credentialErr)
	}
	if !credential.Valid() {
		return MetricsResult{}, codeError(PermissionDenied, op, "METRICS_CLOUD_CREDENTIALS_REQUIRED")
	}
	end := time.Now().UTC()
	if q.End != nil {
		end = q.End.UTC()
	}
	start := end.Add(-48 * time.Hour)
	if q.Start != nil {
		start = q.Start.UTC()
	}
	if start.After(end) {
		return MetricsResult{}, codeError(InvalidArgument, op, "START_AFTER_END")
	}
	period := 5 * time.Minute
	if q.Period != nil {
		period = *q.Period
	}
	if period < time.Second || period%time.Second != 0 {
		return MetricsResult{}, codeError(InvalidArgument, op, "INVALID_PERIOD")
	}
	names := q.Names
	if names == nil {
		names = allMetricNames()
	} else if len(names) == 0 {
		return MetricsResult{}, codeError(InvalidArgument, op, "METRIC_NAMES_EMPTY")
	}
	seen := map[SandboxMetricName]bool{}
	for _, n := range names {
		if seen[n] {
			return MetricsResult{}, codeError(InvalidArgument, op, "DUPLICATE_METRIC")
		}
		seen[n] = true
		if _, ok := metricUnits[n]; !ok {
			return MetricsResult{}, codeError(InvalidArgument, op, "UNSUPPORTED_METRIC")
		}
	}
	info, err := m.client.cfg.control.Get(ctx, id)
	if err != nil {
		return MetricsResult{}, err
	}
	if info.ID != id {
		return MetricsResult{}, codeError(Protocol, op, "INSTANCE_ID_MISMATCH")
	}
	if info.ToolID == "" {
		return MetricsResult{}, codeError(Protocol, op, "TOOL_ID_MISSING")
	}
	out := MetricsResult{Resolution: period}
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4)
	for _, name := range names {
		name := name
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()
			r, e := m.client.cfg.monitor.Query(ctx, monitorRequest{Metric: name, InstanceID: info.ID, ToolID: info.ToolID, Start: start, End: end, Period: period})
			mu.Lock()
			defer mu.Unlock()
			if e != nil {
				code := Unavailable
				requestID := ""
				var typed *Error
				if errors.As(e, &typed) {
					code = typed.Code
					requestID = typed.RequestID
				}
				out.Unavailable = append(out.Unavailable, UnavailableMetric{Name: name, Code: code, RequestID: requestID})
				return
			}
			sort.Slice(r.Points, func(i, j int) bool { return r.Points[i].Timestamp.Before(r.Points[j].Timestamp) })
			resolution := r.Resolution
			if resolution <= 0 {
				resolution = period
			}
			out.Series = append(out.Series, MetricSeries{Name: name, Unit: metricUnits[name], Points: r.Points, Resolution: resolution})
			out.Truncated = out.Truncated || r.Truncated
		}()
	}
	wg.Wait()
	if ctx.Err() != nil {
		return MetricsResult{}, normalizeError(op, ctx.Err())
	}
	if len(out.Series) == 0 && len(out.Unavailable) > 0 {
		return MetricsResult{}, codeError(Unavailable, op, "ALL_METRICS_UNAVAILABLE")
	}
	sort.Slice(out.Series, func(i, j int) bool { return out.Series[i].Name < out.Series[j].Name })
	if len(out.Series) > 0 {
		out.Resolution = out.Series[0].Resolution
		for _, series := range out.Series {
			if series.Resolution != out.Resolution {
				out.Resolution = 0
				break
			}
		}
	}
	sort.Slice(out.Unavailable, func(i, j int) bool { return out.Unavailable[i].Name < out.Unavailable[j].Name })
	return out, nil
}
func allMetricNames() []SandboxMetricName {
	return []SandboxMetricName{SandboxCPUUsagePercent, SandboxCPUUsedCores, SandboxDiskReadBytesPerSecond, SandboxDiskWriteBytesPerSecond, SandboxFSUsagePercent, SandboxFSUsedBytes, SandboxMemoryUsagePercent, SandboxMemoryUsedBytes, SandboxNetworkRxBytesPerSecond, SandboxNetworkTxBytesPerSecond}
}
