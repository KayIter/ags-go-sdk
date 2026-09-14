package ags

import (
	"context"
	"testing"
	"time"
)

func TestMetricsForwardsExplicitWindowAndPeriod(t *testing.T) {
	zone := time.FixedZone("fixture", 8*60*60)
	start := time.Date(2026, 9, 1, 9, 10, 11, 0, zone)
	end := start.Add(7 * time.Minute)
	period := time.Minute
	monitor := &metricTransport{}
	_, err := testMetrics(CloudCredential{"id", "key"}, monitor).Get(context.Background(), MetricsQuery{
		Start:  &start,
		End:    &end,
		Names:  []SandboxMetricName{SandboxMemoryUsedBytes},
		Period: &period,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(monitor.got) != 1 {
		t.Fatalf("monitor calls = %d", len(monitor.got))
	}
	got := monitor.got[0]
	if !got.Start.Equal(start) || got.Start.Location() != time.UTC || !got.End.Equal(end) || got.End.Location() != time.UTC || got.Period != period {
		t.Fatalf("explicit window changed: %#v", got)
	}
}
