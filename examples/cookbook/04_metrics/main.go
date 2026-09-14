package main

import (
	"context"
	"fmt"
	"time"

	ags "github.com/TencentCloudAgentRuntime/ags-go-sdk"
)

func run(client *ags.Client, sandboxID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	end := time.Now().UTC()
	start := end.Add(-time.Hour)
	result, err := client.Sandboxes().GetMetrics(ctx, sandboxID, ags.MetricsQuery{
		Start: &start,
		End:   &end,
		Names: []ags.SandboxMetricName{ags.SandboxCPUUsagePercent, ags.SandboxMemoryUsedBytes},
	})
	if err != nil {
		return err
	}
	for _, series := range result.Series {
		fmt.Printf("%s %s points=%d\n", series.Name, series.Unit, len(series.Points))
	}
	return nil
}

func main() {}
