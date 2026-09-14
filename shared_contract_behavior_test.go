package ags

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestMetricWireMappings(t *testing.T) {
	var fixture struct {
		Metrics []struct {
			Name string     `json:"name"`
			Unit MetricUnit `json:"unit"`
		} `json:"metrics"`
	}
	data, err := os.ReadFile("contracts/metrics.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Metrics) != 10 {
		t.Fatalf("metric count = %d; want 10", len(fixture.Metrics))
	}
	for _, metric := range fixture.Metrics {
		t.Run(metric.Name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request map[string]any
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
				}
				if r.Header.Get("X-TC-Action") != "GetMonitorData" || request["MetricName"] != metric.Name || request["Namespace"] != "QCE/AGS" {
					t.Errorf("Monitor request = %#v", request)
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"Response": map[string]any{"Period": 60, "DataPoints": []any{}}})
			}))
			defer server.Close()
			name := SandboxMetricName(metric.Name)
			if metricUnits[name] != metric.Unit {
				t.Fatalf("unit = %s; want %s", metricUnits[name], metric.Unit)
			}
			transport := newTencentMonitorWithConfig("test", CloudCredential{"id", "key"}, server.URL, server.Client(), time.Second)
			_, err := transport.Query(context.Background(), monitorRequest{Metric: name, InstanceID: "sb", ToolID: "tool", Start: time.Unix(0, 0), End: time.Unix(60, 0), Period: time.Minute})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStableCloudErrorMappings(t *testing.T) {
	var cases []struct {
		Status    int       `json:"status"`
		Cloud     string    `json:"cloud_code"`
		Code      ErrorCode `json:"code"`
		Retryable bool      `json:"retryable"`
	}
	data, err := os.ReadFile("contracts/error-cases.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) != 7 {
		t.Fatalf("Cloud error case count = %d; want 7", len(cases))
	}
	for _, test := range cases {
		t.Run(test.Cloud, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(test.Status)
				_ = json.NewEncoder(w).Encode(map[string]any{"Response": map[string]any{"RequestId": "request-id", "Error": map[string]any{"Code": test.Cloud, "Message": "private fixture body"}}})
			}))
			control := newTencentControlPlane("test", CloudCredential{"id", "key"}, server.URL, server.Client(), time.Second)
			_, err := control.List(context.Background(), SandboxListOptions{Limit: 1})
			server.Close()
			var failure *Error
			if !errors.As(err, &failure) || failure.Code != test.Code || failure.Retryable != test.Retryable || failure.RequestID != "request-id" {
				t.Fatalf("error = %v; want %s retryable=%v", err, test.Code, test.Retryable)
			}
		})
	}
}
