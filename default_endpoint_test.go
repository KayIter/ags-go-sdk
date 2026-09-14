package ags

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

type endpointTransport func(*http.Request) (*http.Response, error)

func (f endpointTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// Exercise the public default, not an overridden httptest URL. Never dial a host.
func TestDefaultControlEndpointAndSigningService(t *testing.T) {
	calls := 0
	client, err := NewClient(WithRegion("ap-test"), WithCredential(CloudCredential{"test-id", "test-key"}), WithHTTPClient(&http.Client{
		Transport: endpointTransport(func(r *http.Request) (*http.Response, error) {
			calls++
			if r.URL.Scheme != "https" || r.URL.Host != "ags.tencentcloudapi.com" {
				t.Errorf("unexpected default URL: %s", r.URL)
			}
			if r.Header.Get("X-TC-Action") != "DescribeSandboxInstanceList" {
				t.Error("unexpected control action")
			}
			if !strings.Contains(r.Header.Get("Authorization"), "/ags/tc3_request") {
				t.Error("expected AGS signing scope")
			}
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"Response":{"InstanceSet":[],"TotalCount":0,"RequestId":"fixture"}}`)), Request: r}, nil
		}),
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Sandboxes().List(context.Background(), SandboxListOptions{}); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("requests = %d, want 1", calls)
	}
	if cp := newDefaultTencentControlPlane("ap-test", CloudCredential{}); cp.endpoint != "ags.tencentcloudapi.com" {
		t.Fatalf("internal default endpoint = %s", cp.endpoint)
	}
}
