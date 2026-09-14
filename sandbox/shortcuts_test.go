package sandbox

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	ags "github.com/TencentCloudAgentRuntime/ags-go-sdk"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func jsonResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func TestCreateDelegatesToDefaultClientAndReturnsCanonicalSandbox(t *testing.T) {
	var mu sync.Mutex
	var actions []string
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		mu.Lock()
		defer mu.Unlock()
		action := request.Header.Get("X-TC-Action")
		if action != "" {
			actions = append(actions, action)
		}
		switch action {
		case "StartSandboxInstance":
			return jsonResponse(`{"Response":{"Instance":{"InstanceId":"sb-shortcut","Status":"RUNNING"},"RequestId":"request-start"}}`), nil
		case "AcquireSandboxInstanceToken":
			return jsonResponse(`{"Response":{"Token":"synthetic-instance-token","TrafficToken":"synthetic-business-token","RequestId":"request-token"}}`), nil
		}
		if request.Header.Get("X-Access-Token") != "synthetic-instance-token" {
			t.Fatalf("data-plane token was not scoped to the generated request")
		}
		if strings.Contains(request.URL.String(), "synthetic-instance-token") {
			t.Fatalf("instance token leaked into URL")
		}
		return jsonResponse(`{"entries":[]}`), nil
	})
	client, err := ags.NewClient(
		ags.WithRegion("ap-test"),
		ags.WithCredential(ags.CloudCredential{SecretID: "synthetic-id", SecretKey: "synthetic-key"}),
		ags.WithControlPlaneEndpoint("https://cloud.example.test"),
		ags.WithHTTPClient(&http.Client{Transport: transport}),
	)
	if err != nil {
		t.Fatal(err)
	}
	previous := resolveDefaultClient
	resolveDefaultClient = func() (*ags.Client, error) { return client, nil }
	t.Cleanup(func() { resolveDefaultClient = previous })

	sb, err := Create(context.Background(), CreateOptions{Tool: ToolRef{Name: "tool"}})
	if err != nil {
		t.Fatal(err)
	}
	defer sb.Close()
	var canonical *ags.Sandbox = sb
	if canonical.ID() != "sb-shortcut" {
		t.Fatalf("unexpected identity %q", canonical.ID())
	}
	if len(actions) != 2 || actions[0] != "StartSandboxInstance" || actions[1] != "AcquireSandboxInstanceToken" {
		t.Fatalf("unexpected control actions: %v", actions)
	}
}
