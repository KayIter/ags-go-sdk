package dataplane

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	process "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/gen/process"
)

func TestRequestKeepsInstanceTokenPrivateAndMapsUser(t *testing.T) {
	client := New("https://runtime.invalid", "private-token", nil)
	request := Request(client, &process.ListRequest{}, "ROOT")
	if got := request.Header().Get("X-Access-Token"); got != "private-token" {
		t.Fatalf("token header = %q", got)
	}
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("root:"))
	if got := request.Header().Get("Authorization"); got != want {
		t.Fatalf("authorization = %q, want %q", got, want)
	}
}

func TestHTTPErrorDoesNotContainResponseBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(response, "sensitive response")
	}))
	defer server.Close()

	client := New(server.URL, "private-token", server.Client())
	_, err := client.ReadFile(context.Background(), "/tmp/file", "USER")
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), "sensitive") || strings.Contains(err.Error(), "private-token") {
		t.Fatalf("error leaked private material: %v", err)
	}
}

func TestNormalizeUser(t *testing.T) {
	for input, want := range map[string]string{"": "user", "USER": "user", "ROOT": "root"} {
		if got := NormalizeUser(input); got != want {
			t.Fatalf("NormalizeUser(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestCodeRequestRewritesManagementPortAndPreservesExplicitPort(t *testing.T) {
	seen := make(chan *http.Request, 1)
	roundTrip := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		seen <- request
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("ok")),
		}, nil
	})
	client := New("https://49983-runtime.example:8443", "private-token", &http.Client{Transport: roundTrip})
	body, err := client.CodeRequest(context.Background(), "/run", []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	_ = body.Close()
	request := <-seen
	if got, want := request.URL.Host, "49999-runtime.example:8443"; got != want {
		t.Fatalf("code host = %q, want %q", got, want)
	}
	if got := request.URL.Path; got != "/run" {
		t.Fatalf("code path = %q", got)
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}
