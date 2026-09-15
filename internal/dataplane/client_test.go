package dataplane

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
)

type accessTokenSourceFunc func(context.Context, string) (string, error)

func (f accessTokenSourceFunc) AcquireToken(ctx context.Context, instanceID string) (string, error) {
	return f(ctx, instanceID)
}

func TestCloudConnectorOwnsRuntimeConnectionMaterial(t *testing.T) {
	var acquiredID string
	baseClient := &http.Client{Timeout: 30 * time.Second, Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("not used")
	})}
	connector := NewCloudConnector("ap-guangzhou", accessTokenSourceFunc(func(_ context.Context, instanceID string) (string, error) {
		acquiredID = instanceID
		return "private-token", nil
	}), baseClient)

	client, err := connector.Connect(context.Background(), "sandbox-id")
	if err != nil {
		t.Fatal(err)
	}
	if acquiredID != "sandbox-id" {
		t.Fatalf("acquired instance = %q", acquiredID)
	}
	if got, want := client.baseURL, "https://49983-sandbox-id.ap-guangzhou.tencentags.com"; got != want {
		t.Fatalf("base URL = %q, want %q", got, want)
	}
	if client.accessToken != "private-token" {
		t.Fatal("connector did not retain the acquired token privately")
	}
	if client.httpClient == baseClient || client.httpClient.Timeout != 0 || baseClient.Timeout != 30*time.Second {
		t.Fatalf("streaming client clone = %#v, base timeout = %v", client.httpClient, baseClient.Timeout)
	}
}

func TestCloudConnectorRejectsMissingTokenAndPreservesSourceError(t *testing.T) {
	connector := NewCloudConnector("ap-guangzhou", accessTokenSourceFunc(func(context.Context, string) (string, error) {
		return "", nil
	}), nil)
	_, err := connector.Connect(context.Background(), "sandbox-id")
	var wire *WireError
	if !errors.As(err, &wire) || wire.Kind != ErrorProtocol || wire.Reason != "TOKEN_MISSING" {
		t.Fatalf("missing token error = %#v", err)
	}

	sourceErr := errors.New("source failure")
	connector = NewCloudConnector("ap-guangzhou", accessTokenSourceFunc(func(context.Context, string) (string, error) {
		return "", sourceErr
	}), nil)
	if _, err = connector.Connect(context.Background(), "sandbox-id"); !errors.Is(err, sourceErr) {
		t.Fatalf("source error = %v", err)
	}
}

func TestRequestKeepsInstanceTokenPrivateAndMapsUser(t *testing.T) {
	seen := make(chan *http.Request, 1)
	roundTrip := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		seen <- request
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("ok"))}, nil
	})
	client := New("https://runtime.invalid", "private-token", &http.Client{Transport: roundTrip})
	body, err := client.ReadFile(context.Background(), "/tmp/file", "ROOT")
	if err != nil {
		t.Fatal(err)
	}
	_ = body.Close()
	request := <-seen
	if got := request.Header.Get("X-Access-Token"); got != "private-token" {
		t.Fatalf("token header = %q", got)
	}
	if got, want := request.Header.Get("Authorization"), "Basic cm9vdDo="; got != want {
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

func TestWireErrorDoesNotExposeConnectMessage(t *testing.T) {
	err := wrapWireError(connect.NewError(connect.CodeInternal, errors.New("sensitive wire message")))
	var wire *WireError
	if !errors.As(err, &wire) || wire.Code != "INTERNAL" {
		t.Fatalf("wire error = %#v", err)
	}
	if strings.Contains(err.Error(), "sensitive") {
		t.Fatalf("wire error leaked Connect message: %v", err)
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
	body, err := client.openCode(context.Background(), "/run", []byte(`{}`))
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
