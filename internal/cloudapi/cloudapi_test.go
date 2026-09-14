package cloudapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAcquireTokenSelectsInstanceAccessToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get("X-Tc-Action") != "AcquireSandboxInstanceToken" {
			t.Fatalf("action = %q", request.Header.Get("X-Tc-Action"))
		}
		_ = json.NewEncoder(response).Encode(map[string]any{
			"Response": map[string]any{
				"Token":        "instance-access-token",
				"TrafficToken": "business-port-traffic-token",
				"RequestId":    "request-fixture",
			},
		})
	}))
	defer server.Close()

	api := NewAGS(Config{
		Region:    "ap-test",
		Endpoint:  server.URL,
		Timeout:   time.Second,
		Transport: server.Client().Transport,
	}, func(context.Context) (Credential, error) {
		return Credential{SecretID: "synthetic-id", SecretKey: "synthetic-key"}, nil
	})

	token, err := api.AcquireToken(context.Background(), "sandbox-fixture")
	if err != nil {
		t.Fatal(err)
	}
	if token != "instance-access-token" {
		t.Fatalf("AcquireToken selected the wrong response credential")
	}
}

func TestAcquireTokenDoesNotFallBackToBusinessTrafficToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(response).Encode(map[string]any{
			"Response": map[string]any{
				"TrafficToken": "business-port-traffic-token",
				"RequestId":    "request-fixture",
			},
		})
	}))
	defer server.Close()

	api := NewAGS(Config{
		Region:    "ap-test",
		Endpoint:  server.URL,
		Timeout:   time.Second,
		Transport: server.Client().Transport,
	}, func(context.Context) (Credential, error) {
		return Credential{SecretID: "synthetic-id", SecretKey: "synthetic-key"}, nil
	})

	token, err := api.AcquireToken(context.Background(), "sandbox-fixture")
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		t.Fatal("AcquireToken fell back to the business-port traffic credential")
	}
}
