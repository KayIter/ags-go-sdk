package ags

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func TestCloudUpdateContract(t *testing.T) {
	var fixture struct {
		Cases []struct {
			ID         string            `json:"id"`
			Timeout    *float64          `json:"timeoutSeconds"`
			Metadata   map[string]string `json:"metadata"`
			Expected   ErrorCode         `json:"expectedCode"`
			CloudCalls int               `json:"cloudCalls"`
		} `json:"cases"`
	}
	data, err := os.ReadFile("contracts/update-cases.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Cases) != 8 {
		t.Fatalf("Update case count = %d; want 8", len(fixture.Cases))
	}
	for _, test := range fixture.Cases {
		t.Run(test.ID, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Header.Get("Authorization") == "" {
					t.Error("Update request was not signed")
				}
				var request map[string]any
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
				}
				w.Header().Set("Content-Type", "application/json")
				switch r.Header.Get("X-TC-Action") {
				case "DescribeSandboxInstanceList":
					instance := map[string]any{
						"InstanceId": "fixture",
						"Metadata": []any{
							map[string]any{"Name": "owner", "Value": "old"},
							map[string]any{"Name": "keep", "Value": "yes"},
							map[string]any{"Name": "x-reserved", "Value": "preserve"},
						},
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"Response": map[string]any{"InstanceSet": []any{instance}}})
				case "UpdateSandboxInstance":
					if request["InstanceId"] != "fixture" {
						t.Errorf("InstanceId = %#v", request["InstanceId"])
					}
					if test.Timeout != nil {
						got, err := time.ParseDuration(request["Timeout"].(string))
						if err != nil || got != time.Duration(*test.Timeout*float64(time.Second)) {
							t.Errorf("Timeout = %#v", request["Timeout"])
						}
					} else if _, exists := request["Timeout"]; exists {
						t.Error("omitted Timeout was sent")
					}
					if len(test.Metadata) > 0 {
						got := map[string]string{}
						for _, entry := range request["Metadata"].([]any) {
							value := entry.(map[string]any)
							got[value["Name"].(string)] = value["Value"].(string)
						}
						want := map[string]string{"owner": "old", "keep": "yes", "x-reserved": "preserve"}
						for key, value := range test.Metadata {
							want[key] = value
						}
						if !reflect.DeepEqual(got, want) {
							t.Errorf("metadata = %v; want %v", got, want)
						}
					} else if _, exists := request["Metadata"]; exists {
						t.Error("omitted Metadata was sent")
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"Response": map[string]any{"RequestId": "update-fixture"}})
				default:
					t.Errorf("unexpected action %q", r.Header.Get("X-TC-Action"))
				}
			}))
			defer server.Close()
			client, err := NewClient(WithRegion("test"), WithCredential(CloudCredential{"id", "key"}), WithControlPlaneEndpoint(server.URL), WithHTTPClient(server.Client()))
			if err != nil {
				t.Fatal(err)
			}
			sandbox := newSandbox(client, "fixture", nil)
			opts := UpdateOptions{MetadataUpsert: test.Metadata}
			if test.Timeout != nil {
				value := time.Duration(*test.Timeout * float64(time.Second))
				opts.Timeout = &value
			}
			result, err := sandbox.Update(context.Background(), opts)
			if test.Expected != "" {
				var failure *Error
				if !errors.As(err, &failure) || failure.Code != test.Expected || failure.Mutation == nil || failure.Mutation.State != MutationNotSent {
					t.Fatalf("validation error = %v", err)
				}
			} else if err != nil || result.InstanceID != "fixture" {
				t.Fatalf("Update = %+v, %v", result, err)
			}
			if int(calls.Load()) != test.CloudCalls {
				t.Fatalf("HTTP calls = %d; want %d", calls.Load(), test.CloudCalls)
			}
		})
	}
}

func TestCloudUpdateCancellationEvidence(t *testing.T) {
	received := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- struct{}{}
		<-release
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"Response": map[string]any{"RequestId": "update-fixture"}})
	}))
	defer server.Close()
	defer close(release)
	client, err := NewClient(WithRegion("test"), WithCredential(CloudCredential{"id", "key"}), WithControlPlaneEndpoint(server.URL), WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatal(err)
	}
	sandbox := newSandbox(client, "fixture", nil)
	timeout := 5 * time.Minute
	first, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	finished := make(chan error, 1)
	go func() {
		_, err := sandbox.Update(first, UpdateOptions{Timeout: &timeout})
		finished <- err
	}()
	select {
	case <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("Update request was not received")
	}
	waiting, cancelWaiting := context.WithTimeout(context.Background(), 20*time.Millisecond)
	_, err = sandbox.Update(waiting, UpdateOptions{Timeout: &timeout})
	cancelWaiting()
	var failure *Error
	if !errors.As(err, &failure) || failure.Code != DeadlineExceeded || failure.Mutation == nil || failure.Mutation.State != MutationNotSent {
		t.Fatalf("lock-wait evidence = %v", err)
	}
	cancelFirst()
	select {
	case err = <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("submitted Update did not observe cancellation")
	}
	if !errors.As(err, &failure) || failure.Code != Canceled || failure.Mutation == nil || failure.Mutation.State != MutationUnknown || failure.Mutation.Phase != MutationSubmit {
		t.Fatalf("submission evidence = %v", err)
	}
}

func TestCloudUpdateRejectsLossyMetadata(t *testing.T) {
	var updates atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-TC-Action") == "UpdateSandboxInstance" {
			updates.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		instance := map[string]any{
			"InstanceId": "fixture",
			"Metadata": []any{
				map[string]any{"Name": "same", "Value": "a"},
				map[string]any{"Name": "same", "Value": "b"},
			},
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"Response": map[string]any{"InstanceSet": []any{instance}}})
	}))
	defer server.Close()
	client, err := NewClient(WithRegion("test"), WithCredential(CloudCredential{"id", "key"}), WithControlPlaneEndpoint(server.URL), WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatal(err)
	}
	_, err = newSandbox(client, "fixture", nil).Update(context.Background(), UpdateOptions{MetadataUpsert: map[string]string{"owner": "new"}})
	var failure *Error
	if !errors.As(err, &failure) || failure.Code != Protocol || failure.Mutation == nil || failure.Mutation.Phase != MutationReadMetadata || failure.Mutation.State != MutationNotSent {
		t.Fatalf("lossy metadata error = %v", err)
	}
	if updates.Load() != 0 {
		t.Fatal("lossy metadata was submitted")
	}
}
