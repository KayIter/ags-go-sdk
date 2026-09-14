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
)

type listContractCase struct {
	ID            string              `json:"id"`
	States        []SandboxState      `json:"states"`
	Metadata      map[string][]string `json:"metadata"`
	Filters       []any               `json:"filters"`
	Offset        int                 `json:"offset"`
	Limit         int                 `json:"limit"`
	Total         int                 `json:"total"`
	Items         int                 `json:"items"`
	Next          *int                `json:"next"`
	ReturnedState string              `json:"returnedState"`
	ServerError   string              `json:"serverError"`
	Code          ErrorCode           `json:"code"`
	Reason        string              `json:"reason"`
}

func TestCloudListContract(t *testing.T) {
	data, err := os.ReadFile("contracts/list-cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Cases []listContractCase `json:"cases"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Cases) != 19 {
		t.Fatalf("list contract case count = %d; want 19", len(fixture.Cases))
	}
	for _, test := range fixture.Cases {
		t.Run(test.ID, func(t *testing.T) {
			var calls atomic.Int32
			captured := make(chan map[string]any, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				var request map[string]any
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
				}
				if r.Header.Get("X-TC-Action") != "DescribeSandboxInstanceList" || r.Header.Get("Authorization") == "" {
					t.Error("List did not use the typed signed Cloud action")
				}
				captured <- request
				w.Header().Set("Content-Type", "application/json")
				response := map[string]any{"RequestId": "list-fixture-request"}
				if test.ServerError != "" {
					response["Error"] = map[string]any{"Code": test.ServerError, "Message": "synthetic restriction"}
				} else {
					items := make([]any, 0, test.Items)
					for i := 0; i < test.Items; i++ {
						state := test.ReturnedState
						if state == "" {
							state = "RUNNING"
						}
						items = append(items, map[string]any{"InstanceId": "fixture-instance", "Status": state, "ToolId": "fixture-tool"})
					}
					response["InstanceSet"], response["TotalCount"] = items, test.Total
				}
				if err := json.NewEncoder(w).Encode(map[string]any{"Response": response}); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()

			client, err := NewClient(WithRegion("test"), WithCredential(CloudCredential{"id", "key"}), WithControlPlaneEndpoint(server.URL), WithHTTPClient(server.Client()))
			if err != nil {
				t.Fatal(err)
			}
			opts := SandboxListOptions{ToolID: "fixture-tool", Offset: test.Offset, Limit: test.Limit, States: test.States, Metadata: test.Metadata}
			before, _ := json.Marshal(opts)
			page, err := client.Sandboxes().List(context.Background(), opts)
			after, _ := json.Marshal(opts)
			if string(before) != string(after) {
				t.Fatal("List mutated caller options")
			}
			if test.Code != "" {
				var failure *Error
				if !errors.As(err, &failure) || failure.Code != test.Code || failure.Reason != test.Reason {
					t.Fatalf("error = %v; want %s/%s", err, test.Code, test.Reason)
				}
				if test.ServerError != "" && (failure.RequestID != "list-fixture-request" || errors.Unwrap(failure) == nil) {
					t.Fatal("List lost remote error evidence")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if len(page.Items) != test.Items || page.TotalCount != test.Total || !reflect.DeepEqual(page.NextOffset, test.Next) {
					t.Fatalf("page = %+v", page)
				}
				if test.ReturnedState != "" && string(page.Items[0].State) != test.ReturnedState {
					t.Fatalf("returned state = %s; want %s", page.Items[0].State, test.ReturnedState)
				}
			}
			wantCalls := int32(1)
			if test.Code != "" && test.ServerError == "" {
				wantCalls = 0
			}
			if calls.Load() != wantCalls {
				t.Fatalf("HTTP calls = %d; want %d", calls.Load(), wantCalls)
			}
			if wantCalls == 1 {
				request := <-captured
				limit := test.Limit
				if limit == 0 {
					limit = 20
				}
				want := map[string]any{"ToolId": "fixture-tool", "Offset": float64(test.Offset), "Limit": float64(limit)}
				if len(test.Filters) > 0 {
					want["Filters"] = test.Filters
				}
				if !reflect.DeepEqual(request, want) {
					t.Fatalf("wire request = %#v; want %#v", request, want)
				}
			}
		})
	}
}
