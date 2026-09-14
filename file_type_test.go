package ags

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func TestConformanceFileTypesPreserveUnknown(t *testing.T) {
	var fixture struct {
		Cases []struct {
			ID       string
			Entry    json.RawMessage
			Expected string
		}
	}
	data, err := os.ReadFile("contracts/filesystem-type-cases.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Cases) != 5 {
		t.Fatal("missing file-type cases")
	}
	for _, c := range fixture.Cases {
		t.Run(c.ID, func(t *testing.T) {
			var count atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count.Add(1)
				if r.Method != "POST" || r.URL.Path != "/filesystem.Filesystem/Stat" {
					t.Error("wrong method")
				}
				if r.Header.Get("X-Access-Token") != "synthetic" || r.Header.Get("Authorization") != "Basic dXNlcjo=" {
					t.Error("wrong identity")
				}
				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(map[string]json.RawMessage{"entry": c.Entry}); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			sb := newSandbox(nil, "synthetic", newDataPlane(server.URL, "synthetic", server.Client()))
			defer sb.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			entry, err := sb.Files().Stat(ctx, "/fixture/link", StatOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if string(entry.Type) != c.Expected {
				t.Fatalf("type=%q want=%q", entry.Type, c.Expected)
			}
			if count.Load() != 1 {
				t.Fatal("unexpected replay")
			}
		})
	}
}
