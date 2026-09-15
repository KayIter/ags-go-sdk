package dataplane

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSemanticCodeAdapter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/contexts":
			_, _ = io.WriteString(response, `{"id":"ctx","language":"python","cwd":"/work"}`)
		case "/execute":
			for _, line := range []string{
				`{"type":"keepalive"}`,
				`{"type":"stdout","text":"out"}`,
				`{"type":"stderr","text":"err"}`,
				`{"type":"result","text":"42","is_main_result":true}`,
				`{"type":"error","name":"ValueError","value":"bad","traceback":"trace"}`,
				`{"type":"number_of_executions","execution_count":2}`,
				`{"type":"end_of_execution"}`,
			} {
				_, _ = fmt.Fprintln(response, line)
			}
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()
	client := New(server.URL, "token", server.Client())
	ctx := context.Background()
	created, err := client.CreateCodeContext(ctx, "python", "/work", 1<<20)
	if err != nil || created.ID != "ctx" || created.CWD != "/work" {
		t.Fatalf("context=%+v err=%v", created, err)
	}
	stream, err := client.StartCode(ctx, CodeRequest{Source: "print(42)", ContextID: created.ID, Env: map[string]string{"A": "B"}}, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	want := []CodeEventKind{CodeKeepalive, CodeStdout, CodeStderr, CodeResultEvent, CodeErrorEvent, CodeExecutionCount, CodeEnd}
	for _, kind := range want {
		event, receiveErr := stream.Recv()
		if receiveErr != nil || event.Kind != kind || event.WireBytes == 0 {
			t.Fatalf("event=%+v err=%v want=%v", event, receiveErr, kind)
		}
	}
	if _, err = stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("end err=%v", err)
	}
	if err = stream.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCodeAdapterRejectsUnknownEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(response, `{"type":"future"}`+"\n")
	}))
	defer server.Close()
	stream, err := New(server.URL, "token", server.Client()).StartCode(context.Background(), CodeRequest{Source: "1"}, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	_, err = stream.Recv()
	var wire *WireError
	if !errors.As(err, &wire) || wire.Reason != "CODE_EVENT_UNKNOWN" || wire.Detail == "" {
		t.Fatalf("error=%#v", err)
	}
}
