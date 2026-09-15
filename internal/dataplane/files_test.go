package dataplane

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"
	fs "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/gen/filesystem"
	fsconnect "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/gen/filesystem/filesystemconnect"
)

type semanticFilesystem struct {
	fsconnect.UnimplementedFilesystemHandler
}

func (semanticFilesystem) Stat(_ context.Context, request *connect.Request[fs.StatRequest]) (*connect.Response[fs.StatResponse], error) {
	target := "target"
	return connect.NewResponse(&fs.StatResponse{Entry: &fs.EntryInfo{Name: "item", Path: request.Msg.Path, Type: fs.FileType_FILE_TYPE_FILE, Size: 7, Mode: 0o644, Permissions: "-rw-r--r--", Owner: "user", Group: "user", SymlinkTarget: &target}}), nil
}

func (semanticFilesystem) MakeDir(_ context.Context, request *connect.Request[fs.MakeDirRequest]) (*connect.Response[fs.MakeDirResponse], error) {
	return connect.NewResponse(&fs.MakeDirResponse{Entry: &fs.EntryInfo{Name: "dir", Path: request.Msg.Path, Type: fs.FileType_FILE_TYPE_DIRECTORY}}), nil
}

func (semanticFilesystem) Move(_ context.Context, request *connect.Request[fs.MoveRequest]) (*connect.Response[fs.MoveResponse], error) {
	return connect.NewResponse(&fs.MoveResponse{Entry: &fs.EntryInfo{Name: "moved", Path: request.Msg.Destination, Type: fs.FileType_FILE_TYPE_FILE}}), nil
}

func (semanticFilesystem) ListDir(_ context.Context, request *connect.Request[fs.ListDirRequest]) (*connect.Response[fs.ListDirResponse], error) {
	return connect.NewResponse(&fs.ListDirResponse{Entries: []*fs.EntryInfo{{Name: "item", Path: request.Msg.Path + "/item", Type: fs.FileType_FILE_TYPE_FILE}}}), nil
}

func (semanticFilesystem) Remove(context.Context, *connect.Request[fs.RemoveRequest]) (*connect.Response[fs.RemoveResponse], error) {
	return connect.NewResponse(&fs.RemoveResponse{}), nil
}

func (semanticFilesystem) WatchDir(_ context.Context, _ *connect.Request[fs.WatchDirRequest], stream *connect.ServerStream[fs.WatchDirResponse]) error {
	if err := stream.Send(&fs.WatchDirResponse{Event: &fs.WatchDirResponse_Start{Start: &fs.WatchDirResponse_StartEvent{}}}); err != nil {
		return err
	}
	return stream.Send(&fs.WatchDirResponse{Event: &fs.WatchDirResponse_Filesystem{Filesystem: &fs.FilesystemEvent{Name: "item", Type: fs.EventType_EVENT_TYPE_CREATE}}})
}

func TestSemanticFileAndWatchAdapters(t *testing.T) {
	mux := http.NewServeMux()
	path, handler := fsconnect.NewFilesystemHandler(semanticFilesystem{})
	mux.Handle(path, handler)
	mux.HandleFunc("/files", func(response http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodGet {
			_, _ = io.WriteString(response, "content")
			return
		}
		reader, err := request.MultipartReader()
		if err != nil {
			t.Error(err)
			return
		}
		part, err := reader.NextPart()
		if err != nil {
			t.Error(err)
			return
		}
		if body, readErr := io.ReadAll(part); readErr != nil || string(body) != "upload" {
			t.Errorf("upload=%q err=%v", body, readErr)
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(response, `[{"name":"item","type":"file","path":"/tmp/item"}]`)
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	client := New(server.URL, "token", server.Client())

	body, err := client.ReadFile(context.Background(), "/tmp/item", "USER")
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(body)
	_ = body.Close()
	if err != nil || string(data) != "content" {
		t.Fatalf("read=%q err=%v", data, err)
	}
	written, err := client.WriteFile(context.Background(), "/tmp/item", strings.NewReader("upload"), "USER")
	if err != nil || written.Path != "/tmp/item" || written.Type != FileTypeFile {
		t.Fatalf("write=%+v err=%v", written, err)
	}
	listed, err := client.ListFiles(context.Background(), "/tmp", 1, "ROOT")
	if err != nil || len(listed) != 1 || listed[0].Type != FileTypeFile {
		t.Fatalf("list=%+v err=%v", listed, err)
	}
	stat, err := client.StatFile(context.Background(), "/tmp/item", "USER")
	if err != nil || stat.Owner != "user" || stat.SymlinkTarget == nil {
		t.Fatalf("stat=%+v err=%v", stat, err)
	}
	directory, err := client.MakeDir(context.Background(), "/tmp/dir", "USER")
	if err != nil || directory.Type != FileTypeDirectory {
		t.Fatalf("mkdir=%+v err=%v", directory, err)
	}
	moved, err := client.MoveFile(context.Background(), "/tmp/item", "/tmp/moved", "USER")
	if err != nil || moved.Path != "/tmp/moved" {
		t.Fatalf("move=%+v err=%v", moved, err)
	}
	if err := client.RemoveFile(context.Background(), "/tmp/moved", "USER"); err != nil {
		t.Fatal(err)
	}
	watch, err := client.StartWatch(context.Background(), "/tmp", true, true, "USER")
	if err != nil {
		t.Fatal(err)
	}
	event, err := watch.Recv(context.Background())
	if err != nil || event.Type != WatchCreate || event.Entry == nil || event.Entry.Path != "/tmp/item" {
		t.Fatalf("watch=%+v err=%v", event, err)
	}
	if err := watch.Invalidate(); err != nil {
		t.Fatal(err)
	}
}
