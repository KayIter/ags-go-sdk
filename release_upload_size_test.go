package ags

import (
	"context"
	"crypto/sha256"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type releaseZeroSource struct{ remaining int64 }

func (s *releaseZeroSource) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if s.remaining == 0 {
		return 0, io.EOF
	}
	n := min(int64(len(p)), s.remaining)
	clear(p[:n])
	s.remaining -= n
	return int(n), nil
}

// A same-size local transfer separates basic multipart/streaming correctness
// from the unaccepted cloud route. It cannot establish cloud upload support.
func TestReleaseUpload32MiBLocal(t *testing.T) {
	const size = int64(32 << 20)
	expected := sha256.New()
	_, _ = io.Copy(expected, &releaseZeroSource{size})
	uploaded := make(chan bool, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/files" || r.URL.Query().Get("path") != "/tmp/large.bin" {
			http.Error(w, "unexpected route", 400)
			return
		}
		if r.Method == "GET" {
			_, _ = io.Copy(w, &releaseZeroSource{size})
			return
		}
		multipart, err := r.MultipartReader()
		if err != nil {
			http.Error(w, "multipart required", 400)
			return
		}
		part, err := multipart.NextPart()
		if err != nil {
			http.Error(w, "part missing", 400)
			return
		}
		digest := sha256.New()
		n, err := io.Copy(digest, io.LimitReader(part, size+1))
		_, end := multipart.NextPart()
		valid := err == nil && n == size && part.FormName() == "file" && end == io.EOF && string(digest.Sum(nil)) == string(expected.Sum(nil))
		uploaded <- valid
		if !valid {
			http.Error(w, "payload mismatch", 400)
			return
		}
		_, _ = io.WriteString(w, `[{"name":"large.bin","path":"/tmp/large.bin","type":"file"}]`)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	plane := newDataPlane(server.URL, "synthetic", server.Client())
	sandbox := newSandbox(nil, "synthetic", plane)
	defer sandbox.Close()
	source := &releaseZeroSource{size}
	if _, err := sandbox.Files().Write(ctx, "/tmp/large.bin", source, WriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if source.remaining != 0 || !<-uploaded {
		t.Fatal("upload count or digest mismatch")
	}
	reader, err := sandbox.Files().Read(ctx, "/tmp/large.bin", ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	digest := sha256.New()
	n, err := io.Copy(digest, io.LimitReader(reader, size+1))
	if err != nil || n != size || string(digest.Sum(nil)) != string(expected.Sum(nil)) {
		t.Fatalf("download count/digest mismatch: bytes=%d error=%v", n, err)
	}
}
