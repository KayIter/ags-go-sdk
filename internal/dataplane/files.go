package dataplane

import (
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"time"

	fs "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/gen/filesystem"
)

// FileType is the private normalized file kind.
type FileType uint8

const (
	FileTypeUnknown FileType = iota
	FileTypeFile
	FileTypeDirectory
)

// FileInfo is the wire-independent file metadata returned to the root adapter.
type FileInfo struct {
	Name, Path, Permissions, Owner, Group string
	Type                                  FileType
	Size                                  int64
	Mode                                  uint32
	ModifiedAt                            time.Time
	SymlinkTarget                         *string
}

// ReadFile performs the runtime streaming-read request.
func (c *Client) ReadFile(ctx context.Context, path, user string) (io.ReadCloser, error) {
	endpoint, err := url.Parse(c.baseURL + "/files")
	if err != nil {
		return nil, err
	}
	query := endpoint.Query()
	query.Set("path", path)
	query.Set("username", NormalizeUser(user))
	endpoint.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	c.setHeaders(req.Header, user)
	response, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		_ = response.Body.Close()
		return nil, httpError(response.StatusCode)
	}
	return response.Body, nil
}

// WriteFile streams a multipart upload and returns normalized metadata.
func (c *Client) WriteFile(ctx context.Context, path string, body io.Reader, user string) (FileInfo, error) {
	pipeReader, pipeWriter := io.Pipe()
	multipartWriter := multipart.NewWriter(pipeWriter)
	copyDone := make(chan error, 1)
	go func() {
		part, err := multipartWriter.CreateFormFile("file", path)
		if err == nil {
			_, err = io.Copy(part, body)
		}
		if closeErr := multipartWriter.Close(); err == nil {
			err = closeErr
		}
		_ = pipeWriter.CloseWithError(err)
		copyDone <- err
	}()
	endpoint, err := url.Parse(c.baseURL + "/files")
	if err != nil {
		_ = pipeReader.Close()
		return FileInfo{}, err
	}
	query := endpoint.Query()
	query.Set("path", path)
	query.Set("username", NormalizeUser(user))
	endpoint.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), pipeReader)
	if err != nil {
		_ = pipeReader.Close()
		return FileInfo{}, err
	}
	req.Header.Set("Content-Type", multipartWriter.FormDataContentType())
	c.setHeaders(req.Header, user)
	response, err := c.httpClient.Do(req)
	if err != nil {
		_ = pipeReader.Close()
		return FileInfo{}, err
	}
	defer response.Body.Close()
	if copyErr := <-copyDone; copyErr != nil {
		return FileInfo{}, copyErr
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return FileInfo{}, httpError(response.StatusCode)
	}
	var entries []struct {
		Name string  `json:"name"`
		Type *string `json:"type"`
		Path string  `json:"path"`
	}
	if err := json.NewDecoder(response.Body).Decode(&entries); err != nil {
		return FileInfo{}, err
	}
	if len(entries) == 0 {
		return FileInfo{}, protocolError("WRITE_INFO_MISSING")
	}
	info := FileInfo{Name: entries[0].Name, Path: entries[0].Path}
	if entries[0].Type != nil {
		if *entries[0].Type == "dir" {
			info.Type = FileTypeDirectory
		} else {
			info.Type = FileTypeFile
		}
	}
	return info, nil
}

func (c *Client) ListFiles(ctx context.Context, path string, depth int, user string) ([]FileInfo, error) {
	response, err := c.filesystem.ListDir(ctx, request(c, &fs.ListDirRequest{Path: path, Depth: uint32(depth)}, user))
	if err != nil {
		return nil, wrapWireError(err)
	}
	out := make([]FileInfo, 0, len(response.Msg.GetEntries()))
	for _, entry := range response.Msg.GetEntries() {
		out = append(out, fileInfo(entry))
	}
	return out, nil
}

func (c *Client) StatFile(ctx context.Context, path, user string) (FileInfo, error) {
	response, err := c.filesystem.Stat(ctx, request(c, &fs.StatRequest{Path: path}, user))
	if err != nil {
		return FileInfo{}, wrapWireError(err)
	}
	return checkedFileInfo(response.Msg.GetEntry())
}

func (c *Client) MakeDir(ctx context.Context, path, user string) (FileInfo, error) {
	response, err := c.filesystem.MakeDir(ctx, request(c, &fs.MakeDirRequest{Path: path}, user))
	if err != nil {
		return FileInfo{}, wrapWireError(err)
	}
	return checkedFileInfo(response.Msg.GetEntry())
}

func (c *Client) MoveFile(ctx context.Context, source, destination, user string) (FileInfo, error) {
	response, err := c.filesystem.Move(ctx, request(c, &fs.MoveRequest{Source: source, Destination: destination}, user))
	if err != nil {
		return FileInfo{}, wrapWireError(err)
	}
	return checkedFileInfo(response.Msg.GetEntry())
}

func (c *Client) RemoveFile(ctx context.Context, path, user string) error {
	_, err := c.filesystem.Remove(ctx, request(c, &fs.RemoveRequest{Path: path}, user))
	return wrapWireError(err)
}

func checkedFileInfo(entry *fs.EntryInfo) (FileInfo, error) {
	if entry == nil {
		return FileInfo{}, protocolError("ENTRY_INFO_MISSING")
	}
	return fileInfo(entry), nil
}

func fileInfo(entry *fs.EntryInfo) FileInfo {
	if entry == nil {
		return FileInfo{}
	}
	kind := FileTypeUnknown
	switch entry.GetType() {
	case fs.FileType_FILE_TYPE_FILE:
		kind = FileTypeFile
	case fs.FileType_FILE_TYPE_DIRECTORY:
		kind = FileTypeDirectory
	}
	out := FileInfo{Name: entry.GetName(), Path: entry.GetPath(), Type: kind, Size: entry.GetSize(), Mode: entry.GetMode(), Permissions: entry.GetPermissions(), Owner: entry.GetOwner(), Group: entry.GetGroup()}
	if entry.ModifiedTime != nil {
		out.ModifiedAt = entry.ModifiedTime.AsTime()
	}
	if entry.SymlinkTarget != nil {
		value := entry.GetSymlinkTarget()
		out.SymlinkTarget = &value
	}
	return out
}
