// Package dataplane contains the private runtime wire adapter. It deliberately
// keeps instance access material and generated protocol clients out of the
// public ags package contract.
package dataplane

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"strings"

	"connectrpc.com/connect"
	filesystemconnect "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/gen/filesystem/filesystemconnect"
	processconnect "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/gen/process/processconnect"
)

const codePort = "49999"

// ErrWriteInfoMissing means the upload endpoint returned no result entry.
var ErrWriteInfoMissing = errors.New("write info missing")

// Client owns one generation's private endpoint, instance token, HTTP client,
// and generated RPC clients. It never exposes the token to its caller.
type Client struct {
	baseURL     string
	accessToken string
	httpClient  *http.Client
	filesystem  filesystemconnect.FilesystemClient
	process     processconnect.ProcessClient
}

// New creates a private wire client. The caller owns lifecycle cancellation;
// Client owns only immutable connection material.
func New(baseURL, accessToken string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{
		baseURL:     baseURL,
		accessToken: accessToken,
		httpClient:  httpClient,
		filesystem:  filesystemconnect.NewFilesystemClient(httpClient, baseURL, connect.WithProtoJSON()),
		process:     processconnect.NewProcessClient(httpClient, baseURL, connect.WithProtoJSON()),
	}
}

// BaseURL returns the immutable runtime endpoint for internal adapter composition.
func (c *Client) BaseURL() string { return c.baseURL }

// HTTPClient returns the generation's HTTP client without exposing access material.
func (c *Client) HTTPClient() *http.Client { return c.httpClient }

// Filesystem returns the generated filesystem client inside the internal boundary.
func (c *Client) Filesystem() filesystemconnect.FilesystemClient { return c.filesystem }

// Process returns the generated process client inside the internal boundary.
func (c *Client) Process() processconnect.ProcessClient { return c.process }

// NormalizeUser maps the SDK's empty/default identity to the runtime username.
func NormalizeUser(user string) string {
	if user == "" || user == "USER" {
		return "user"
	}
	if user == "ROOT" {
		return "root"
	}
	return user
}

// Request attaches private generation authentication to a Connect request.
func Request[T any](client *Client, message *T, user string) *connect.Request[T] {
	request := connect.NewRequest(message)
	client.setHeaders(request.Header(), user)
	return request
}

func (c *Client) setHeaders(header http.Header, user string) {
	user = NormalizeUser(user)
	header.Set("X-Access-Token", c.accessToken)
	header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(user+":")))
}

// HTTPError reports only a response status. Bodies and connection material are
// intentionally discarded before the error crosses the internal boundary.
type HTTPError struct {
	StatusCode int
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("data plane returned HTTP status %d", e.StatusCode)
}

// WriteInfo is the minimal wire response returned by the runtime file upload endpoint.
type WriteInfo struct {
	Name string
	Type *string
	Path string
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
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	c.setHeaders(request.Header, user)
	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		_ = response.Body.Close()
		return nil, &HTTPError{StatusCode: response.StatusCode}
	}
	return response.Body, nil
}

// WriteFile streams a multipart upload and returns only the SDK-independent wire fields.
func (c *Client) WriteFile(ctx context.Context, path string, body io.Reader, user string) (WriteInfo, error) {
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
		return WriteInfo{}, err
	}
	query := endpoint.Query()
	query.Set("path", path)
	query.Set("username", NormalizeUser(user))
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), pipeReader)
	if err != nil {
		_ = pipeReader.Close()
		return WriteInfo{}, err
	}
	request.Header.Set("Content-Type", multipartWriter.FormDataContentType())
	c.setHeaders(request.Header, user)
	response, err := c.httpClient.Do(request)
	if err != nil {
		_ = pipeReader.Close()
		return WriteInfo{}, err
	}
	defer response.Body.Close()
	if copyErr := <-copyDone; copyErr != nil {
		return WriteInfo{}, copyErr
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return WriteInfo{}, &HTTPError{StatusCode: response.StatusCode}
	}
	var entries []struct {
		Name string  `json:"name"`
		Type *string `json:"type"`
		Path string  `json:"path"`
	}
	if err := json.NewDecoder(response.Body).Decode(&entries); err != nil {
		return WriteInfo{}, err
	}
	if len(entries) == 0 {
		return WriteInfo{}, ErrWriteInfoMissing
	}
	return WriteInfo{Name: entries[0].Name, Type: entries[0].Type, Path: entries[0].Path}, nil
}

// CodeRequest sends one private Code service request and returns its streaming body.
func (c *Client) CodeRequest(ctx context.Context, requestPath string, body []byte) (io.ReadCloser, error) {
	endpoint, err := url.Parse(c.baseURL)
	if err != nil {
		return nil, err
	}
	hostname := endpoint.Hostname()
	if strings.HasPrefix(hostname, "49983-") {
		port := endpoint.Port()
		hostname = codePort + strings.TrimPrefix(hostname, "49983")
		endpoint.Host = hostname
		if port != "" {
			endpoint.Host = net.JoinHostPort(hostname, port)
		}
	}
	endpoint.Path, endpoint.RawQuery = requestPath, ""
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Access-Token", c.accessToken)
	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		_ = response.Body.Close()
		return nil, &HTTPError{StatusCode: response.StatusCode}
	}
	return response.Body, nil
}
