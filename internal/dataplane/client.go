// Package dataplane contains the private runtime wire adapter. It deliberately
// keeps instance access material and generated protocol clients out of the
// public ags package contract.
package dataplane

import (
	"encoding/base64"
	"net/http"

	"connectrpc.com/connect"
	filesystemconnect "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/gen/filesystem/filesystemconnect"
	processconnect "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/gen/process/processconnect"
)

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

func request[T any](client *Client, message *T, user string) *connect.Request[T] {
	request := connect.NewRequest(message)
	client.setHeaders(request.Header(), user)
	return request
}

func (c *Client) setHeaders(header http.Header, user string) {
	user = NormalizeUser(user)
	header.Set("X-Access-Token", c.accessToken)
	header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(user+":")))
}
