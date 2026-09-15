// Package dataplane contains the private runtime wire adapter. It deliberately
// keeps instance access material and generated protocol clients out of the
// public ags package contract.
package dataplane

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"

	"connectrpc.com/connect"
	filesystemconnect "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/gen/filesystem/filesystemconnect"
	processconnect "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/gen/process/processconnect"
)

const runtimeDomain = "tencentags.com"

// AccessTokenSource obtains connection material for one Sandbox generation.
// Implementations remain behind internal package boundaries.
type AccessTokenSource interface {
	AcquireToken(context.Context, string) (string, error)
}

// CloudConnector resolves Cloud-managed Sandbox connection material without
// exposing the endpoint or instance token to the root SDK package.
type CloudConnector struct {
	region     string
	source     AccessTokenSource
	httpClient *http.Client
}

// NewCloudConnector creates a private connector for one Client identity.
func NewCloudConnector(region string, source AccessTokenSource, httpClient *http.Client) *CloudConnector {
	return &CloudConnector{region: region, source: source, httpClient: httpClient}
}

// Connect resolves an instance token and creates an immutable wire client.
func (c *CloudConnector) Connect(ctx context.Context, instanceID string) (*Client, error) {
	if c == nil || c.source == nil {
		return nil, protocolError("TOKEN_SOURCE_MISSING")
	}
	accessToken, err := c.source.AcquireToken(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	if accessToken == "" {
		return nil, protocolError("TOKEN_MISSING")
	}
	httpClient := &http.Client{}
	if c.httpClient != nil {
		*httpClient = *c.httpClient
		httpClient.Timeout = 0
	}
	baseURL := fmt.Sprintf("https://49983-%s.%s.%s", instanceID, c.region, runtimeDomain)
	return New(baseURL, accessToken, httpClient), nil
}

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
