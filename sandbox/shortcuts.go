package sandbox

import (
	"context"

	ags "github.com/TencentCloudAgentRuntime/ags-go-sdk"
)

// Public aliases keep the shortcut API compact while preserving one canonical resource and
// model implementation in the root ags package.
type (
	Sandbox            = ags.Sandbox
	CreateOptions      = ags.CreateOptions
	ConnectOptions     = ags.ConnectOptions
	SandboxListOptions = ags.SandboxListOptions
	SandboxInfo        = ags.SandboxInfo
	SandboxPage        = ags.SandboxPage
	SandboxState       = ags.SandboxState
	ToolRef            = ags.ToolRef
	MountOption        = ags.MountOption
	SandboxAuthMode    = ags.SandboxAuthMode
)

// Lifecycle and authentication aliases preserve the root package's canonical values.
const (
	Creating = ags.Creating
	Running  = ags.Running
	Pausing  = ags.Pausing
	Paused   = ags.Paused
	Resuming = ags.Resuming
	Stopping = ags.Stopping
	Stopped  = ags.Stopped
	Failed   = ags.Failed
	Unknown  = ags.Unknown

	AuthDefault = ags.SandboxAuthDefault
	AuthToken   = ags.SandboxAuthToken
	AuthNone    = ags.SandboxAuthNone
	AuthPublic  = ags.SandboxAuthPublic
)

var resolveDefaultClient = ags.DefaultClient

// Create creates a Sandbox with the cached environment-backed default Client.
func Create(ctx context.Context, opts CreateOptions) (*Sandbox, error) {
	client, err := resolveDefaultClient()
	if err != nil {
		return nil, err
	}
	return client.Sandboxes().Create(ctx, opts)
}

// Connect resumes or conditionally extends an existing Sandbox with the default Client.
func Connect(ctx context.Context, id string, opts ...ConnectOptions) (*Sandbox, error) {
	client, err := resolveDefaultClient()
	if err != nil {
		return nil, err
	}
	return client.Sandboxes().Connect(ctx, id, opts...)
}

// Get reads Sandbox information without resuming, extending, or opening the data plane.
func Get(ctx context.Context, id string) (SandboxInfo, error) {
	client, err := resolveDefaultClient()
	if err != nil {
		return SandboxInfo{}, err
	}
	return client.Sandboxes().Get(ctx, id)
}

// List returns one server-filtered Cloud page through the default Client.
func List(ctx context.Context, opts SandboxListOptions) (SandboxPage, error) {
	client, err := resolveDefaultClient()
	if err != nil {
		return SandboxPage{}, err
	}
	return client.Sandboxes().List(ctx, opts)
}
