package ags

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/controlplane"
	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/dataplane"
)

// tencentControlPlane is the Native control-plane adapter. Generated request
// models and TC3 signing stay behind internal/controlplane.
type tencentControlPlane struct {
	region     string
	credential CredentialProvider
	client     *http.Client
	endpoint   string
	timeout    time.Duration
	api        *controlplane.AGS
	runtime    *dataplane.CloudConnector
}

func newDefaultTencentControlPlane(region string, credential CredentialProvider) *tencentControlPlane {
	return newTencentControlPlane(region, credential, "ags.tencentcloudapi.com", nil, 30*time.Second)
}
func newTencentControlPlane(region string, credential CredentialProvider, endpoint string, client *http.Client, timeout time.Duration) *tencentControlPlane {
	if client == nil {
		client = &http.Client{Timeout: timeout}
	}
	c := &tencentControlPlane{region: region, credential: credential, client: client, endpoint: endpoint, timeout: timeout}
	c.api = controlplane.NewAGS(controlplane.Config{Region: region, Endpoint: endpoint, Timeout: timeout, Transport: client.Transport}, c.cloudCredential)
	c.runtime = dataplane.NewCloudConnector(region, c.api, client)
	return c
}
func (c *tencentControlPlane) Create(ctx context.Context, opts CreateOptions) (SandboxInfo, error) {
	in := controlplane.CreateInput{ToolID: opts.Tool.ID, ToolName: opts.Tool.Name, ClientToken: opts.ClientToken, Metadata: opts.Metadata, Env: opts.Env, AuthMode: string(opts.AuthMode)}
	for _, mount := range opts.MountOptions {
		in.MountOptions = append(in.MountOptions, controlplane.MountOption{Name: mount.Name, MountPath: mount.MountPath, SubPath: mount.SubPath, ReadOnly: mount.ReadOnly})
	}
	if opts.Timeout != nil {
		in.Timeout = opts.Timeout.String()
	}
	out, err := c.api.Create(ctx, in)
	if err != nil {
		return SandboxInfo{}, mapCloudError(err, "StartSandboxInstance")
	}
	return instanceFrom(out), nil
}
func (c *tencentControlPlane) Get(ctx context.Context, id string) (SandboxInfo, error) {
	out, err := c.api.List(ctx, controlplane.ListInput{InstanceIDs: []string{id}, Limit: 1})
	if err != nil {
		return SandboxInfo{}, mapCloudError(err, "DescribeSandboxInstanceList")
	}
	if len(out.Items) != 1 || out.Items[0].ID != id {
		return SandboxInfo{}, codeError(NotFound, "Sandboxes.Get", "INSTANCE_NOT_FOUND")
	}
	return instanceFrom(out.Items[0]), nil
}
func (c *tencentControlPlane) Connect(ctx context.Context, id string, timeout time.Duration) (SandboxInfo, error) {
	info, err := c.Get(ctx, id)
	if err != nil {
		return SandboxInfo{}, err
	}
	if info.State == Creating || info.State == Resuming || info.State == Pausing {
		target := Running
		if info.State == Pausing {
			target = Paused
		}
		info, err = c.waitFor(ctx, id, target)
		if err != nil {
			return SandboxInfo{}, err
		}
	}
	if info.State == Paused {
		return c.Resume(ctx, id, ResumeOptions{Timeout: &timeout})
	}
	if info.State != Running {
		return SandboxInfo{}, codeError(Conflict, "Sandboxes.Connect", "STATE_NOT_CONNECTABLE")
	}
	if timeout < 5*time.Minute {
		return SandboxInfo{}, codeError(InvalidArgument, "Sandboxes.Connect", "TIMEOUT_OUT_OF_RANGE")
	}
	if info.ExpiresAt.IsZero() {
		return SandboxInfo{}, codeError(Protocol, "Sandboxes.Connect", "EXPIRY_REQUIRED")
	}
	// Cloud has no conditional-extend action. This read/compare/update is not
	// atomic with another client's lifetime mutation; callers must serialize it.
	if time.Until(info.ExpiresAt) < timeout {
		if err := c.Update(ctx, id, UpdateOptions{Timeout: &timeout}); err != nil {
			return SandboxInfo{}, err
		}
		return c.Get(ctx, id)
	}
	return info, nil
}
func (c *tencentControlPlane) List(ctx context.Context, opts SandboxListOptions) (SandboxPage, error) {
	in, err := cloudListFilters(opts)
	if err != nil {
		return SandboxPage{}, err
	}
	out, err := c.api.List(ctx, in)
	if err != nil {
		return SandboxPage{}, mapCloudError(err, "DescribeSandboxInstanceList")
	}
	items := make([]SandboxInfo, 0, len(out.Items))
	for _, value := range out.Items {
		items = append(items, instanceFrom(value))
	}
	total := out.TotalCount
	next := opts.Offset + len(items)
	var nextPtr *int
	if next < total {
		nextPtr = &next
	}
	return SandboxPage{Items: items, TotalCount: total, NextOffset: nextPtr}, nil
}
func (c *tencentControlPlane) Pause(ctx context.Context, id string, opts PauseOptions) (SandboxInfo, error) {
	memory := opts.Mode != PauseDisk
	if err := c.api.Pause(ctx, id, memory); err != nil {
		return SandboxInfo{}, mapCloudError(err, "PauseSandboxInstance")
	}
	return c.waitFor(ctx, id, Paused)
}
func (c *tencentControlPlane) Resume(ctx context.Context, id string, opts ResumeOptions) (SandboxInfo, error) {
	timeout := ""
	if opts.Timeout != nil {
		timeout = opts.Timeout.String()
	}
	if err := c.api.Resume(ctx, id, timeout); err != nil {
		return SandboxInfo{}, mapCloudError(err, "ResumeSandboxInstance")
	}
	return c.waitFor(ctx, id, Running)
}
func (c *tencentControlPlane) Delete(ctx context.Context, id string) error {
	return mapCloudError(c.api.Stop(ctx, id), "StopSandboxInstance")
}
func (c *tencentControlPlane) dataPlane(ctx context.Context, id string) (dataPlane, error) {
	wire, err := c.runtime.Connect(ctx, id)
	if err != nil {
		var protocol *dataplane.WireError
		if errors.As(err, &protocol) {
			return nil, normalizeError("AcquireSandboxInstanceToken", err)
		}
		return nil, mapCloudError(err, "AcquireSandboxInstanceToken")
	}
	return newRuntimeDataPlane(wire, c.timeout), nil
}
func (c *tencentControlPlane) waitFor(ctx context.Context, id string, target SandboxState) (SandboxInfo, error) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		info, err := c.Get(ctx, id)
		if err != nil {
			return SandboxInfo{}, err
		}
		if info.State == target {
			return info, nil
		}
		if info.State == Failed || info.State == Stopped {
			return SandboxInfo{}, codeError(Conflict, "Sandbox.WaitFor", "TERMINAL_STATE")
		}
		select {
		case <-ctx.Done():
			return SandboxInfo{}, normalizeError("Sandbox.WaitFor", ctx.Err())
		case <-ticker.C:
		}
	}
}
func instanceFrom(value controlplane.Instance) SandboxInfo {
	state := SandboxState(value.Status)
	switch state {
	case "STARTING":
		state = Creating
	case "RUNNING", "PAUSING", "PAUSED", "RESUMING", "STOPPING", "STOPPED", "FAILED":
	case "STOP_FAILED", "PAUSE_FAILED":
		state = Failed
	default:
		state = Unknown
	}
	return SandboxInfo{ID: value.ID, ToolID: value.ToolID, ToolName: value.ToolName, State: state, CreatedAt: parseTime(value.CreatedAt), ExpiresAt: parseTime(value.ExpiresAt), Metadata: value.Metadata}
}
func parseTime(value string) time.Time { parsed, _ := time.Parse(time.RFC3339, value); return parsed }

func (c *tencentControlPlane) cloudCredential(ctx context.Context) (controlplane.Credential, error) {
	value, err := retrieveCloudCredential(ctx, c.credential)
	return controlplane.Credential{SecretID: value.SecretID, SecretKey: value.SecretKey, Token: value.Token}, err
}

func mapCloudError(err error, operation string) error {
	if err == nil {
		return nil
	}
	var sdk *Error
	if errors.As(err, &sdk) {
		return sdk
	}
	if errors.Is(err, context.Canceled) {
		return &Error{Code: Canceled, Operation: operation, Cause: err}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &Error{Code: DeadlineExceeded, Operation: operation, Cause: err}
	}
	code, reason, retryable, requestID := Unavailable, "CLOUD_API_UNAVAILABLE", true, ""
	var cloud *controlplane.Error
	if errors.As(err, &cloud) {
		requestID = cloud.RequestID
		value := strings.ToLower(cloud.Code)
		switch {
		case strings.Contains(value, "authfailure") || strings.Contains(value, "invalidcredential"):
			code, reason, retryable = Unauthenticated, "CLOUD_AUTHENTICATION_FAILED", false
		case strings.Contains(value, "unauthorizedoperation") || strings.Contains(value, "permission"):
			code, reason, retryable = PermissionDenied, "CLOUD_PERMISSION_DENIED", false
		case strings.Contains(value, "notfound") || strings.Contains(value, "notexist"):
			code, reason, retryable = NotFound, "RESOURCE_NOT_FOUND", false
		case strings.Contains(value, "conflict") || strings.Contains(value, "incorrectstate"):
			code, reason, retryable = Conflict, "RESOURCE_CONFLICT", false
		case strings.Contains(value, "limitexceeded") || strings.Contains(value, "requestlimit"):
			code, reason = ResourceExhausted, "REQUEST_LIMIT_EXCEEDED"
		case strings.Contains(value, "invalidparameter") || strings.Contains(value, "missingparameter"):
			code, reason, retryable = InvalidArgument, "CLOUD_INVALID_ARGUMENT", false
		case strings.Contains(value, "malformedresponse"):
			code, reason, retryable = Protocol, "CLOUD_RESPONSE_MALFORMED", false
		}
	}
	return &Error{Code: code, Operation: operation, Reason: reason, RequestID: requestID, Retryable: retryable, Cause: err}
}

// runtimeDataPlane stays below the public facade and never exposes token, host, or proto values.
