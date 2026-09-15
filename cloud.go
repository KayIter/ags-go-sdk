package ags

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/controlplane"
)

// controlAdapter only converts stable public models to private models. Cloud
// orchestration, signing, token acquisition, and runtime construction belong
// to internal/controlplane.Service.
type controlAdapter struct {
	service  *controlplane.Service
	endpoint string
}

func newDefaultTencentControlPlane(region string, credential CredentialProvider) *controlAdapter {
	return newTencentControlPlane(region, credential, "ags.tencentcloudapi.com", nil, 30*time.Second)
}

func newTencentControlPlane(region string, credential CredentialProvider, endpoint string, client *http.Client, timeout time.Duration) *controlAdapter {
	return newControlAdapter(region, credential, endpoint, "monitor.tencentcloudapi.com", client, timeout)
}

func newControlAdapter(region string, credential CredentialProvider, controlEndpoint, monitorEndpoint string, client *http.Client, timeout time.Duration) *controlAdapter {
	source := func(ctx context.Context) (controlplane.Credential, error) {
		value, err := retrieveCloudCredential(ctx, credential)
		return controlplane.Credential{SecretID: value.SecretID, SecretKey: value.SecretKey, Token: value.Token}, err
	}
	return &controlAdapter{service: controlplane.NewService(controlplane.ServiceConfig{Region: region, ControlEndpoint: controlEndpoint, MonitorEndpoint: monitorEndpoint, Timeout: timeout, HTTPClient: client, Credential: source}), endpoint: controlEndpoint}
}

func (c *controlAdapter) Create(ctx context.Context, opts CreateOptions) (SandboxInfo, error) {
	in := controlplane.CreateRequest{ToolID: opts.Tool.ID, ToolName: opts.Tool.Name, Timeout: opts.Timeout, ClientToken: opts.ClientToken, Metadata: opts.Metadata, Env: opts.Env, AuthMode: string(opts.AuthMode)}
	for _, mount := range opts.MountOptions {
		in.MountOptions = append(in.MountOptions, controlplane.MountOption{Name: mount.Name, MountPath: mount.MountPath, SubPath: mount.SubPath, ReadOnly: mount.ReadOnly})
	}
	out, err := c.service.Create(ctx, in)
	return sandboxInfoFrom(out), normalizeError("", err)
}

func (c *controlAdapter) Get(ctx context.Context, id string) (SandboxInfo, error) {
	out, err := c.service.Get(ctx, id)
	return sandboxInfoFrom(out), normalizeError("", err)
}

func (c *controlAdapter) Connect(ctx context.Context, id string, timeout time.Duration) (SandboxInfo, error) {
	out, err := c.service.Connect(ctx, id, timeout)
	if err != nil {
		err = mapMutationError(id, err)
	}
	return sandboxInfoFrom(out), normalizeError("", err)
}

func (c *controlAdapter) List(ctx context.Context, opts SandboxListOptions) (SandboxPage, error) {
	in, err := controlListRequest(opts)
	if err != nil {
		return SandboxPage{}, err
	}
	out, err := c.service.List(ctx, in)
	if err != nil {
		return SandboxPage{}, normalizeError("", err)
	}
	items := make([]SandboxInfo, 0, len(out.Items))
	for _, item := range out.Items {
		items = append(items, sandboxInfoFrom(item))
	}
	next := opts.Offset + len(items)
	var nextPtr *int
	if next < out.TotalCount {
		nextPtr = &next
	}
	return SandboxPage{Items: items, TotalCount: out.TotalCount, NextOffset: nextPtr}, nil
}

func (c *controlAdapter) Pause(ctx context.Context, id string, opts PauseOptions) (SandboxInfo, error) {
	out, err := c.service.Pause(ctx, id, opts.Mode != PauseDisk)
	return sandboxInfoFrom(out), normalizeError("", err)
}

func (c *controlAdapter) Resume(ctx context.Context, id string, opts ResumeOptions) (SandboxInfo, error) {
	out, err := c.service.Resume(ctx, id, opts.Timeout)
	return sandboxInfoFrom(out), normalizeError("", err)
}

func (c *controlAdapter) Delete(ctx context.Context, id string) error {
	return normalizeError("", c.service.Delete(ctx, id))
}

func (c *controlAdapter) waitFor(ctx context.Context, id string, target SandboxState) (SandboxInfo, error) {
	out, err := c.service.WaitFor(ctx, id, controlplane.State(target))
	return sandboxInfoFrom(out), normalizeError("", err)
}

func (c *controlAdapter) dataPlane(ctx context.Context, id string) (dataPlane, error) {
	generation, err := c.service.Runtime(ctx, id)
	if err != nil {
		return nil, normalizeError("", err)
	}
	return newRuntimeDataPlane(generation), nil
}

func (c *controlAdapter) Update(ctx context.Context, id string, opts UpdateOptions) error {
	err := c.service.Update(ctx, id, opts.Timeout, opts.MetadataUpsert)
	if err == nil {
		return nil
	}
	return mapMutationError(id, err)
}

func mapMutationError(id string, err error) error {
	var mutation *controlplane.MutationError
	if !errors.As(err, &mutation) {
		return updateFailure(id, MutationUnknown, MutationSubmit, normalizeError("Sandbox.Update", err))
	}
	return updateFailure(id, MutationState(mutation.State), MutationPhase(mutation.Phase), normalizeError("Sandbox.Update", mutation.Cause))
}

func (c *controlAdapter) Query(ctx context.Context, q monitorRequest) (monitorResponse, error) {
	out, err := c.service.QueryMetric(ctx, controlplane.MetricRequest{Name: string(q.Metric), InstanceID: q.InstanceID, ToolID: q.ToolID, Start: q.Start, End: q.End, Period: q.Period})
	if err != nil {
		return monitorResponse{}, normalizeError("", err)
	}
	points := make([]MetricPoint, 0, len(out.Points))
	for _, point := range out.Points {
		points = append(points, MetricPoint{Timestamp: point.Timestamp, Value: point.Value})
	}
	resolution := q.Period
	if out.Resolution > 0 {
		resolution = out.Resolution
	}
	truncated := len(points) >= 1440 && q.End.Sub(q.Start) > resolution*time.Duration(len(points))
	return monitorResponse{Points: points, Resolution: resolution, Truncated: truncated, RequestID: out.RequestID}, nil
}

func sandboxInfoFrom(value controlplane.Sandbox) SandboxInfo {
	return SandboxInfo{ID: value.ID, ToolID: value.ToolID, ToolName: value.ToolName, State: SandboxState(value.State), CreatedAt: value.CreatedAt, ExpiresAt: value.ExpiresAt, Metadata: value.Metadata}
}

func instanceFrom(value controlplane.Instance) SandboxInfo {
	return sandboxInfoFrom(controlplane.SandboxForTest(value))
}

type monitorAdapter struct{ *controlAdapter }

func newTencentMonitor(region string, credential CredentialProvider, client *http.Client) *monitorAdapter {
	return newTencentMonitorWithConfig(region, credential, "monitor.tencentcloudapi.com", client, 30*time.Second)
}

func newTencentMonitorWithConfig(region string, credential CredentialProvider, endpoint string, client *http.Client, timeout time.Duration) *monitorAdapter {
	return &monitorAdapter{newControlAdapter(region, credential, "ags.tencentcloudapi.com", endpoint, client, timeout)}
}

func (m *monitorAdapter) Query(ctx context.Context, q monitorRequest) (monitorResponse, error) {
	return m.controlAdapter.Query(ctx, q)
}

func copyListOptions(opts SandboxListOptions) SandboxListOptions {
	opts.States = append([]SandboxState(nil), opts.States...)
	metadata := make(map[string][]string, len(opts.Metadata))
	for key, values := range opts.Metadata {
		metadata[key] = append([]string(nil), values...)
	}
	opts.Metadata = metadata
	return opts
}

func controlListRequest(opts SandboxListOptions) (controlplane.ListRequest, error) {
	in := controlplane.ListRequest{ToolID: opts.ToolID, Offset: opts.Offset, Limit: opts.Limit, Metadata: opts.Metadata}
	for _, state := range opts.States {
		var internal controlplane.State
		switch state {
		case Creating:
			internal = controlplane.StateCreating
		case Running, Pausing, Paused, Stopping, Stopped, Failed:
			internal = controlplane.State(state)
		case Resuming:
			return controlplane.ListRequest{}, codeError(Unsupported, "Sandboxes.List", "LIST_STATE_UNAVAILABLE")
		default:
			return controlplane.ListRequest{}, codeError(InvalidArgument, "Sandboxes.List", "LIST_STATE_INVALID")
		}
		in.States = append(in.States, internal)
	}
	if len(opts.Metadata) > 5 {
		return controlplane.ListRequest{}, codeError(InvalidArgument, "Sandboxes.List", "LIST_METADATA_INVALID")
	}
	for key, values := range opts.Metadata {
		if key == "" || strings.TrimSpace(key) != key || strings.ContainsRune(key, 0) || len(values) == 0 {
			return controlplane.ListRequest{}, codeError(InvalidArgument, "Sandboxes.List", "LIST_METADATA_INVALID")
		}
	}
	for _, values := range opts.Metadata {
		if len(values) > 1 {
			return controlplane.ListRequest{}, codeError(Unsupported, "Sandboxes.List", "LIST_METADATA_OR_UNAVAILABLE")
		}
	}
	return in, nil
}
