package controlplane

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/dataplane"
	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/model"
	internalruntime "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/runtime"
)

// Service owns Cloud control-plane orchestration, Monitor transport, and
// acquisition of immutable runtime generations. Public API types are mapped by
// the root facade and never cross this boundary.
type Service struct {
	timeout time.Duration
	ags     *AGS
	monitor *Monitor
	runtime *dataplane.CloudConnector
}

type ServiceConfig struct {
	Region, ControlEndpoint, MonitorEndpoint string
	Timeout                                  time.Duration
	HTTPClient                               *http.Client
	Credential                               CredentialSource
	RuntimeSource                            dataplane.AccessTokenSource
}

func NewService(config ServiceConfig) *Service {
	client := cloneHTTPClient(config.HTTPClient)
	agsAPI := NewAGS(Config{Region: config.Region, Endpoint: config.ControlEndpoint, Timeout: config.Timeout, Transport: client.Transport}, config.Credential)
	monitorAPI := NewMonitor(Config{Region: config.Region, Endpoint: config.MonitorEndpoint, Timeout: config.Timeout, Transport: client.Transport}, config.Credential)
	source := config.RuntimeSource
	if source == nil {
		source = agsAPI
	}
	return &Service{timeout: config.Timeout, ags: agsAPI, monitor: monitorAPI, runtime: dataplane.NewCloudConnector(config.Region, source, client)}
}

func cloneHTTPClient(client *http.Client) *http.Client {
	value := http.Client{}
	if client != nil {
		value = *client
	}
	value.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &value
}

type State string

const (
	StateCreating State = "CREATING"
	StateRunning  State = "RUNNING"
	StatePausing  State = "PAUSING"
	StatePaused   State = "PAUSED"
	StateResuming State = "RESUMING"
	StateStopping State = "STOPPING"
	StateStopped  State = "STOPPED"
	StateFailed   State = "FAILED"
	StateUnknown  State = "UNKNOWN"
)

type Sandbox struct {
	ID, ToolID, ToolName string
	State                State
	CreatedAt, ExpiresAt time.Time
	Metadata             map[string]string
}

type CreateRequest struct {
	ToolID, ToolName, ClientToken string
	Timeout                       *time.Duration
	Metadata, Env                 map[string]string
	MountOptions                  []MountOption
	AuthMode                      string
}

type ListRequest struct {
	ToolID   string
	Offset   int
	Limit    int
	States   []State
	Metadata map[string][]string
}

type SandboxPage struct {
	Items      []Sandbox
	TotalCount int
}

type MetricRequest struct {
	Name, InstanceID, ToolID string
	Start, End               time.Time
	Period                   time.Duration
}

type MetricPoint struct {
	Timestamp time.Time
	Value     float64
}

type MetricResponse struct {
	Points     []MetricPoint
	Resolution time.Duration
	RequestID  string
}

func (s *Service) Create(ctx context.Context, request CreateRequest) (Sandbox, error) {
	in := CreateInput{ToolID: request.ToolID, ToolName: request.ToolName, ClientToken: request.ClientToken, Metadata: request.Metadata, Env: request.Env, MountOptions: request.MountOptions, AuthMode: request.AuthMode}
	if request.Timeout != nil {
		in.Timeout = request.Timeout.String()
	}
	out, err := s.ags.Create(ctx, in)
	return sandboxFrom(out), cloudError(err, "StartSandboxInstance")
}

func (s *Service) Get(ctx context.Context, id string) (Sandbox, error) {
	out, err := s.ags.List(ctx, ListInput{InstanceIDs: []string{id}, Limit: 1})
	if err != nil {
		return Sandbox{}, cloudError(err, "DescribeSandboxInstanceList")
	}
	if len(out.Items) != 1 || out.Items[0].ID != id {
		return Sandbox{}, failure(model.NotFound, "Sandboxes.Get", "INSTANCE_NOT_FOUND", false, nil)
	}
	return sandboxFrom(out.Items[0]), nil
}

func (s *Service) List(ctx context.Context, request ListRequest) (SandboxPage, error) {
	in := ListInput{ToolID: request.ToolID, Offset: request.Offset, Limit: request.Limit, Metadata: request.Metadata}
	for _, state := range request.States {
		switch state {
		case StateCreating:
			in.Statuses = append(in.Statuses, "STARTING")
		default:
			in.Statuses = append(in.Statuses, string(state))
		}
	}
	out, err := s.ags.List(ctx, in)
	if err != nil {
		return SandboxPage{}, cloudError(err, "DescribeSandboxInstanceList")
	}
	page := SandboxPage{TotalCount: out.TotalCount, Items: make([]Sandbox, 0, len(out.Items))}
	for _, item := range out.Items {
		page.Items = append(page.Items, sandboxFrom(item))
	}
	return page, nil
}

func (s *Service) Connect(ctx context.Context, id string, timeout time.Duration) (Sandbox, error) {
	info, err := s.Get(ctx, id)
	if err != nil {
		return Sandbox{}, err
	}
	if info.State == StateCreating || info.State == StateResuming || info.State == StatePausing {
		target := StateRunning
		if info.State == StatePausing {
			target = StatePaused
		}
		if info, err = s.WaitFor(ctx, id, target); err != nil {
			return Sandbox{}, err
		}
	}
	if info.State == StatePaused {
		return s.Resume(ctx, id, &timeout)
	}
	if info.State != StateRunning {
		return Sandbox{}, failure(model.Conflict, "Sandboxes.Connect", "STATE_NOT_CONNECTABLE", false, nil)
	}
	if timeout < 5*time.Minute {
		return Sandbox{}, failure(model.InvalidArgument, "Sandboxes.Connect", "TIMEOUT_OUT_OF_RANGE", false, nil)
	}
	if info.ExpiresAt.IsZero() {
		return Sandbox{}, failure(model.Protocol, "Sandboxes.Connect", "EXPIRY_REQUIRED", false, nil)
	}
	if time.Until(info.ExpiresAt) < timeout {
		if err := s.Update(ctx, id, &timeout, nil); err != nil {
			return Sandbox{}, err
		}
		return s.Get(ctx, id)
	}
	return info, nil
}

func (s *Service) Pause(ctx context.Context, id string, memory bool) (Sandbox, error) {
	if err := s.ags.Pause(ctx, id, memory); err != nil {
		return Sandbox{}, cloudError(err, "PauseSandboxInstance")
	}
	return s.WaitFor(ctx, id, StatePaused)
}

func (s *Service) Resume(ctx context.Context, id string, timeout *time.Duration) (Sandbox, error) {
	value := ""
	if timeout != nil {
		value = timeout.String()
	}
	if err := s.ags.Resume(ctx, id, value); err != nil {
		return Sandbox{}, cloudError(err, "ResumeSandboxInstance")
	}
	return s.WaitFor(ctx, id, StateRunning)
}

func (s *Service) Delete(ctx context.Context, id string) error {
	return cloudError(s.ags.Stop(ctx, id), "StopSandboxInstance")
}

type MutationError struct {
	State, Phase string
	Cause        error
}

func (e *MutationError) Error() string { return "sandbox update failed during " + e.Phase }
func (e *MutationError) Unwrap() error { return e.Cause }

func (s *Service) Update(ctx context.Context, id string, timeout *time.Duration, upsert map[string]string) error {
	in := UpdateInput{InstanceID: id}
	if timeout != nil {
		in.Timeout = timeout.String()
	}
	if len(upsert) > 0 {
		metadata, err := s.ags.MetadataForUpdate(ctx, id)
		if err != nil {
			return &MutationError{State: "NOT_SENT", Phase: "READ_METADATA", Cause: cloudError(err, "DescribeSandboxInstanceList")}
		}
		for key, value := range upsert {
			metadata[key] = value
		}
		in.Metadata = metadata
	}
	if err := ctx.Err(); err != nil {
		return &MutationError{State: "NOT_SENT", Phase: "SUBMIT", Cause: contextError(ctx, "Sandbox.Update")}
	}
	if err := s.ags.Update(ctx, in); err != nil {
		return &MutationError{State: "UNKNOWN", Phase: "SUBMIT", Cause: cloudError(err, "UpdateSandboxInstance")}
	}
	return nil
}

func (s *Service) Runtime(ctx context.Context, id string) (*internalruntime.Generation, error) {
	wire, err := s.runtime.Connect(ctx, id)
	if err != nil {
		return nil, runtimeError(err, "AcquireSandboxInstanceToken")
	}
	return internalruntime.NewGeneration(wire, s.timeout), nil
}

func (s *Service) QueryMetric(ctx context.Context, request MetricRequest) (MetricResponse, error) {
	out, err := s.monitor.Query(ctx, MonitorInput{Namespace: "QCE/AGS", MetricName: request.Name, InstanceID: request.InstanceID, ToolID: request.ToolID, Start: request.Start, End: request.End, Period: request.Period})
	if err != nil {
		return MetricResponse{}, cloudError(err, "Metrics.Get")
	}
	response := MetricResponse{Resolution: out.Period, RequestID: out.RequestID, Points: make([]MetricPoint, 0, len(out.Points))}
	for _, point := range out.Points {
		response.Points = append(response.Points, MetricPoint{Timestamp: point.Timestamp, Value: point.Value})
	}
	return response, nil
}

// WaitFor polls Cloud state without acquiring a runtime generation.
func (s *Service) WaitFor(ctx context.Context, id string, target State) (Sandbox, error) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		info, err := s.Get(ctx, id)
		if err != nil {
			return Sandbox{}, err
		}
		if info.State == target {
			return info, nil
		}
		if info.State == StateFailed || info.State == StateStopped {
			return Sandbox{}, failure(model.Conflict, "Sandbox.WaitFor", "TERMINAL_STATE", false, nil)
		}
		select {
		case <-ctx.Done():
			return Sandbox{}, contextError(ctx, "Sandbox.WaitFor")
		case <-ticker.C:
		}
	}
}

func sandboxFrom(value Instance) Sandbox {
	state := State(value.Status)
	switch state {
	case "STARTING":
		state = StateCreating
	case StateRunning, StatePausing, StatePaused, StateResuming, StateStopping, StateStopped, StateFailed:
	case "STOP_FAILED", "PAUSE_FAILED":
		state = StateFailed
	default:
		state = StateUnknown
	}
	created, _ := time.Parse(time.RFC3339, value.CreatedAt)
	expires, _ := time.Parse(time.RFC3339, value.ExpiresAt)
	return Sandbox{ID: value.ID, ToolID: value.ToolID, ToolName: value.ToolName, State: state, CreatedAt: created, ExpiresAt: expires, Metadata: value.Metadata}
}

// SandboxForTest exposes low-level normalization without exposing Cloud SDK types.
func SandboxForTest(value Instance) Sandbox { return sandboxFrom(value) }

func contextError(ctx context.Context, operation string) error {
	code := model.Canceled
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		code = model.DeadlineExceeded
	}
	return &model.Error{Code: code, Operation: operation, Cause: ctx.Err()}
}

func failure(code, operation, reason string, retryable bool, cause error) error {
	return &model.Error{Code: code, Operation: operation, Reason: reason, Retryable: retryable, Cause: cause}
}

func cloudError(err error, operation string) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return &model.Error{Code: model.Canceled, Operation: operation, Cause: err}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &model.Error{Code: model.DeadlineExceeded, Operation: operation, Cause: err}
	}
	code, reason, retryable, requestID := model.Unavailable, "CLOUD_API_UNAVAILABLE", true, ""
	var cloud *Error
	if errors.As(err, &cloud) {
		requestID = cloud.RequestID
		value := strings.ToLower(cloud.Code)
		switch {
		case strings.Contains(value, "authfailure") || strings.Contains(value, "invalidcredential"):
			code, reason, retryable = model.Unauthenticated, "CLOUD_AUTHENTICATION_FAILED", false
		case strings.Contains(value, "unauthorizedoperation") || strings.Contains(value, "permission"):
			code, reason, retryable = model.PermissionDenied, "CLOUD_PERMISSION_DENIED", false
		case strings.Contains(value, "notfound") || strings.Contains(value, "notexist"):
			code, reason, retryable = model.NotFound, "RESOURCE_NOT_FOUND", false
		case strings.Contains(value, "conflict") || strings.Contains(value, "incorrectstate"):
			code, reason, retryable = model.Conflict, "RESOURCE_CONFLICT", false
		case strings.Contains(value, "limitexceeded") || strings.Contains(value, "requestlimit"):
			code, reason = model.ResourceExhausted, "REQUEST_LIMIT_EXCEEDED"
		case strings.Contains(value, "invalidparameter") || strings.Contains(value, "missingparameter"):
			code, reason, retryable = model.InvalidArgument, "CLOUD_INVALID_ARGUMENT", false
		case strings.Contains(value, "malformedresponse"):
			code, reason, retryable = model.Protocol, "CLOUD_RESPONSE_MALFORMED", false
		}
	}
	return &model.Error{Code: code, Operation: operation, Reason: reason, Retryable: retryable, RequestID: requestID, Cause: err}
}

// NormalizeCloudError converts a low-level Cloud failure into the internal
// stable error model. It is exported only across internal package boundaries.
func NormalizeCloudError(err error, operation string) error { return cloudError(err, operation) }

func runtimeError(err error, operation string) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return &model.Error{Code: model.Canceled, Operation: operation, Cause: err}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &model.Error{Code: model.DeadlineExceeded, Operation: operation, Cause: err}
	}
	code, reason, retryable, cause := model.Unavailable, "REQUEST_FAILED", true, err
	var wire *dataplane.WireError
	var timeout net.Error
	if errors.As(err, &wire) {
		reason, retryable = wire.Reason, wire.Retryable
		if reason == "" {
			reason = "REQUEST_FAILED"
		}
		if wire.Detail != "" {
			cause = errors.New(wire.Detail)
		}
		switch {
		case wire.Kind == dataplane.ErrorProtocol:
			code = model.Protocol
		case wire.Kind == dataplane.ErrorHTTP && wire.StatusCode == 401:
			code = model.Unauthenticated
		case wire.Kind == dataplane.ErrorHTTP && wire.StatusCode == 403:
			code = model.PermissionDenied
		case wire.Kind == dataplane.ErrorHTTP && wire.StatusCode == 404:
			code = model.NotFound
		case wire.Kind == dataplane.ErrorHTTP && wire.StatusCode == 409:
			code = model.Conflict
		case wire.Kind == dataplane.ErrorHTTP && wire.StatusCode == 429:
			code = model.ResourceExhausted
		}
	} else if errors.As(err, &timeout) && timeout.Timeout() {
		code = model.DeadlineExceeded
	}
	return &model.Error{Code: code, Operation: operation, Reason: reason, Retryable: retryable, Cause: cause}
}
