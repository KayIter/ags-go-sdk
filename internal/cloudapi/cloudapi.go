// Package cloudapi isolates Tencent Cloud's generated SDK from AGS's public API.
//
// It owns TC3 signing, wire models, endpoints, and Cloud API error extraction.
// The parent ags package owns product semantics and maps these neutral values to
// the stable Native SDK surface.
package cloudapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	ags "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/ags/v20250920"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	tcerr "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/errors"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/profile"
	monitor "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/monitor/v20180724"
)

type Credential struct{ SecretID, SecretKey, Token string }
type CredentialSource func(context.Context) (Credential, error)

type Config struct {
	Region, Endpoint string
	Timeout          time.Duration
	Transport        http.RoundTripper
}

type Error struct {
	Code, RequestID string
	Cause           error
}

func (e *Error) Error() string {
	if e.Code == "" {
		return fmt.Sprintf("Tencent Cloud SDK request failed: %v", e.Cause)
	}
	return "Tencent Cloud SDK request failed: " + e.Code
}
func (e *Error) Unwrap() error { return e.Cause }

type Instance struct {
	ID, ToolID, ToolName, Status, CreatedAt, ExpiresAt string
	Metadata                                           map[string]string
}

type CreateInput struct {
	ToolID, ToolName, Timeout, ClientToken string
	Metadata                               map[string]string
	Env                                    map[string]string
	MountOptions                           []MountOption
	AuthMode                               string
}

type MountOption struct {
	Name, MountPath, SubPath string
	ReadOnly                 *bool
}

func (in CreateInput) String() string   { return "CreateInput{REDACTED}" }
func (in CreateInput) GoString() string { return in.String() }

type ListInput struct {
	InstanceIDs []string
	ToolID      string
	Offset      int
	Limit       int
	Statuses    []string
	Metadata    map[string][]string
}
type Page struct {
	Items      []Instance
	TotalCount int
}
type MonitorInput struct {
	Namespace, MetricName, InstanceID, ToolID string
	Period                                    time.Duration
	Start, End                                time.Time
}
type Point struct {
	Timestamp time.Time
	Value     float64
}
type MonitorOutput struct {
	Points    []Point
	Period    time.Duration
	RequestID string
}

type AGS struct {
	config     Config
	credential CredentialSource
}

func NewAGS(config Config, credential CredentialSource) *AGS {
	return &AGS{config: config, credential: credential}
}

func (a *AGS) Create(ctx context.Context, in CreateInput) (Instance, error) {
	client, callCtx, cancel, err := a.client(ctx)
	if err != nil {
		return Instance{}, err
	}
	defer cancel()
	req := ags.NewStartSandboxInstanceRequest()
	req.CustomConfiguration = createEnvironment(in.Env)
	req.ToolId, req.ToolName = stringPtr(in.ToolID), stringPtr(in.ToolName)
	req.Timeout, req.ClientToken = stringPtr(in.Timeout), stringPtr(in.ClientToken)
	req.AuthMode = stringPtr(in.AuthMode)
	for _, mount := range in.MountOptions {
		entry := &ags.MountOption{
			Name:      stringPtr(mount.Name),
			MountPath: stringPtr(mount.MountPath),
			SubPath:   stringPtr(mount.SubPath),
			ReadOnly:  mount.ReadOnly,
		}
		req.MountOptions = append(req.MountOptions, entry)
	}
	metadataKeys := make([]string, 0, len(in.Metadata))
	for name := range in.Metadata {
		metadataKeys = append(metadataKeys, name)
	}
	sort.Strings(metadataKeys)
	for _, name := range metadataKeys {
		value := in.Metadata[name]
		n, v := name, value
		req.Metadata = append(req.Metadata, &ags.MetadataVar{Name: &n, Value: &v})
	}
	response, err := client.StartSandboxInstanceWithContext(callCtx, req)
	if err != nil {
		return Instance{}, wrapCall(callCtx, err)
	}
	if response == nil || response.Response == nil || response.Response.Instance == nil {
		return Instance{}, &Error{Code: "ClientError.MalformedResponse", Cause: errors.New("StartSandboxInstance response has no Instance")}
	}
	return fromInstance(response.Response.Instance), nil
}

func (a *AGS) List(ctx context.Context, in ListInput) (Page, error) {
	client, callCtx, cancel, err := a.client(ctx)
	if err != nil {
		return Page{}, err
	}
	defer cancel()
	req := ags.NewDescribeSandboxInstanceListRequest()
	for _, id := range in.InstanceIDs {
		id := id
		req.InstanceIds = append(req.InstanceIds, &id)
	}
	req.ToolId = stringPtr(in.ToolID)
	offset, limit := int64(in.Offset), int64(in.Limit)
	req.Offset, req.Limit = &offset, &limit
	req.Filters = listFilters(in)
	response, err := client.DescribeSandboxInstanceListWithContext(callCtx, req)
	if err != nil {
		return Page{}, wrapCall(callCtx, err)
	}
	if response == nil || response.Response == nil {
		return Page{}, &Error{Code: "ClientError.MalformedResponse", Cause: errors.New("DescribeSandboxInstanceList response is empty")}
	}
	out := Page{TotalCount: int(valueInt64(response.Response.TotalCount))}
	for _, item := range response.Response.InstanceSet {
		if item != nil {
			out.Items = append(out.Items, fromInstance(item))
		}
	}
	return out, nil
}

func (a *AGS) Pause(ctx context.Context, id string, memory bool) error {
	client, callCtx, cancel, err := a.client(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	req := ags.NewPauseSandboxInstanceRequest()
	req.InstanceId, req.Memory = &id, &memory
	_, err = client.PauseSandboxInstanceWithContext(callCtx, req)
	return wrapCall(callCtx, err)
}
func (a *AGS) Resume(ctx context.Context, id, timeout string) error {
	client, callCtx, cancel, err := a.client(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	req := ags.NewResumeSandboxInstanceRequest()
	req.InstanceId, req.Timeout = &id, stringPtr(timeout)
	_, err = client.ResumeSandboxInstanceWithContext(callCtx, req)
	return wrapCall(callCtx, err)
}
func (a *AGS) Stop(ctx context.Context, id string) error {
	client, callCtx, cancel, err := a.client(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	req := ags.NewStopSandboxInstanceRequest()
	req.InstanceId = &id
	_, err = client.StopSandboxInstanceWithContext(callCtx, req)
	return wrapCall(callCtx, err)
}
func (a *AGS) AcquireToken(ctx context.Context, id string) (string, error) {
	client, callCtx, cancel, err := a.client(ctx)
	if err != nil {
		return "", err
	}
	defer cancel()
	req := ags.NewAcquireSandboxInstanceTokenRequest()
	req.InstanceId = &id
	response, err := client.AcquireSandboxInstanceTokenWithContext(callCtx, req)
	if err != nil {
		return "", wrapCall(callCtx, err)
	}
	if response == nil || response.Response == nil {
		return "", &Error{Code: "ClientError.MalformedResponse", Cause: errors.New("AcquireSandboxInstanceToken response is empty")}
	}
	// Token is the instance access credential accepted by the management data
	// plane (envd and Code). TrafficToken is a separate, restricted credential
	// for sandbox business-port traffic and must not be injected here.
	return valueString(response.Response.Token), nil
}

func (a *AGS) client(ctx context.Context) (*ags.Client, context.Context, context.CancelFunc, error) {
	credential, err := a.credential(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	if credential.SecretID == "" || credential.SecretKey == "" {
		return nil, nil, nil, &Error{Code: "AuthFailure.CredentialMissing"}
	}
	profile, err := clientProfile(a.config)
	if err != nil {
		return nil, nil, nil, err
	}
	signed := common.NewCredential(credential.SecretID, credential.SecretKey)
	if credential.Token != "" {
		signed = common.NewTokenCredential(credential.SecretID, credential.SecretKey, credential.Token)
	}
	client, err := ags.NewClient(signed, a.config.Region, profile)
	if err != nil {
		return nil, nil, nil, wrap(err)
	}
	client.WithHttpTransport(noRedirect(a.config.Transport))
	callCtx, cancel := context.WithTimeout(ctx, a.config.Timeout)
	return client, callCtx, cancel, nil
}

type Monitor struct {
	config     Config
	credential CredentialSource
}

func NewMonitor(config Config, credential CredentialSource) *Monitor {
	return &Monitor{config: config, credential: credential}
}
func (m *Monitor) Query(ctx context.Context, in MonitorInput) (MonitorOutput, error) {
	credential, err := m.credential(ctx)
	if err != nil {
		return MonitorOutput{}, err
	}
	if credential.SecretID == "" || credential.SecretKey == "" {
		return MonitorOutput{}, &Error{Code: "AuthFailure.CredentialMissing"}
	}
	clientProfile, err := clientProfile(m.config)
	if err != nil {
		return MonitorOutput{}, err
	}
	signed := common.NewCredential(credential.SecretID, credential.SecretKey)
	if credential.Token != "" {
		signed = common.NewTokenCredential(credential.SecretID, credential.SecretKey, credential.Token)
	}
	client, err := monitor.NewClient(signed, m.config.Region, clientProfile)
	if err != nil {
		return MonitorOutput{}, wrap(err)
	}
	client.WithHttpTransport(noRedirect(m.config.Transport))
	callCtx, cancel := context.WithTimeout(ctx, m.config.Timeout)
	defer cancel()
	req := monitor.NewGetMonitorDataRequest()
	req.Namespace, req.MetricName = &in.Namespace, &in.MetricName
	period := uint64(in.Period.Seconds())
	req.Period = &period
	start, end := in.Start.Format(time.RFC3339), in.End.Format(time.RFC3339)
	req.StartTime, req.EndTime = &start, &end
	dimensions := []*monitor.Dimension{{Name: stringPtr("instance_id"), Value: &in.InstanceID}}
	if in.ToolID != "" {
		dimensions = append(dimensions, &monitor.Dimension{Name: stringPtr("tool_id"), Value: &in.ToolID})
	}
	req.Instances = []*monitor.Instance{{Dimensions: dimensions}}
	response, err := client.GetMonitorDataWithContext(callCtx, req)
	if err != nil {
		return MonitorOutput{}, wrapCall(callCtx, err)
	}
	if response == nil || response.Response == nil {
		return MonitorOutput{}, &Error{Code: "ClientError.MalformedResponse", Cause: errors.New("GetMonitorData response is empty")}
	}
	out := MonitorOutput{Period: time.Duration(valueUint64(response.Response.Period)) * time.Second, RequestID: valueString(response.Response.RequestId)}
	for _, set := range response.Response.DataPoints {
		if set == nil {
			continue
		}
		n := len(set.Timestamps)
		if len(set.Values) < n {
			n = len(set.Values)
		}
		for i := 0; i < n; i++ {
			if set.Timestamps[i] != nil && set.Values[i] != nil {
				out.Points = append(out.Points, Point{Timestamp: time.Unix(int64(*set.Timestamps[i]), 0).UTC(), Value: float64(*set.Values[i])})
			}
		}
	}
	return out, nil
}

func clientProfile(config Config) (*profile.ClientProfile, error) {
	endpoint := config.Endpoint
	if !strings.Contains(endpoint, "://") {
		endpoint = "https://" + endpoint
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" || (parsed.Path != "" && parsed.Path != "/") {
		return nil, &Error{Code: "ClientError.InvalidEndpoint", Cause: fmt.Errorf("invalid Tencent Cloud endpoint %q", config.Endpoint)}
	}
	httpProfile := profile.NewHttpProfile()
	httpProfile.Endpoint = parsed.Host
	httpProfile.Scheme = parsed.Scheme
	httpProfile.ReqMethod = "POST"
	httpProfile.ReqTimeout = max(1, int(config.Timeout.Round(time.Second)/time.Second))
	value := profile.NewClientProfile()
	value.HttpProfile = httpProfile
	return value, nil
}

type rejectRedirects struct{ base http.RoundTripper }

func noRedirect(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return rejectRedirects{base: base}
}
func (t rejectRedirects) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := t.base.RoundTrip(request)
	if err != nil {
		return response, err
	}
	if response.StatusCode >= 300 && response.StatusCode < 400 {
		io.Copy(io.Discard, response.Body)
		response.Body.Close()
		return nil, fmt.Errorf("Tencent Cloud endpoint redirect rejected: HTTP %d", response.StatusCode)
	}
	// The official Go SDK discards a Tencent JSON error envelope when HTTP is
	// non-200. Normalize only a valid Cloud API error to 200 so its official
	// decoder preserves Code and RequestId for the public stable error contract.
	if response.StatusCode != http.StatusOK && response.Body != nil {
		body, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		response.Body = io.NopCloser(bytes.NewReader(body))
		var envelope struct {
			Response struct {
				Error *struct {
					Code string `json:"Code"`
				} `json:"Error"`
			} `json:"Response"`
		}
		if json.Unmarshal(body, &envelope) == nil && envelope.Response.Error != nil && envelope.Response.Error.Code != "" {
			response.StatusCode = http.StatusOK
			response.Status = "200 OK"
		}
	}
	return response, nil
}

func wrap(err error) error {
	if err == nil {
		return nil
	}
	var sdk *tcerr.TencentCloudSDKError
	if errors.As(err, &sdk) {
		return &Error{Code: sdk.GetCode(), RequestID: sdk.GetRequestId(), Cause: err}
	}
	return &Error{Cause: err}
}
func wrapCall(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return wrap(err)
}
func fromInstance(value *ags.SandboxInstance) Instance {
	metadata := map[string]string{}
	for _, item := range value.Metadata {
		if item != nil {
			metadata[valueString(item.Name)] = valueString(item.Value)
		}
	}
	return Instance{ID: valueString(value.InstanceId), ToolID: valueString(value.ToolId), ToolName: valueString(value.ToolName), Status: valueString(value.Status), CreatedAt: valueString(value.CreateTime), ExpiresAt: valueString(value.ExpiresAt), Metadata: metadata}
}
func stringPtr(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
func valueString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
func valueInt64(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}
func valueUint64(value *uint64) uint64 {
	if value == nil {
		return 0
	}
	return *value
}
