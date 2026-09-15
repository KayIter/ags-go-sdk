package ags

import (
	"context"
	"errors"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

// ClientOption configures NewClient. Use the provided With functions; private configuration is
// not an extension API.
type ClientOption func(*clientConfig)
type clientConfig struct {
	region          string
	credential      CredentialProvider
	timeout         time.Duration
	control         controlPlane
	monitor         monitorTransport
	controlEndpoint string
	monitorEndpoint string
	httpClient      *http.Client
}

// WithRegion selects the nonempty Tencent Cloud region, overriding TENCENTCLOUD_REGION.
func WithRegion(v string) ClientOption { return func(c *clientConfig) { c.region = v } }

// WithCredential selects Cloud control and Metrics using a static SecretID/SecretKey pair.
func WithCredential(v CloudCredential) ClientOption {
	return func(c *clientConfig) { c.credential = v }
}

// WithTemporaryCredential explicitly selects Cloud control using a static SecretID, SecretKey,
// and optional session Token.
func WithTemporaryCredential(v TemporaryCloudCredential) ClientOption {
	return func(c *clientConfig) { c.credential = v }
}

// WithCredentialProvider selects a concurrent, per-request credential source.
func WithCredentialProvider(v CredentialProvider) ClientOption {
	return func(c *clientConfig) { c.credential = v }
}
func withControlPlane(cp controlPlane) ClientOption { return func(c *clientConfig) { c.control = cp } }
func withMonitorTransport(m monitorTransport) ClientOption {
	return func(c *clientConfig) { c.monitor = m }
}

// WithRequestTimeout sets a positive per-request timeout (default 30 seconds). Use context
// deadlines to bound orchestration and stream lifetime; this is not a sandbox lifetime.
func WithRequestTimeout(v time.Duration) ClientOption { return func(c *clientConfig) { c.timeout = v } }

// WithControlPlaneEndpoint selects the official Cloud AGS endpoint (default
// ags.tencentcloudapi.com).
func WithControlPlaneEndpoint(v string) ClientOption {
	return func(c *clientConfig) { c.controlEndpoint = v }
}

// WithMonitorEndpoint selects the official Cloud Monitor endpoint (default
// monitor.tencentcloudapi.com).
func WithMonitorEndpoint(v string) ClientOption {
	return func(c *clientConfig) { c.monitorEndpoint = v }
}

// WithHTTPClient supplies trusted HTTP configuration; nil uses SDK defaults. The SDK rejects
// redirects and reuses the transport without taking ownership of the shared client. Do not log
// or forward credentials in custom transports.
func WithHTTPClient(v *http.Client) ClientOption { return func(c *clientConfig) { c.httpClient = v } }

// noRedirectClient copies caller-supplied HTTP settings and forces redirects to be
// returned to the signed request path. Following a redirect could otherwise forward
// Cloud authorization headers to an endpoint that was not part of the signature.
func noRedirectClient(client *http.Client) *http.Client {
	value := http.Client{}
	if client != nil {
		value = *client
	}
	value.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &value
}

// Client binds region, providers, endpoints and HTTP configuration. Construct with NewClient;
// do not replace its public service pointer concurrently.
type Client struct {
	cfg       clientConfig
	sandboxes *SandboxManager
}

// Sandboxes returns the sandbox control service bound to this client's Cloud identity.
func (c *Client) Sandboxes() *SandboxManager { return c.sandboxes }

// NewClient constructs a client without creating a sandbox. Region defaults from
// TENCENTCLOUD_REGION; request timeout defaults to 30 seconds. Explicit control credentials
// are read for each signed request so temporary credentials can rotate within one logical
// Cloud identity.
func NewClient(opts ...ClientOption) (*Client, error) {
	c := clientConfig{region: os.Getenv("TENCENTCLOUD_REGION"), credential: EnvironmentCredentials{}, timeout: 30 * time.Second, controlEndpoint: "ags.tencentcloudapi.com", monitorEndpoint: "monitor.tencentcloudapi.com"}
	for _, opt := range opts {
		if opt == nil {
			return nil, codeError(InvalidArgument, "NewClient", "OPTION_REQUIRED")
		}
		opt(&c)
	}
	if c.region == "" {
		return nil, codeError(InvalidArgument, "NewClient", "REGION_REQUIRED")
	}
	if c.timeout <= 0 {
		return nil, codeError(InvalidArgument, "NewClient", "TIMEOUT_OUT_OF_RANGE")
	}
	c.httpClient = noRedirectClient(c.httpClient)
	if c.credential == nil {
		return nil, codeError(InvalidArgument, "NewClient", "CREDENTIAL_PROVIDER_REQUIRED")
	}
	var adapter *controlAdapter
	if c.control == nil || c.monitor == nil {
		adapter = newControlAdapter(c.region, c.credential, c.controlEndpoint, c.monitorEndpoint, c.httpClient, c.timeout)
	}
	if c.control == nil {
		c.control = adapter
	}
	if c.monitor == nil {
		c.monitor = adapter
	}
	out := &Client{cfg: c}
	out.sandboxes = &SandboxManager{client: out}
	return out, nil
}

// SandboxManager provides control-plane operations and returns the same Sandbox type as the
// shortcut package.
type SandboxManager struct{ client *Client }

// Delete explicitly stops an instance by ID without acquiring a data-plane
// connection. This also permits cleanup after a failed Create readiness check.
func (m *SandboxManager) Delete(ctx context.Context, id string) error {
	if id == "" {
		return codeError(InvalidArgument, "Sandboxes.Delete", "INSTANCE_ID_REQUIRED")
	}
	info, err := m.Get(ctx, id)
	var failure *Error
	if errors.As(err, &failure) && failure.Code == NotFound {
		return nil
	}
	if err != nil {
		return err
	}
	if info.State == Stopped {
		return nil
	}
	if info.State == Paused {
		if _, err = m.client.cfg.control.Resume(ctx, id, ResumeOptions{}); err != nil {
			return err
		}
	}
	err = m.client.cfg.control.Delete(ctx, id)
	if errors.As(err, &failure) && failure.Code == NotFound {
		return nil
	}
	return err
}

// Create submits one sandbox creation and waits for RUNNING and data-plane readiness within
// ctx. It never retries or automatically deletes an accepted resource. Post-acceptance errors
// preserve a known InstanceID. Returned handles require local Close and explicitly owned
// remote Delete.
func (m *SandboxManager) Create(ctx context.Context, opts CreateOptions) (*Sandbox, error) {
	var err error
	opts, err = copyCreateEnv(opts)
	if err != nil {
		return nil, err
	}
	if err := validateCreate(opts); err != nil {
		return nil, err
	}
	info, err := m.client.cfg.control.Create(ctx, opts)
	if err != nil {
		return nil, err
	}
	sandbox, err := m.connected(ctx, info)
	if err != nil {
		failure := &Error{Code: Unavailable, Operation: "Sandboxes.Create", Reason: "CONNECTION_FAILED", InstanceID: info.ID, Cause: err}
		var sdk *Error
		if errors.As(err, &sdk) {
			failure.Code, failure.Reason, failure.RequestID, failure.Retryable = sdk.Code, sdk.Reason, sdk.RequestID, sdk.Retryable
		}
		return nil, failure
	}
	return sandbox, nil
}

// Connect resumes a paused sandbox and extends a running sandbox only when its
// remaining lifetime is shorter than Timeout (five minutes by default).
func (m *SandboxManager) Connect(ctx context.Context, id string, options ...ConnectOptions) (*Sandbox, error) {
	if id == "" {
		return nil, codeError(InvalidArgument, "Sandboxes.Connect", "INSTANCE_ID_REQUIRED")
	}
	timeout, err := connectTimeout(options)
	if err != nil {
		return nil, err
	}
	info, err := m.client.cfg.control.Connect(ctx, id, timeout)
	if err != nil {
		return nil, err
	}
	return m.connected(ctx, info)
}

// Get reads the exact nonempty sandbox ID without extending lifetime, resuming or connecting
// envd. Missing resources return NotFound.
func (m *SandboxManager) Get(ctx context.Context, id string) (SandboxInfo, error) {
	if id == "" {
		return SandboxInfo{}, codeError(InvalidArgument, "Sandboxes.Get", "INSTANCE_ID_REQUIRED")
	}
	return m.client.cfg.control.Get(ctx, id)
}

// List returns one server-filtered Cloud page. Limit defaults to 20. Unsupported filters fail
// explicitly rather than being dropped.
func (m *SandboxManager) List(ctx context.Context, opts SandboxListOptions) (SandboxPage, error) {
	if opts.Limit == 0 {
		opts.Limit = 20
	}
	if opts.Limit < 1 || opts.Limit > 100 {
		return SandboxPage{}, codeError(InvalidArgument, "Sandboxes.List", "LIMIT_OUT_OF_RANGE")
	}
	if opts.Offset < 0 {
		return SandboxPage{}, codeError(InvalidArgument, "Sandboxes.List", "PAGINATION_OUT_OF_RANGE")
	}
	opts = copyListOptions(opts)
	return m.client.cfg.control.List(ctx, opts)
}
func (m *SandboxManager) connected(ctx context.Context, info SandboxInfo) (*Sandbox, error) {
	for info.State != Running {
		if info.State == Failed || info.State == Stopped {
			return nil, codeError(Conflict, "Sandboxes.Connect", "TERMINAL_STATE")
		}
		select {
		case <-ctx.Done():
			return nil, normalizeError("Sandboxes.Connect", ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
		var err error
		info, err = m.Get(ctx, info.ID)
		if err != nil {
			return nil, err
		}
	}
	dp, err := m.client.cfg.control.dataPlane(ctx, info.ID)
	if err != nil {
		return nil, err
	}
	if err := dp.Ready(ctx); err != nil {
		_ = dp.Close()
		return nil, err
	}
	return newSandbox(m.client, info.ID, dp), nil
}
func validateCreate(opts CreateOptions) error {
	if (opts.Tool.ID == "") == (opts.Tool.Name == "") {
		return codeError(InvalidArgument, "Sandboxes.Create", "EXACTLY_ONE_TOOL_REFERENCE_REQUIRED")
	}
	if opts.Timeout != nil && (*opts.Timeout < 30*time.Second || *opts.Timeout > 24*time.Hour || *opts.Timeout%time.Second != 0) {
		return codeError(InvalidArgument, "Sandboxes.Create", "TIMEOUT_OUT_OF_RANGE")
	}
	if len(opts.ClientToken) > 64 {
		return codeError(InvalidArgument, "Sandboxes.Create", "CLIENT_TOKEN_TOO_LONG")
	}
	for key, value := range opts.Metadata {
		if key == "" || strings.ContainsAny(key, "\x00\r\n") || strings.ContainsRune(value, 0) {
			return codeError(InvalidArgument, "Sandboxes.Create", "METADATA_INVALID")
		}
	}
	for _, mount := range opts.MountOptions {
		if mount.Name == "" || strings.ContainsRune(mount.Name, 0) || strings.ContainsRune(mount.MountPath, 0) || strings.ContainsRune(mount.SubPath, 0) {
			return codeError(InvalidArgument, "Sandboxes.Create", "MOUNT_OPTION_INVALID")
		}
	}
	switch opts.AuthMode {
	case "", SandboxAuthDefault, SandboxAuthToken, SandboxAuthNone, SandboxAuthPublic:
	default:
		return codeError(InvalidArgument, "Sandboxes.Create", "AUTH_MODE_INVALID")
	}
	return nil
}

// ConnectOptions controls sandbox lifetime, not the HTTP or context deadline.
type ConnectOptions struct {
	// Timeout defaults to five minutes. Connect never intentionally shortens a
	// running sandbox's remaining lifetime. Running updates require at least 300 seconds;
	// paused resume accepts at least 30 seconds. Both allow up to 24 hours.
	Timeout *time.Duration
}

func connectTimeout(options []ConnectOptions) (time.Duration, error) {
	if len(options) > 1 {
		return 0, codeError(InvalidArgument, "Sandboxes.Connect", "SINGLE_OPTIONS_REQUIRED")
	}
	timeout := 5 * time.Minute
	if len(options) == 1 && options[0].Timeout != nil {
		timeout = *options[0].Timeout
	}
	if timeout < 30*time.Second || timeout > 24*time.Hour || timeout%time.Second != 0 {
		return 0, codeError(InvalidArgument, "Sandboxes.Connect", "TIMEOUT_OUT_OF_RANGE")
	}
	return timeout, nil
}

func copyResumeOptions(options ResumeOptions) (ResumeOptions, error) {
	if options.Timeout == nil {
		return options, nil
	}
	timeout := *options.Timeout
	if timeout < 30*time.Second || timeout > 24*time.Hour || timeout%time.Second != 0 {
		return ResumeOptions{}, codeError(InvalidArgument, "Sandbox.Resume", "TIMEOUT_OUT_OF_RANGE")
	}
	options.Timeout = &timeout
	return options, nil
}

var createEnvKey = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func copyCreateEnv(opts CreateOptions) (CreateOptions, error) {
	values := make(map[string]string, len(opts.Env))
	for key, value := range opts.Env {
		if !createEnvKey.MatchString(key) || strings.ContainsRune(value, 0) {
			return opts, codeError(InvalidArgument, "Sandboxes.Create", "ENV_INVALID")
		}
		values[key] = value
	}
	if len(values) > 0 {
		opts.Env = values
	}
	opts.Metadata = cloneStringMap(opts.Metadata)
	opts.MountOptions = append([]MountOption(nil), opts.MountOptions...)
	for i := range opts.MountOptions {
		if opts.MountOptions[i].ReadOnly != nil {
			value := *opts.MountOptions[i].ReadOnly
			opts.MountOptions[i].ReadOnly = &value
		}
	}
	return opts, nil
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	copy := make(map[string]string, len(values))
	for key, value := range values {
		copy[key] = value
	}
	return copy
}

var defaultClient struct {
	sync.Mutex
	client *Client
}

// DefaultClient lazily caches the first successfully configured environment-backed client.
// Mode, endpoints and provider bindings then remain fixed; do not change process environment
// to switch tenants. Use NewClient for explicit multi-account configuration.
func DefaultClient() (*Client, error) {
	defaultClient.Lock()
	defer defaultClient.Unlock()
	if defaultClient.client == nil {
		client, err := NewClient()
		if err != nil {
			return nil, err
		}
		defaultClient.client = client
	}
	return defaultClient.client, nil
}
