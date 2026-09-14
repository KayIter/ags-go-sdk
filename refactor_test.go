package ags

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRefactorCreateReplayDoesNotDelete(t *testing.T) {
	control := &createCleanupControl{metricControl: metricControl{info: SandboxInfo{ID: "existing", State: Running}}}
	client, err := NewClient(WithRegion("test"), withControlPlane(control))
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Sandboxes().Create(context.Background(), CreateOptions{Tool: ToolRef{Name: "test"}, ClientToken: "same-operation"})
	if err == nil || control.deleted {
		t.Fatalf("connection failure must not delete replayed instance: err=%v deleted=%v", err, control.deleted)
	}
}

type rotatingCredentials struct{ calls atomic.Int32 }

func (p *rotatingCredentials) Retrieve(ctx context.Context) (CloudCredential, error) {
	return CloudCredential{SecretID: fmt.Sprintf("id-%d", p.calls.Add(1)), SecretKey: "synthetic"}, ctx.Err()
}

type rotatingTemporaryCredentials struct{ calls atomic.Int32 }

func (p *rotatingTemporaryCredentials) Retrieve(ctx context.Context) (CloudCredential, error) {
	value, err := p.RetrieveTemporary(ctx)
	return CloudCredential{SecretID: value.SecretID, SecretKey: value.SecretKey}, err
}

func (p *rotatingTemporaryCredentials) RetrieveTemporary(ctx context.Context) (TemporaryCloudCredential, error) {
	call := p.calls.Add(1)
	return TemporaryCloudCredential{SecretID: "temporary-id", SecretKey: "synthetic", Token: fmt.Sprintf("token-%d", call)}, ctx.Err()
}

func TestRefactorProviderResolvedPerRequest(t *testing.T) {
	var auth []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = append(auth, r.Header.Get("Authorization"))
		_, _ = io.WriteString(w, `{"Response":{"InstanceSet":[{"InstanceId":"sb","Status":"RUNNING"}]}}`)
	}))
	defer server.Close()
	provider := &rotatingCredentials{}
	client, err := NewClient(WithRegion("test"), WithCredentialProvider(provider), WithControlPlaneEndpoint(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err = client.Sandboxes().Get(context.Background(), "sb"); err != nil {
			t.Fatal(err)
		}
	}
	if len(auth) != 2 || !strings.Contains(auth[0], "Credential=id-1/") || !strings.Contains(auth[1], "Credential=id-2/") {
		t.Fatalf("stale credentials: %v", auth)
	}
}

func TestTemporaryProviderResolvedPerRequest(t *testing.T) {
	var tokens []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokens = append(tokens, r.Header.Get("X-Tc-Token"))
		_, _ = io.WriteString(w, `{"Response":{"InstanceSet":[{"InstanceId":"sb","Status":"RUNNING"}]}}`)
	}))
	defer server.Close()
	provider := &rotatingTemporaryCredentials{}
	client, err := NewClient(WithRegion("test"), WithCredentialProvider(provider), WithControlPlaneEndpoint(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err = client.Sandboxes().Get(context.Background(), "sb"); err != nil {
			t.Fatal(err)
		}
	}
	if len(tokens) != 2 || tokens[0] != "token-1" || tokens[1] != "token-2" {
		t.Fatalf("stale temporary credentials: %q", tokens)
	}
}

func TestEnvironmentCredentialsReadOptionalTokenPerRequest(t *testing.T) {
	t.Setenv("TENCENTCLOUD_SECRET_ID", "environment-id")
	t.Setenv("TENCENTCLOUD_SECRET_KEY", "environment-key")
	t.Setenv("TENCENTCLOUD_TOKEN", "token-1")
	first, err := (EnvironmentCredentials{}).RetrieveTemporary(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("TENCENTCLOUD_TOKEN", "token-2")
	second, err := (EnvironmentCredentials{}).RetrieveTemporary(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first.Token != "token-1" || second.Token != "token-2" {
		t.Fatalf("environment token was cached: first=%q second=%q", first.Token, second.Token)
	}
}

func TestRefactorInjectedHTTPHonorsRequestBudget(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
		case <-time.After(time.Second):
		}
	}))
	defer server.Close()
	client, err := NewClient(WithRegion("test"), WithCredential(CloudCredential{"id", "key"}), WithControlPlaneEndpoint(server.URL), WithHTTPClient(server.Client()), WithRequestTimeout(30*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Sandboxes().Get(context.Background(), "sb")
	var failure *Error
	if !errors.As(err, &failure) || failure.Code != DeadlineExceeded {
		t.Fatalf("deadline not preserved: %v", err)
	}
}

func TestRefactorCloseIsLocalAndCannotRevive(t *testing.T) {
	control := &createCleanupControl{metricControl: metricControl{info: SandboxInfo{ID: "sb", State: Running}}}
	client, _ := NewClient(WithRegion("test"), withControlPlane(control))
	plane := newDataPlane("http://127.0.0.1:1", "synthetic", http.DefaultClient)
	sb := newSandbox(client, "sb", plane)
	if err := sb.Close(); err != nil {
		t.Fatal(err)
	}
	if err := sb.Close(); err != nil {
		t.Fatal(err)
	}
	if control.deleted {
		t.Fatal("local close deleted remote")
	}
	if sb.ID() != "sb" {
		t.Fatal("identity changed")
	}
	_, err := sb.Resume(context.Background(), ResumeOptions{})
	var failure *Error
	if !errors.As(err, &failure) || failure.Reason != "SANDBOX_CLOSED" {
		t.Fatalf("closed sandbox revived: %v", err)
	}
	if _, err = sb.Files().Read(context.Background(), "/tmp/x", ReadOptions{}); !errors.As(err, &failure) || failure.Reason != "SANDBOX_CLOSED" {
		t.Fatalf("closed sandbox accepted read: %v", err)
	}
}

func TestRefactorCreateUsesCallerDeadline(t *testing.T) {
	control := metricControl{info: SandboxInfo{ID: "pending", State: Creating}}
	client, _ := NewClient(WithRegion("test"), withControlPlane(control))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := client.Sandboxes().Create(ctx, CreateOptions{Tool: ToolRef{Name: "tool"}})
	var failure *Error
	if !errors.As(err, &failure) || failure.Code != DeadlineExceeded || failure.InstanceID != "pending" || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lost budget or recovery identity: %v", err)
	}
}

func TestRefactorCredentialFormatting(t *testing.T) {
	credential := CloudCredential{SecretID: "synthetic-id", SecretKey: "synthetic-secret"}
	temporary := TemporaryCloudCredential{SecretID: "temporary-id", SecretKey: "temporary-secret", Token: "temporary-token"}
	for _, format := range []string{"%v", "%+v", "%#v"} {
		if strings.Contains(fmt.Sprintf(format, credential), "synthetic-secret") {
			t.Errorf("credential leaks through %s", format)
		}
		if strings.Contains(fmt.Sprintf(format, temporary), "temporary-secret") || strings.Contains(fmt.Sprintf(format, temporary), "temporary-token") {
			t.Errorf("temporary credential leaks through %s", format)
		}
	}
	encoded, err := json.Marshal(temporary)
	if err != nil || strings.Contains(string(encoded), "temporary-") {
		t.Fatalf("temporary credential JSON leaked: %s err=%v", encoded, err)
	}
}

func TestRefactorStaticCredentialsAreIsolatedFromEnvironment(t *testing.T) {
	t.Setenv("TENCENTCLOUD_SECRET_ID", "env-id")
	t.Setenv("TENCENTCLOUD_SECRET_KEY", "synthetic")
	auth := make(chan string, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth <- r.Header.Get("Authorization")
		_, _ = io.WriteString(w, `{"Response":{"InstanceSet":[{"InstanceId":"sb","Status":"RUNNING"}]}}`)
	}))
	defer server.Close()
	environment, err := NewClient(WithRegion("test"), WithControlPlaneEndpoint(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	static, err := NewClient(WithRegion("test"), WithControlPlaneEndpoint(server.URL), WithCredential(CloudCredential{"static-id", "synthetic"}))
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("TENCENTCLOUD_SECRET_ID", "rotated-env-id")
	if _, err = static.Sandboxes().Get(context.Background(), "sb"); err != nil {
		t.Fatal(err)
	}
	if _, err = environment.Sandboxes().Get(context.Background(), "sb"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(<-auth, "Credential=static-id/") || !strings.Contains(<-auth, "Credential=rotated-env-id/") {
		t.Fatal("static override or request-time environment lookup was not isolated")
	}
}
