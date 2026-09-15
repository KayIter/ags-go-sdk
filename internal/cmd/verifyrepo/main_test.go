package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrivateWireBoundaryRejectsRootWireImport(t *testing.T) {
	for name, imported := range map[string]string{
		"connect":   "connectrpc.com/connect",
		"generated": "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/gen/process",
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			writeFixture(t, root, "wire.go", "package ags\nimport _ \""+imported+"\"\n")
			failures := privateWireBoundaryFailures(root)
			if !containsFailure(failures, "imports private wire package") {
				t.Fatalf("failures = %v", failures)
			}
		})
	}
}

func TestPrivateWireBoundaryAllowsDataPlaneGeneratedImport(t *testing.T) {
	root := t.TempDir()
	writeFixture(t, root, "internal/dataplane/files.go", `package dataplane
import _ "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/gen/filesystem"
`)
	if failures := privateWireBoundaryFailures(root); len(failures) != 0 {
		t.Fatalf("failures = %v", failures)
	}
}

func TestPrivateWireBoundaryRejectsGeneratedImportOutsideDataPlane(t *testing.T) {
	root := t.TempDir()
	writeFixture(t, root, "internal/other/wire.go", `package other
import _ "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/gen/process"
`)
	failures := privateWireBoundaryFailures(root)
	if !containsFailure(failures, "outside internal/dataplane") {
		t.Fatalf("failures = %v", failures)
	}
}

func TestPrivateWireBoundaryRejectsClientEscapeHatches(t *testing.T) {
	root := t.TempDir()
	writeFixture(t, root, "internal/dataplane/client.go", `package dataplane
type Client struct{}
func (*Client) BaseURL() string { return "" }
func Request[T any](*Client, *T, string) {}
`)
	failures := privateWireBoundaryFailures(root)
	if !containsFailure(failures, "Client.BaseURL") || !containsFailure(failures, "escape hatch Request") {
		t.Fatalf("failures = %v", failures)
	}
}

func TestPrivateWireBoundaryRejectsRootConnectionMaterial(t *testing.T) {
	root := t.TempDir()
	writeFixture(t, root, "runtime.go", `package ags
import wire "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/dataplane"
var _ = wire.New("https://49983-instance.region.tencentags.com", "token", nil)
`)
	failures := privateWireBoundaryFailures(root)
	if !containsFailure(failures, "runtime endpoint outside internal/dataplane") || !containsFailure(failures, "constructs a raw data-plane client") {
		t.Fatalf("failures = %v", failures)
	}
}

func writeFixture(t *testing.T, root, name, contents string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}

func containsFailure(failures []string, fragment string) bool {
	for _, failure := range failures {
		if strings.Contains(failure, fragment) {
			return true
		}
	}
	return false
}
