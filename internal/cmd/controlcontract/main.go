// Command controlcontract synchronizes and verifies the public SDK's selected
// control-plane contract against a pinned ags-cli checkout.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/controlplane"
)

const (
	routingFile       = "contracts/control-plane.json"
	contractDirectory = "contracts/controlplane/ags/v20250920"
	manifestFile      = contractDirectory + "/MANIFEST.json"
	effectiveFile     = contractDirectory + "/effective.json"
	sourceRepository  = "https://github.com/TencentCloudAgentRuntime/ags-cli"
	sdkAPIBrief       = "Agent Sandbox 沙箱管理与调用"
)

type actionRoute struct {
	Name  string `json:"name"`
	Route string `json:"route"`
}

type routingContract struct {
	Service    string        `json:"service"`
	Version    string        `json:"version"`
	Actions    []actionRoute `json:"actions"`
	RawActions []string      `json:"raw_actions"`
}

type manifest struct {
	SchemaVersion            int    `json:"schema_version"`
	SourceRepository         string `json:"source_repository"`
	SourceRevision           string `json:"source_revision"`
	SourcePath               string `json:"source_path"`
	SourceAPIJSONSHA256      string `json:"source_schema_sha256"`
	SourceAPIPatchJSONSHA256 string `json:"source_patch_sha256"`
	SourceEffectiveSHA256    string `json:"source_effective_sha256"`
	SDKEffectiveSHA256       string `json:"sdk_effective_sha256"`
	SDKEffectiveActions      int    `json:"sdk_effective_actions"`
	SDKEffectiveObjects      int    `json:"sdk_effective_objects"`
}

type apiContract struct {
	Actions  map[string]json.RawMessage `json:"actions"`
	Metadata json.RawMessage            `json:"metadata"`
	Objects  map[string]json.RawMessage `json:"objects"`
	Version  string                     `json:"version"`
}

type actionShape struct {
	Input  string `json:"input"`
	Output string `json:"output"`
}

type objectShape struct {
	Members []memberShape `json:"members"`
}

type memberShape struct {
	Name           string `json:"name"`
	Type           string `json:"type"`
	Member         string `json:"member"`
	Required       *bool  `json:"required,omitempty"`
	OutputRequired *bool  `json:"output_required,omitempty"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "controlcontract:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: controlcontract <sync|verify> [flags]")
	}
	switch args[0] {
	case "sync":
		return runSync(args[1:], stdout)
	case "verify":
		return runVerify(args[1:], stdout)
	default:
		return fmt.Errorf("unknown subcommand %q (allowed: sync, verify)", args[0])
	}
}

func runSync(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("sync", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	cliDir := fs.String("cli-dir", "", "path to a pinned ags-cli checkout")
	revision := fs.String("revision", "", "required ags-cli revision")
	root := fs.String("root", ".", "ags-go-sdk repository root")
	check := fs.Bool("check", false, "compare generated files without writing")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || *cliDir == "" || *revision == "" {
		return errors.New("sync requires --cli-dir and --revision and accepts no positional arguments")
	}
	routing, err := readRouting(*root)
	if err != nil {
		return err
	}
	generated, err := generateFromCLI(*cliDir, *revision, routing)
	if err != nil {
		return err
	}
	files := []struct {
		path string
		data []byte
	}{
		{path: filepath.Join(*root, effectiveFile), data: generated.effective},
		{path: filepath.Join(*root, manifestFile), data: generated.manifest},
	}
	for _, file := range files {
		if *check {
			current, readErr := os.ReadFile(file.path)
			if readErr != nil {
				return fmt.Errorf("read generated file %s: %w", file.path, readErr)
			}
			if !bytes.Equal(current, file.data) {
				return fmt.Errorf("generated control contract is stale: %s", file.path)
			}
			continue
		}
		if err := writeAtomic(file.path, file.data); err != nil {
			return err
		}
	}
	verb := "synchronized"
	if *check {
		verb = "current"
	}
	fmt.Fprintf(stdout, "control contract %s: %d actions, %d objects, revision %s\n", verb, generated.actions, generated.objects, *revision)
	return nil
}

func runVerify(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	root := fs.String("root", ".", "ags-go-sdk repository root")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("verify accepts no positional arguments")
	}
	routing, err := readRouting(*root)
	if err != nil {
		return err
	}
	manifestData, err := os.ReadFile(filepath.Join(*root, manifestFile))
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	var recorded manifest
	if err := json.Unmarshal(manifestData, &recorded); err != nil {
		return fmt.Errorf("decode manifest: %w", err)
	}
	effective, err := os.ReadFile(filepath.Join(*root, effectiveFile))
	if err != nil {
		return fmt.Errorf("read effective snapshot: %w", err)
	}
	if got := digest(effective); got != recorded.SDKEffectiveSHA256 {
		return fmt.Errorf("effective snapshot SHA-256 is %s, expected %s", got, recorded.SDKEffectiveSHA256)
	}
	for name, value := range map[string]string{
		"source api.json":       recorded.SourceAPIJSONSHA256,
		"source api.patch.json": recorded.SourceAPIPatchJSONSHA256,
		"source effective API":  recorded.SourceEffectiveSHA256,
	} {
		if !validDigest(value) {
			return fmt.Errorf("%s SHA-256 is invalid", name)
		}
	}
	wantSourcePath := filepath.ToSlash(filepath.Join("api", routing.Service, "v"+routing.Version))
	if recorded.SchemaVersion != 1 || recorded.SourceRepository != sourceRepository || recorded.SourceRevision == "" || recorded.SourcePath != wantSourcePath {
		return errors.New("manifest source metadata is incomplete")
	}
	var api apiContract
	if err := json.Unmarshal(effective, &api); err != nil {
		return fmt.Errorf("decode effective snapshot: %w", err)
	}
	closure, err := validateContract(api, routing)
	if err != nil {
		return err
	}
	if len(api.Actions) != recorded.SDKEffectiveActions || len(closure) != recorded.SDKEffectiveObjects {
		return fmt.Errorf("manifest counts are %d actions/%d objects, snapshot has %d/%d", recorded.SDKEffectiveActions, recorded.SDKEffectiveObjects, len(api.Actions), len(closure))
	}
	if err := compareImplementationRoutes(routing); err != nil {
		return err
	}
	typed, raw := routeCounts(routing)
	fmt.Fprintf(stdout, "control contract verified: %d actions, %d objects, %d typed, %d raw\n", len(api.Actions), len(closure), typed, raw)
	return nil
}

type generatedContract struct {
	effective, manifest []byte
	actions, objects    int
}

func generateFromCLI(cliDir, revision string, routing routingContract) (generatedContract, error) {
	head, err := commandOutput(cliDir, "git", "rev-parse", "HEAD")
	if err != nil {
		return generatedContract{}, fmt.Errorf("read ags-cli revision: %w", err)
	}
	if strings.TrimSpace(string(head)) != revision {
		return generatedContract{}, fmt.Errorf("ags-cli revision is %s, expected %s", strings.TrimSpace(string(head)), revision)
	}
	relativeAPIPath := filepath.Join("api", routing.Service, "v"+routing.Version)
	status, err := commandOutput(cliDir, "git", "status", "--porcelain")
	if err != nil {
		return generatedContract{}, fmt.Errorf("inspect ags-cli contract worktree: %w", err)
	}
	if strings.TrimSpace(string(status)) != "" {
		return generatedContract{}, fmt.Errorf("ags-cli worktree is not clean: %s", strings.TrimSpace(string(status)))
	}
	apiPath := filepath.Join(cliDir, relativeAPIPath)
	apiData, err := os.ReadFile(filepath.Join(apiPath, "api.json"))
	if err != nil {
		return generatedContract{}, fmt.Errorf("read source api.json: %w", err)
	}
	patchData, err := os.ReadFile(filepath.Join(apiPath, "api.patch.json"))
	if err != nil {
		return generatedContract{}, fmt.Errorf("read source api.patch.json: %w", err)
	}
	rendered, err := commandOutput(cliDir, "go", "run", "./cmd/internal/apipatch", "render", "--api", relativeAPIPath)
	if err != nil {
		return generatedContract{}, fmt.Errorf("render CLI effective API: %w", err)
	}
	compact, err := compactJSON(rendered)
	if err != nil {
		return generatedContract{}, fmt.Errorf("compact CLI effective API: %w", err)
	}
	var full apiContract
	if err := json.Unmarshal(rendered, &full); err != nil {
		return generatedContract{}, fmt.Errorf("decode CLI effective API: %w", err)
	}
	reduced, err := pruneContract(full, routing)
	if err != nil {
		return generatedContract{}, err
	}
	effective, err := marshalFile(reduced)
	if err != nil {
		return generatedContract{}, err
	}
	recorded := manifest{
		SchemaVersion: 1, SourceRepository: sourceRepository, SourceRevision: revision,
		SourcePath:          filepath.ToSlash(relativeAPIPath),
		SourceAPIJSONSHA256: digest(apiData), SourceAPIPatchJSONSHA256: digest(patchData),
		SourceEffectiveSHA256: digest(compact), SDKEffectiveSHA256: digest(effective),
		SDKEffectiveActions: len(reduced.Actions), SDKEffectiveObjects: len(reduced.Objects),
	}
	manifestData, err := marshalFile(recorded)
	if err != nil {
		return generatedContract{}, err
	}
	return generatedContract{effective: effective, manifest: manifestData, actions: len(reduced.Actions), objects: len(reduced.Objects)}, nil
}

func readRouting(root string) (routingContract, error) {
	data, err := os.ReadFile(filepath.Join(root, routingFile))
	if err != nil {
		return routingContract{}, fmt.Errorf("read action routing: %w", err)
	}
	var routing routingContract
	if err := json.Unmarshal(data, &routing); err != nil {
		return routingContract{}, fmt.Errorf("decode action routing: %w", err)
	}
	if routing.Service == "" || routing.Version == "" || len(routing.Actions) == 0 {
		return routingContract{}, errors.New("action routing metadata is incomplete")
	}
	seen, raw := map[string]bool{}, map[string]bool{}
	for _, name := range routing.RawActions {
		if raw[name] || name == "" {
			return routingContract{}, fmt.Errorf("duplicate or empty raw action %q", name)
		}
		raw[name] = true
	}
	previous := ""
	for _, action := range routing.Actions {
		if action.Name == "" || seen[action.Name] {
			return routingContract{}, fmt.Errorf("duplicate or empty action %q", action.Name)
		}
		if previous != "" && action.Name < previous {
			return routingContract{}, errors.New("action routing must be sorted by action name")
		}
		if action.Route != string(controlplane.OfficialTyped) && action.Route != string(controlplane.CommonRaw) {
			return routingContract{}, fmt.Errorf("action %s has unsupported route %q", action.Name, action.Route)
		}
		if raw[action.Name] != (action.Route == string(controlplane.CommonRaw)) {
			return routingContract{}, fmt.Errorf("action %s route and raw_actions disagree", action.Name)
		}
		seen[action.Name], previous = true, action.Name
	}
	for name := range raw {
		if !seen[name] {
			return routingContract{}, fmt.Errorf("raw action %s is not in the action registry", name)
		}
	}
	return routing, nil
}

func pruneContract(full apiContract, routing routingContract) (apiContract, error) {
	metadata, err := sdkMetadata(full.Metadata)
	if err != nil {
		return apiContract{}, err
	}
	reduced := apiContract{Actions: map[string]json.RawMessage{}, Metadata: metadata, Objects: map[string]json.RawMessage{}, Version: full.Version}
	for _, route := range routing.Actions {
		raw, ok := full.Actions[route.Name]
		if !ok {
			return apiContract{}, fmt.Errorf("allowlisted action %s is absent from CLI effective API", route.Name)
		}
		reduced.Actions[route.Name] = raw
	}
	needed, err := reachableObjects(reduced.Actions, full.Objects)
	if err != nil {
		return apiContract{}, err
	}
	for name := range needed {
		reduced.Objects[name] = full.Objects[name]
	}
	if _, err := validateContract(reduced, routing); err != nil {
		return apiContract{}, err
	}
	return reduced, nil
}

// sdkMetadata keeps source identity fields while replacing the full-service brief, which also
// describes API-key Actions outside this SDK's reviewed allowlist.
func sdkMetadata(raw json.RawMessage) (json.RawMessage, error) {
	var metadata map[string]any
	if err := json.Unmarshal(raw, &metadata); err != nil {
		return nil, fmt.Errorf("decode CLI metadata: %w", err)
	}
	if _, exists := metadata["api_brief"]; exists {
		metadata["api_brief"] = sdkAPIBrief
	}
	data, err := json.Marshal(metadata)
	if err != nil {
		return nil, fmt.Errorf("encode SDK metadata: %w", err)
	}
	return data, nil
}

func validateContract(api apiContract, routing routingContract) (map[string]bool, error) {
	if len(api.Actions) != len(routing.Actions) {
		return nil, fmt.Errorf("effective snapshot has %d actions, routing declares %d", len(api.Actions), len(routing.Actions))
	}
	for _, route := range routing.Actions {
		if _, ok := api.Actions[route.Name]; !ok {
			return nil, fmt.Errorf("routed action %s is absent from effective snapshot", route.Name)
		}
	}
	closure, err := reachableObjects(api.Actions, api.Objects)
	if err != nil {
		return nil, err
	}
	if len(closure) != len(api.Objects) {
		extra := make([]string, 0)
		for name := range api.Objects {
			if !closure[name] {
				extra = append(extra, name)
			}
		}
		sort.Strings(extra)
		return nil, fmt.Errorf("effective snapshot contains objects outside routed action closure: %s", strings.Join(extra, ", "))
	}
	for name, raw := range api.Objects {
		var object objectShape
		if err := json.Unmarshal(raw, &object); err != nil {
			return nil, fmt.Errorf("decode object %s: %w", name, err)
		}
		for index, member := range object.Members {
			if member.Name == "" || member.Type == "" || member.Member == "" {
				return nil, fmt.Errorf("object %s member %d lacks name, type, or member", name, index)
			}
		}
	}
	return closure, nil
}

func reachableObjects(actions, objects map[string]json.RawMessage) (map[string]bool, error) {
	needed := map[string]bool{}
	pending := make([]string, 0)
	for name, raw := range actions {
		var action actionShape
		if err := json.Unmarshal(raw, &action); err != nil {
			return nil, fmt.Errorf("decode action %s: %w", name, err)
		}
		if action.Input == "" || action.Output == "" {
			return nil, fmt.Errorf("action %s lacks input or output shape", name)
		}
		for _, shape := range []string{action.Input, action.Output} {
			if _, ok := objects[shape]; !ok {
				return nil, fmt.Errorf("action %s references missing object %s", name, shape)
			}
			if !needed[shape] {
				needed[shape] = true
				pending = append(pending, shape)
			}
		}
	}
	for len(pending) > 0 {
		name := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		var object objectShape
		if err := json.Unmarshal(objects[name], &object); err != nil {
			return nil, fmt.Errorf("decode object %s: %w", name, err)
		}
		for _, member := range object.Members {
			if _, ok := objects[member.Member]; ok && !needed[member.Member] {
				needed[member.Member] = true
				pending = append(pending, member.Member)
			}
		}
	}
	return needed, nil
}

func compareImplementationRoutes(routing routingContract) error {
	want := make(map[string]controlplane.ActionRoute, len(routing.Actions))
	for _, action := range routing.Actions {
		want[action.Name] = controlplane.ActionRoute(action.Route)
	}
	got := controlplane.ActionRoutes()
	if len(got) != len(want) {
		return fmt.Errorf("implementation has %d action routes, contract declares %d", len(got), len(want))
	}
	for name, route := range want {
		if got[name] != route {
			return fmt.Errorf("implementation route for %s is %q, expected %q", name, got[name], route)
		}
	}
	return nil
}

func routeCounts(routing routingContract) (typed, raw int) {
	for _, action := range routing.Actions {
		if action.Route == string(controlplane.OfficialTyped) {
			typed++
		} else {
			raw++
		}
	}
	return typed, raw
}

func commandOutput(directory, name string, args ...string) ([]byte, error) {
	command := exec.Command(name, args...)
	command.Dir = directory
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		return nil, fmt.Errorf("%s: %w: %s", name, err, strings.TrimSpace(stderr.String()))
	}
	return output, nil
}

func compactJSON(data []byte) ([]byte, error) {
	var output bytes.Buffer
	if err := json.Compact(&output, data); err != nil {
		return nil, err
	}
	output.WriteByte('\n')
	return output.Bytes(), nil
}

func marshalFile(value any) ([]byte, error) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode generated contract: %w", err)
	}
	return append(data, '\n'), nil
}

func writeAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create contract directory: %w", err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".controlcontract-*")
	if err != nil {
		return fmt.Errorf("create temporary contract: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if _, err = temporary.Write(data); err != nil {
		temporary.Close()
		return fmt.Errorf("write temporary contract: %w", err)
	}
	if err = temporary.Chmod(0o644); err != nil {
		temporary.Close()
		return fmt.Errorf("set contract permissions: %w", err)
	}
	if err = temporary.Close(); err != nil {
		return fmt.Errorf("close temporary contract: %w", err)
	}
	if err = os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("replace generated contract: %w", err)
	}
	return nil
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func validDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
