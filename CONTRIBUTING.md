# Contributing

[中文](CONTRIBUTING-zh.md)

Thank you for contributing to the Agent Sandbox Go SDK. Bug reports, focused feature proposals,
tests, documentation, and code changes are welcome.

## Before opening a change

- Search existing issues and pull requests.
- Keep one change focused on one problem.
- Discuss public API changes before implementation when possible.
- Do not include credentials, Sandbox instance access material, real resource identifiers,
  private endpoints, customer data, or Cloud test output.
- Report suspected vulnerabilities through the process in [SECURITY.md](SECURITY.md).

## Development environment

- Go 1.22 or later. CI verifies with Go 1.22 and `GOTOOLCHAIN=local`.
- Run `make tools` with Go 1.25.4 or newer to install the pinned generators and Gitleaks. These
  build tools have a higher toolchain requirement than the SDK. Run SDK gates with Go 1.22.12
  and `GOTOOLCHAIN=local`.

Clone your fork and create a branch from `main`:

```bash
git clone https://github.com/YOUR_ACCOUNT/ags-go-sdk.git
cd ags-go-sdk
git remote add upstream https://github.com/TencentCloudAgentRuntime/ags-go-sdk.git
git switch -c feature/short-description upstream/main
```

## Make a change

- Keep public signatures in SDK-owned types. Generated Cloud, protobuf, and Connect types belong
  behind internal adapters.
- Add GoDoc to every exported symbol.
- Add deterministic offline tests for new behavior, cancellation, bounds, and errors.
- Copy caller-owned maps, slices, and pointed-to values before asynchronous use.
- Do not add automatic retries for mutations, commands, uploads, or streams.
- Update `MIGRATION.md` and `CHANGELOG.md` for public or behavioral changes.
- Keep English and Chinese documentation aligned.

Run the complete offline gate:

```bash
make verify
```

Default tests must not contact Tencent Cloud.

## Package boundaries

Keep the user-facing API cohesive and the wire implementation private:

- The root `ags` package owns `Client`, `Sandbox`, service facades, options, results, and stable
  errors. Adding a feature does not by itself justify another public package.
- The public `sandbox` package contains only environment-backed convenience functions and aliases;
  it must return the canonical root `*ags.Sandbox`.
- `internal/cloudapi` owns Cloud request routing and generated Cloud-model adaptation.
- `internal/dataplane` owns immutable runtime endpoints, instance access material, authentication
  headers, and low-level HTTP/Connect clients.
- `internal/gen` contains generated filesystem and process bindings. Root adapters translate their
  messages into SDK-owned public models.

Do not move Files, Commands, Code, PTY, or Metrics into separate public packages merely to reduce
file size. Split private implementation behind `internal/` boundaries while preserving the simple
`Client -> Sandbox` user model.

## Protocol changes

Protocol definitions are copied source with recorded provenance. Before editing them:

1. Confirm the upstream revision and license.
2. Preserve the modification notice and module package mapping.
3. Update `contracts/proto.json` hashes.
4. Run `make generate` and review the generated diff.
5. Run `make verify-generate` and `make verify`.

Never edit files under `internal/gen/` by hand.

## Optional real Cloud tests

Cloud tests are opt-in and are never part of `go test ./...`. Use a dedicated test account and
Tool. The minimum environment is:

```bash
export TENCENTCLOUD_SECRET_ID=your-test-secret-id
export TENCENTCLOUD_SECRET_KEY=your-test-secret-key
export AGS_E2E_REGION=ap-guangzhou
export AGS_E2E_TOOL_ID=your-test-tool-id
export AGS_E2E_CODE_TOOL_ID=your-code-interpreter-tool-id
export AGS_E2E=1
```

Set `TENCENTCLOUD_TOKEN` only for temporary Cloud credentials. Metrics uses the same credentials,
region, and instance as the other journey tests.

Run Cloud tests serially:

```bash
make test-e2e
```

Each test must record only its in-memory resource ownership, delete every instance it created
with an independent cleanup context, and confirm the terminal state. Do not run Cloud tests
against shared production resources or modify a shared Tool.

## Pull request

Use a Conventional Commit subject such as `feat(sandbox): add bounded watch stream`. In the pull
request, describe the user-visible behavior, migration impact, tests run, protocol or dependency
changes, and any remaining limitation. Do not publish a tag or module version from a pull request.

Contributions are licensed under the repository's [Apache-2.0 license](LICENSE).
