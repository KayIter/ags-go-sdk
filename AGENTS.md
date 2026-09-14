# Repository guidance

This repository is the public Go SDK for Tencent Cloud Agent Sandbox.

## Scope

- Keep the module path `github.com/TencentCloudAgentRuntime/ags-go-sdk`.
- The public product model is `Client -> Sandbox`; the `sandbox` package contains only
  environment-backed shortcuts that return the same `*ags.Sandbox` type.
- Public authentication uses Tencent Cloud SecretID, SecretKey, and optional temporary
  credential Token. Sandbox instance access material stays internal to a data-plane generation.
- Prefer the official Tencent Cloud Go SDK for Cloud actions. An internal generic transport may
  cover only an action listed in `contracts/control-plane.json`; never expose arbitrary action
  calls.
- Public signatures must not expose Tencent Cloud generated models, protobuf messages, Connect
  types, endpoints, headers, or secret-bearing connection objects.

## Compatibility

The SDK is pre-1.0. Public changes still require an API diff, a `MIGRATION.md` entry, tests, and
an `Unreleased` changelog entry. Do not retain duplicate deprecated implementations solely to
hide a breaking change.

## Implementation rules

- Support Go 1.22 with `GOTOOLCHAIN=local`.
- Every remote operation accepts `context.Context`; cancellation never proves that a remote side
  effect was undone.
- Do not automatically retry mutations, command execution, uploads, or streams.
- Copy caller-owned maps, slices, and pointed-to option values before asynchronous use.
- Keep readers, event queues, callbacks, command output, Code events, and aggregation bounded.
- Bind data-plane requests and handles to one Sandbox generation. Pause, Resume, and Close must
  not revive an old generation.
- Use `*ags.Error` for stable public error categories. Formatted errors must not include
  credentials, authorization headers, instance access material, response bodies, or causes.
- Use `apply_patch` for source edits. Preserve unrelated contributor changes.

## Verification

Run `make verify` before proposing a change. Default verification must remain offline. Tests that
need loopback servers are unit tests; tests that contact Tencent Cloud must require explicit
environment opt-in and independently clean up every created resource.

When proto files change, update `contracts/proto.json`, regenerate with pinned tools, and prove
that the generation drift check fails on a temporary modification and passes after restoration.
Do not edit generated files by hand.

## Documentation and security

Keep English and Chinese README and contribution guides aligned. Examples must compile, use
placeholders, call `Close`, and show explicit remote cleanup. Never commit credentials, tokens,
resource IDs from real runs, private endpoints, local filesystem paths, progress ledgers, or
approval records.
