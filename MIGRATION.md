# Migration from v0.1.5 and earlier

The next pre-1.0 release intentionally converges the SDK on one `Client -> Sandbox` model. It is
source incompatible with v0.1.5. Migrate all packages in one change; the old and new resource
models are not designed to coexist.

## Entry points

| Earlier API | New API | Notes |
| --- | --- | --- |
| `sandbox/core.Create`, `sandbox/code.Create` | `sandbox.Create` or `client.Sandboxes().Create` | Both paths return `*ags.Sandbox`. |
| `sandbox/core.Connect`, `sandbox/code.Connect` | `sandbox.Connect` or `client.Sandboxes().Connect` | Connect may Resume or extend lifetime and verifies data-plane readiness. |
| package-level `List` | `sandbox.List` or `client.Sandboxes().List` | Returns SDK-owned `SandboxPage`. |
| package-level `Kill` | `Sandbox.Delete` or `client.Sandboxes().Delete` | Delete is explicit and idempotent for NotFound. |
| `WithClient(*generated.Client)` | `ags.NewClient(...)` | Generated Cloud clients are no longer public configuration. |
| generated credential interfaces | `ags.CredentialProvider` | One Client binds one logical Cloud identity. |

The default shortcut reads `TENCENTCLOUD_REGION`, `TENCENTCLOUD_SECRET_ID`,
`TENCENTCLOUD_SECRET_KEY`, and optional `TENCENTCLOUD_TOKEN`. Use separate explicit Clients for
multiple identities.

## Sandbox and services

| Earlier API | New API |
| --- | --- |
| `SandboxId` | `ID()` |
| `GetInfo` | `Info(ctx)` |
| `SetTimeoutSeconds` | `Update(ctx, UpdateOptions{Timeout: ...})` |
| `Kill` | `Delete(ctx)` |
| `sb.Files`, `sb.Commands`, `sb.Code` | `sb.Files()`, `sb.Commands()`, `sb.Code()` |
| mutable public connection config | managed internal generation obtained by Create, Connect, or Resume |
| `constant.EnvdPort`, `constant.CodePort` | no public replacement | Management ports are SDK implementation details; use `GetHost(port)` only for Tool-exposed application ports. |
| `constant.AgentSandboxInternalEndpoint` | `WithControlPlaneEndpoint` for tests or approved endpoint overrides | The SDK default remains the public Cloud endpoint. |

`Close()` is now local-only. Always call `Delete` explicitly for a remote instance that your
application owns. Use an independent cleanup context because the operation context may already be
canceled.

## Options and Cloud models

Creation uses `ags.CreateOptions`, `ags.ToolRef`, `ags.MountOption`, and `ags.SandboxAuthMode`.
List, Get, and Metrics return SDK-owned types. Code that accessed generated Cloud models or
protobuf values must switch to these public models.

Generated filesystem and process packages are implementation details under `internal/gen`.
Consumers that imported the former `pb/*` paths must migrate to the corresponding SDK-owned
Files, Commands, PTY, Watch, and process models; generated messages are not a supported public
extension point.

Timeout fields use `time.Duration` pointers. Create and Resume accept 30 seconds through 24 hours
in whole seconds. Update accepts 300 seconds through 24 hours. Values are never clamped. Current
Cloud deployments may enforce a 300-second policy for Create or Resume; a 30-299 second request
is still sent unchanged and any service rejection remains a structured `*ags.Error`.

## Files

- `Read` returns `io.ReadCloser`; close it after complete or partial reads.
- `Write` accepts `io.Reader` and streams multipart content without retaining the whole file.
- Rename is `Move`.
- `Stat`, `Exists`, `MakeDir`, `Remove`, and `Watch` use SDK-owned option and result types.
- `FileInfo` preserves Owner, Group, mode, permissions, and an optional symlink target.

## Commands and PTY

- Foreground output is bounded independently for stdout and stderr and reports truncation.
- `Start` returns a generation-bound `CommandHandle`; its PID is not a reconnectable global
  identity.
- `Commands.Connect` accepts a PID and returns the same handle type after a start/PID barrier.
- `Commands.List(ctx, CommandListOptions{User: ...})` can select `User` or `Root`; omitting the
  optional value preserves the `User` default.
- `CommandHandle.Close` stops local observation. Use `Signal` to request remote termination.
- Earlier PTY option/session values move to `PTYOptions`, `PTYEvent`, and `PTYSession`, following Go's initialism convention.
- PTY returns a bounded event stream. Drain events while waiting and close the session.

## Code contexts

`RunCodeConfig.ContextId` is replaced by `RunCodeOptions.Context`:

```go
managed, err := sb.Code().CreateContext(ctx, ags.CreateCodeContextOptions{})
result, err := sb.Code().Run(ctx, source, ags.RunCodeOptions{Context: managed}, callbacks)
```

A managed `CodeContext` is bound to its Sandbox and generation. Cross-Sandbox use and use after
Pause/Resume are rejected locally before HTTP. To ask the service to validate an existing ID:

```go
external, err := ags.NewExternalCodeContextRef(existingContextID)
result, err := sb.Code().Run(ctx, source, ags.RunCodeOptions{Context: external}, callbacks)
```

An external reference does not prove existence, ownership, or validity after lifecycle changes.
The SDK does not create a new context to imitate recovery.

## Errors

Replace string matching with `errors.As(err, &sdkError)` and branch on `sdkError.Code` and
`sdkError.Reason`. A Create failure after service acceptance may include `InstanceID`. Update
failures may include `Mutation` evidence; an unknown outcome is not permission to retry.

## Validated downstream baseline

The migration was compile-checked against these public consumer revisions without committing
changes to either downstream repository:

| Consumer | Revision | Result and remaining downstream work |
| --- | --- | --- |
| `TencentCloudAgentRuntime/ags-cli` | `faf0164263075bf957722c70d42ad48b2946ec16` | All packages compile after moving command, code, file, and webshell usage to the canonical Cloud-managed `Sandbox`. The CLI's private protobuf PTY path stays CLI-internal. A downstream PR is still required. This revision itself declares Go 1.25, so it cannot truthfully be used as a whole-repository Go 1.22 check. |
| `TencentCloudAgentRuntime/ags-cookbook` | `fa14c68194cd57972d1ea14293359cc0fa8176f4` | The `tutorials/sdk/go`, `examples/hybrid-cookbook`, and `examples/custom-image-go-sdk` modules compile with Go 1.22 after migration. The custom-image example retains the official typed Cloud SDK for its custom creation request, then uses this SDK for managed Connect and data-plane services. A downstream PR is still required. |

The repository's `make verify-consumers` gate compiles public examples and the separate E2E
module with the selected local Go toolchain, and rejects imports of the removed legacy packages.
It does not clone or mutate downstream repositories during normal SDK verification.
