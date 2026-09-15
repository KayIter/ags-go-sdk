# Architecture

[中文](ARCHITECTURE-zh.md)

The SDK keeps one cohesive public model while separating private Cloud and runtime
implementation by ownership. Applications import only the root `ags` package and,
optionally, the `sandbox` shortcut package.

## Public model

```text
Client -> SandboxManager -> Sandbox
                              |-- Files
                              |-- Commands
                              |-- PTY
                              |-- Code
                              `-- Metrics
```

`Client` binds one Tencent Cloud identity, region, endpoints, and request policy.
Create and Connect return the canonical `*ags.Sandbox`. The `sandbox` package is a
small environment-backed convenience layer over the same Client and Sandbox types;
it does not contain another implementation.

## Dependency direction

```text
ags / sandbox (public facades)
        |
        +--> internal/controlplane --> official Tencent Cloud SDK
        |
        `--> internal/runtime --> internal/dataplane --> internal/gen
                    |
                    `--> internal/model
```

- The root package owns public options, validation, stable errors, and conversion
  between public values and private DTOs.
- `internal/controlplane` owns Cloud clients, signing integration, action mapping,
  lifecycle polling, Metrics requests, instance-token acquisition, and construction
  of a fresh runtime generation.
- `internal/runtime` owns generation validity, request lifetimes, handles, stream
  pumps, bounded queues, output aggregation, and local invalidation.
- `internal/dataplane` owns immutable endpoints and instance access material,
  authentication headers, HTTP/Connect calls, protocol parsing, and start barriers.
- `internal/gen` contains generated protocol bindings. It may be consumed only by
  the data-plane implementation and protocol fixtures.
- `internal/model` contains the smallest wire-independent DTO and error vocabulary
  needed across private layers. It never aliases public or generated types.

Internal packages never import the root `ags` package. Root production code never
imports Connect, generated bindings, or the Tencent Cloud generated SDK.

## Lifecycle ownership

Each connected Sandbox owns exactly one current data-plane generation. Readers,
watches, command handles, PTYs, Code executions, and managed Code contexts belong to
the generation that created them.

- Pause invalidates the current generation before the Cloud mutation is submitted.
- Resume obtains new instance access material and installs a new generation only
  after the data plane is ready.
- Close synchronously invalidates local work and never deletes the remote instance.
- Delete is the explicit remote cleanup operation.
- Caller cancellation stops local observation; it does not prove that an accepted
  remote side effect was undone.

Normal stream close, generation invalidation, and explicit remote process signals are
separate operations. Moving implementation across layers must not merge those meanings.

## Adding a capability

1. Define SDK-owned public inputs and results only when a user-facing contract is needed.
2. Add the smallest private DTO to its owning internal package; use `internal/model`
   only when more than one private layer needs the value.
3. Add semantic data-plane operations instead of exposing clients, endpoints, tokens,
   generated messages, or generic request builders.
4. Put lifecycle and bounded asynchronous behavior in `internal/runtime`, not in a
   public handle wrapper.
5. Add deterministic loopback tests for mappings, cancellation, bounds, errors, and
   side-effect semantics. Real Cloud tests remain explicitly opt-in.
6. Run `make verify`. The repository verifier enforces the dependency direction and
   the approved public-root responsibilities.

Public API changes require prior discussion, an API snapshot review, migration notes,
and a changelog entry. An internal refactor must keep the public API snapshot unchanged.

## Verification

The default verification is offline and includes formatting, tests, vet, race tests,
public consumer compilation, API snapshot checks, generated-code drift checks,
repository architecture checks, and secret scanning. Optional Cloud validation and
its required environment are documented in [test/README.md](test/README.md).

See [CONTRIBUTING.md](CONTRIBUTING.md) for the contribution workflow.
