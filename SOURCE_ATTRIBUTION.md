# Source attribution

## Filesystem and process protocols

The following files are derived from the E2B Infrastructure repository at revision
`97d10b529fd506f2ea3ac6a262a8b453fd8cbb14`, under Apache-2.0:

- `proto/tool/filesystem/filesystem.proto`
- `proto/tool/process/process.proto`

Tencent modified the copies in 2026 by adding the
`github.com/TencentCloudAgentRuntime/ags-go-sdk` Go package mapping and a prominent modification
notice. The current hashes and pinned generator versions are machine-readable in
[`contracts/proto.json`](contracts/proto.json).

The files under `internal/gen/` are generated from these modified protocol files by Buf,
`protoc-gen-go`, and `protoc-gen-connect-go`. Generated files must not be edited manually.

## Control-plane contract metadata

[`contracts/control-plane.json`](contracts/control-plane.json) records the source repository,
revision, source-file hashes, effective schema hash, approved SDK action subset, and static typed
transport routes. The full CLI schema is not copied into this repository.

## Verification

`make verify-contracts` checks the public contract registries and source hashes. `make
verify-generate` regenerates protocol code with pinned tools in a temporary worktree and rejects
unexplained drift.
