# Real Cloud E2E

This nested module is an explicit, serial test boundary. Root `go test ./...` does not enter it
and never accesses Cloud services.

Set all variables before running:

```bash
export AGS_E2E=1
export TENCENTCLOUD_SECRET_ID=...
export TENCENTCLOUD_SECRET_KEY=...
export AGS_E2E_REGION=ap-guangzhou
export AGS_E2E_TOOL_ID=...       # custom envd-capable Tool used for Files, Commands, PTY, and Metrics
export AGS_E2E_CODE_TOOL_ID=...  # code-interpreter Tool used for Code and context lifecycle
make test-e2e
```

`TENCENTCLOUD_TOKEN` is optional for temporary Cloud credentials. The tests never accept an API
key or a Sandbox instance token.

The suite is serial (`-p=1`), creates at most four sandboxes, and registers independent cleanup
for each accepted creation. Cleanup sends Delete and polls until the control plane reports
`STOPPED` or `NOT_FOUND`. A failed cleanup fails the test. Do not run it against a shared Tool
unless you are authorized to create and delete test instances.

The explicit-client journey covers Files, streamed upload/read, Watch, Commands Run/List/Connect,
PTY, all ten Metrics with an explicit Start/End window, Update, and Connect. A dedicated
code-interpreter journey covers Code, managed/external contexts, Pause/Resume, and generation
invalidation. Keeping the Tool IDs separate avoids claiming that an arbitrary custom image
provides the Code service on port 49999. A separate default-shortcut journey proves the
environment-backed entrance. The 30-second creation boundary has its own resource and cleanup
path.
