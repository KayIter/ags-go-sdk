#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
go_cmd="${GO:-go}"

if rg -n 'github\.com/TencentCloudAgentRuntime/ags-go-sdk/(connection|constant|sandbox/(core|code)|tool)(/|"|$)' \
  --glob '*.go' "$repo_root"; then
  echo "legacy SDK imports remain in consumer-facing Go sources" >&2
  exit 1
fi

(
  cd "$repo_root"
  GOTOOLCHAIN=local "$go_cmd" test -run '^$' ./examples/...
)

# The E2E module is also an external-module compile fixture. Compilation is offline;
# real Cloud execution remains an explicit test-e2e action.
(
  cd "$repo_root/test"
  GOTOOLCHAIN=local "$go_cmd" test -run '^$' ./...
)
