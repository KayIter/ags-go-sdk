#!/usr/bin/env bash
set -euo pipefail

test "${AGS_E2E:-}" = "1" || { echo "set AGS_E2E=1 to run real Cloud tests" >&2; exit 1; }
test -n "${TENCENTCLOUD_SECRET_ID:-}" || { echo "TENCENTCLOUD_SECRET_ID is required" >&2; exit 1; }
test -n "${TENCENTCLOUD_SECRET_KEY:-}" || { echo "TENCENTCLOUD_SECRET_KEY is required" >&2; exit 1; }
test -n "${AGS_E2E_REGION:-}" || { echo "AGS_E2E_REGION is required" >&2; exit 1; }
test -n "${AGS_E2E_TOOL_ID:-}" || { echo "AGS_E2E_TOOL_ID is required" >&2; exit 1; }
test -n "${AGS_E2E_CODE_TOOL_ID:-}" || { echo "AGS_E2E_CODE_TOOL_ID is required" >&2; exit 1; }

export TENCENTCLOUD_REGION="${AGS_E2E_REGION}"

cd "$(dirname "${BASH_SOURCE[0]}")/../test"
GOTOOLCHAIN=local "${GO:-go}" test -count=1 -p=1 ./...
