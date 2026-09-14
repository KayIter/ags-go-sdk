#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
temp_root="$(mktemp -d)"
trap 'rm -rf "$temp_root"' EXIT
export BUF_CACHE_DIR="$temp_root/buf-cache"
go_cmd="${GO:-go}"
export PATH="$("$go_cmd" env GOPATH)/bin:$PATH"

command -v buf >/dev/null || { echo "buf is required; run make tools" >&2; exit 1; }
command -v protoc-gen-go >/dev/null || { echo "protoc-gen-go is required; run make tools" >&2; exit 1; }
command -v protoc-gen-connect-go >/dev/null || { echo "protoc-gen-connect-go is required; run make tools" >&2; exit 1; }

test "$(buf --version)" = "1.47.2" || { echo "buf v1.47.2 is required" >&2; exit 1; }
test "$(protoc-gen-go --version)" = "protoc-gen-go v1.36.11" || { echo "protoc-gen-go v1.36.11 is required" >&2; exit 1; }
protoc-gen-connect-go --version | grep -q '1.18.1' || { echo "protoc-gen-connect-go v1.18.1 is required" >&2; exit 1; }

cp -R "$repo_root/proto" "$temp_root/proto"
(
  cd "$temp_root/proto"
  PATH="$("$go_cmd" env GOPATH)/bin:$PATH" buf generate
)
diff -ru "$repo_root/pb" "$temp_root/pb"
