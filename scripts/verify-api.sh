#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
snapshot="$repo_root/contracts/public-api.txt"
actual="$(mktemp)"
trap 'rm -f "$actual"' EXIT
go_cmd="${GO:-go}"

(
  cd "$repo_root"
  GOTOOLCHAIN=local "$go_cmd" doc -all .
  GOTOOLCHAIN=local "$go_cmd" doc -all ./sandbox
) >"$actual"

diff -u <(perl -0777 -pe 's/\s+\z/\n/' "$snapshot") <(perl -0777 -pe 's/\s+\z/\n/' "$actual")
