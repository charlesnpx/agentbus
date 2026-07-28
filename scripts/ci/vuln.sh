#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)
cd "$ROOT"

export CGO_ENABLED="${CGO_ENABLED:-0}"
export GOCACHE="${GOCACHE:-${TMPDIR:-/tmp}/agentbus-go-cache}"
export GOMODCACHE="${GOMODCACHE:-${TMPDIR:-/tmp}/agentbus-gomod-cache}"
mkdir -p -- "$GOCACHE" "$GOMODCACHE"

GOVULNCHECK_VERSION="${GOVULNCHECK_VERSION:-v1.1.4}"
STAGE="${VULN_STAGE:-}"
cleanup_stage=0
if [[ -z "$STAGE" ]]; then
  STAGE=$(mktemp -d "${TMPDIR:-/tmp}/agentbus-vuln.XXXXXX")
  cleanup_stage=1
fi
cleanup() {
  if ((cleanup_stage)); then
    rm -rf -- "$STAGE"
  fi
}
trap cleanup EXIT

GOBIN="${GOBIN:-$STAGE/bin}"
mkdir -p -- "$GOBIN"

run() {
  printf '\n==> %s\n' "$*"
  "$@"
}

run env GOBIN="$GOBIN" go install "golang.org/x/vuln/cmd/govulncheck@$GOVULNCHECK_VERSION"
run "$GOBIN/govulncheck" ./...

printf '\nvuln: ok govulncheck=%s\n' "$GOVULNCHECK_VERSION"
