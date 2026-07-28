#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)
# Pinned multi-arch manifest-list digest for golang:1.26-trixie (Go 1.26.1, Debian trixie);
# runs natively on both amd64 and arm64 (no --platform: emulation breaks pidfd syscalls).
IMAGE="${DOCKER_IMAGE:-golang:1.26-trixie@sha256:4ee9ffa999b4583ce281939cdff828763083610292f252279a0cee77473bd9a7}"

if [[ "${AGENTBUS_STRICT_DOCKER_INSIDE:-0}" != "1" ]]; then
  if ! command -v docker >/dev/null 2>&1; then
    printf 'product-e2e: docker is required for the strict-capable lane\n' >&2
    exit 127
  fi
  printf 'product-e2e: entering strict-capable container image=%s\n' "$IMAGE"
  exec docker run --rm -i \
      --privileged \
    --cgroupns=private \
    -e AGENTBUS_STRICT_DOCKER_INSIDE=1 \
    -e CGO_ENABLED="${CGO_ENABLED:-0}" \
    -e GOCACHE="${CONTAINER_GOCACHE:-/tmp/agentbus-go-cache}" \
    -e GOMODCACHE="${CONTAINER_GOMODCACHE:-/tmp/agentbus-gomod-cache}" \
    -v "$ROOT:/workspace" \
    -w /workspace \
    "$IMAGE" bash scripts/ci/product-e2e.sh
fi

cd "$ROOT"

export CGO_ENABLED="${CGO_ENABLED:-0}"
export GOCACHE="${GOCACHE:-${TMPDIR:-/tmp}/agentbus-go-cache}"
export GOMODCACHE="${GOMODCACHE:-${TMPDIR:-/tmp}/agentbus-gomod-cache}"
mkdir -p -- "$GOCACHE" "$GOMODCACHE"

VERSION=$(tr -d '[:space:]' <"$ROOT/VERSION")
STAGE="${PRODUCT_E2E_STAGE:-}"
cleanup_stage=0
if [[ -z "$STAGE" ]]; then
  STAGE=$(mktemp -d "${TMPDIR:-/tmp}/agentbus-product-e2e.XXXXXX")
  cleanup_stage=1
fi
cleanup() {
  if ((cleanup_stage)); then
    rm -rf -- "$STAGE"
  fi
}
trap cleanup EXIT

BIN="${PRODUCT_E2E_BIN:-$STAGE/agentbus}"

run() {
  printf '\n==> %s\n' "$*"
  "$@"
}

require_python3() {
  if ! command -v python3 >/dev/null 2>&1; then
    printf 'product-e2e: python3 is required for JSON smoke validation\n' >&2
    exit 127
  fi
}

validate_version_json() {
  local raw=$1
  EXPECTED_VERSION="$VERSION" python3 -c '
import json
import os
import sys

doc = json.loads(sys.argv[1])
if doc.get("schema") != 1:
    raise SystemExit("schema is not 1")
if doc.get("version") != os.environ["EXPECTED_VERSION"]:
    raise SystemExit(f"version={doc.get('version')!r}, want {os.environ['EXPECTED_VERSION']!r}")
if not isinstance(doc.get("protocolVersion"), int) or doc["protocolVersion"] <= 0:
    raise SystemExit("protocolVersion is missing or invalid")
' "$raw"
}

smoke_compiled_binary() {
  local stdout stderr code version_json
  stdout=$(mktemp "${TMPDIR:-/tmp}/agentbus-product-stdout.XXXXXX")
  stderr=$(mktemp "${TMPDIR:-/tmp}/agentbus-product-stderr.XXXXXX")

  run "$BIN" --help
  version_json=$("$BIN" version --json)
  validate_version_json "$version_json"

  set +e
  "$BIN" >"$stdout" 2>"$stderr"
  code=$?
  set -e
  if [[ "$code" -ne 2 ]]; then
    printf 'product-e2e: no-args exit=%d, want 2\nstdout=%s\nstderr=%s\n' "$code" "$(cat "$stdout")" "$(cat "$stderr")" >&2
    rm -f -- "$stdout" "$stderr"
    return 1
  fi
  rm -f -- "$stdout" "$stderr"
}

require_python3
run go mod download
run go run ./scripts/ci/strict-cgroup-preflight
run go build -trimpath -ldflags "-X main.version=$VERSION" -o "$BIN" ./cmd/agentbus
run smoke_compiled_binary
run env AGENTBUS_E2E_PREBUILT_BINARY="$BIN" AGENTBUS_RUN_STRICT_E2E=1 go test -tags abd_strict_e2e ./internal/served -run '^TestProductionStrict.*E2B$' -count=1

printf '\nproduct-e2e: ok binary=%s\n' "$BIN"
