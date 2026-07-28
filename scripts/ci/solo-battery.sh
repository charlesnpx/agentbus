#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)
cd "$ROOT"

export CGO_ENABLED="${CGO_ENABLED:-0}"
export GOCACHE="${GOCACHE:-${TMPDIR:-/tmp}/agentbus-go-cache}"
export GOMODCACHE="${GOMODCACHE:-${TMPDIR:-/tmp}/agentbus-gomod-cache}"
mkdir -p -- "$GOCACHE" "$GOMODCACHE"

# Several tests build helper binaries hermetically (GOPROXY=off), so the
# module cache must be warm before any test phase runs.
go mod download

cpu_count() {
  local count
  if command -v nproc >/dev/null 2>&1; then
    nproc
    return
  fi
  if count=$(getconf _NPROCESSORS_ONLN 2>/dev/null) && [[ "$count" =~ ^[0-9]+$ ]] && ((count > 0)); then
    printf '%s\n' "$count"
    return
  fi
  printf '4\n'
}

run() {
  printf '\n==> %s\n' "$*"
  "$@"
}

check_gofmt() {
  local out
  out=$(mktemp "${TMPDIR:-/tmp}/agentbus-gofmt.XXXXXX")
  gofmt -l . >"$out"
  if [[ -s "$out" ]]; then
    printf 'solo-battery: gofmt produced output; run gofmt on these files:\n' >&2
    cat "$out" >&2
    rm -f -- "$out"
    return 1
  fi
  rm -f -- "$out"
}

pkg_parallel="${GO_TEST_PACKAGES_PARALLEL:-$(cpu_count)}"
test_parallel="${GO_TEST_PARALLEL:-1}"

printf 'solo-battery: root=%s\n' "$ROOT"
printf 'solo-battery: CGO_ENABLED=%s GOCACHE=%s GOMODCACHE=%s\n' "$CGO_ENABLED" "$GOCACHE" "$GOMODCACHE"
printf 'solo-battery: package_parallel=%s test_parallel=%s\n' "$pkg_parallel" "$test_parallel"

run check_gofmt
run go build ./...
run go vet ./...
run go vet -tags abd_strict_e2e ./internal/served/
run go test -count=1 -p "$pkg_parallel" -parallel "$test_parallel" ./...
run go test -count=1 ./...

if [[ "${SOLO_BATTERY_RACE:-0}" == "1" ]]; then
  run env CGO_ENABLED=1 go test -race -count=1 ./...
fi

run env GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./...
run env GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build ./...

printf '\nsolo-battery: ok\n'
