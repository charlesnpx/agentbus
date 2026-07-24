#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)
# Pinned multi-arch manifest-list digest for golang:1.26-trixie (Go 1.26.1, Debian trixie);
# runs natively on both amd64 and arm64 (no --platform: emulation breaks pidfd syscalls).
IMAGE="${DOCKER_IMAGE:-golang:1.26-trixie@sha256:4ee9ffa999b4583ce281939cdff828763083610292f252279a0cee77473bd9a7}"
CONTAINER_GOCACHE="${CONTAINER_GOCACHE:-${GOCACHE:-/tmp/agentbus-go-cache}}"
CONTAINER_GOMODCACHE="${CONTAINER_GOMODCACHE:-${GOMODCACHE:-/tmp/agentbus-gomod-cache}}"

if ! command -v docker >/dev/null 2>&1; then
  printf 'docker-cgroup-v2: docker is required\n' >&2
  exit 127
fi

printf 'docker-cgroup-v2: root=%s image=%s\n' "$ROOT" "$IMAGE"
printf 'docker-cgroup-v2: container GOCACHE=%s GOMODCACHE=%s\n' "$CONTAINER_GOCACHE" "$CONTAINER_GOMODCACHE"

docker run --rm -i \
  --privileged \
  --cgroupns=private \
  -e CGO_ENABLED="${CGO_ENABLED:-0}" \
  -e GOCACHE="$CONTAINER_GOCACHE" \
  -e GOMODCACHE="$CONTAINER_GOMODCACHE" \
  -e GO_TEST_PACKAGES_PARALLEL="${GO_TEST_PACKAGES_PARALLEL:-}" \
  -e GO_TEST_PARALLEL="${GO_TEST_PARALLEL:-}" \
  -e GO_TEST_RACE_PACKAGES_PARALLEL="${GO_TEST_RACE_PACKAGES_PARALLEL:-}" \
  -v "$ROOT:/workspace" \
  -w /workspace \
  "$IMAGE" bash -s <<'CONTAINER'
set -euo pipefail

mkdir -p -- "$GOCACHE" "$GOMODCACHE"

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

pkg_parallel="${GO_TEST_PACKAGES_PARALLEL:-$(cpu_count)}"
test_parallel="${GO_TEST_PARALLEL:-1}"
race_pkg_parallel="${GO_TEST_RACE_PACKAGES_PARALLEL:-2}"
worst=0

run_partition() {
  local name=$1
  shift
  local code
  printf '\n==> [%s] %s\n' "$name" "$*"
  set +e
  "$@"
  code=$?
  set -e
  if ((code != 0)); then
    printf 'docker-cgroup-v2: partition %s failed exit=%d\n' "$name" "$code" >&2
    if ((code > worst)); then
      worst=$code
    elif ((worst == 0)); then
      worst=1
    fi
  fi
}

run_required() {
  printf '\n==> [required] %s\n' "$*"
  "$@"
}

# The strict E2E builds the CLI hermetically (GOPROXY=off), so the module
# cache must be warm before any partition runs.
go mod download
run_required go run ./scripts/ci/strict-cgroup-preflight

printf 'docker-cgroup-v2: go=%s\n' "$(go version)"
printf 'docker-cgroup-v2: CGO_ENABLED=%s GOCACHE=%s GOMODCACHE=%s\n' "${CGO_ENABLED:-}" "$GOCACHE" "$GOMODCACHE"
printf 'docker-cgroup-v2: package_parallel=%s test_parallel=%s race_package_parallel=%s\n' "$pkg_parallel" "$test_parallel" "$race_pkg_parallel"

run_partition build-client-cmd go build ./client ./cmd/agentbus
run_partition build-engine go build ./engine/...
run_partition build-internal go build ./internal/...
# Lease-sensitive packages take the delegated cgroup root lease and must not
# run concurrently with each other or with other packages inside one
# container: they get the serial partition (plus strict-e2e for served) and
# are excluded from the parallel and race partitions.
lease_sensitive='(internal/cgroup|engine/execution/custodian|internal/served)$'
mapfile -t rest_pkgs < <(go list ./... | grep -Ev "$lease_sensitive")

run_partition serial-conformance env AGENTBUS_CGROUP_CONFORMANCE=1 go test ./internal/cgroup ./engine/execution/custodian ./internal/served -count=1 -p 1 -parallel 1
run_partition serial-race env CGO_ENABLED=1 go test -race ./internal/cgroup ./engine/execution/custodian ./internal/served -count=1 -p 1 -parallel 1
run_partition parallel-rest go test "${rest_pkgs[@]}" -count=1 -p "$pkg_parallel" -parallel "$test_parallel"
run_partition race env CGO_ENABLED=1 go test -race "${rest_pkgs[@]}" -count=1 -p "$race_pkg_parallel" -parallel "$test_parallel"
run_partition strict-e2e env AGENTBUS_RUN_STRICT_E2E=1 go test -tags abd_strict_e2e ./internal/served -run TestProductionStrict -count=1 -p 1 -parallel 1

printf '\ndocker-cgroup-v2: worst=%d\n' "$worst"
exit "$worst"
CONTAINER
