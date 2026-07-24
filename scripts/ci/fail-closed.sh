#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)
cd "$ROOT"

export CGO_ENABLED="${CGO_ENABLED:-0}"
export GOCACHE="${GOCACHE:-${TMPDIR:-/tmp}/agentbus-go-cache}"
export GOMODCACHE="${GOMODCACHE:-${TMPDIR:-/tmp}/agentbus-gomod-cache}"
mkdir -p -- "$GOCACHE" "$GOMODCACHE"

MODE="${1:-auto}"
if [[ "$MODE" == "auto" ]]; then
  case "$(go env GOOS)" in
    darwin) MODE=darwin ;;
    linux) MODE=linux-restricted ;;
    *)
      printf 'fail-closed: unsupported GOOS for auto mode: %s\n' "$(go env GOOS)" >&2
      exit 2
      ;;
  esac
fi

run() {
  printf '\n==> %s\n' "$*"
  "$@"
}

assert_empty_state_root() {
  local state_root=$1
  local residue
  if [[ ! -d "$state_root" ]]; then
    return 0
  fi
  residue=$(find "$state_root" -mindepth 1 -maxdepth 4 -print | sort)
  if [[ -n "$residue" ]]; then
    printf 'fail-closed: state root mutated after typed unsupported failure; forbidden residue:\n%s\n' "$residue" >&2
    return 1
  fi
}

run_darwin() {
  run go test ./internal/agentbusserve ./client ./internal/cgroup ./engine/execution/custodian -run 'Test(ProductionStrictServeFailsTypedOnDarwin|ProductionServeLauncherUnsupportedLeavesFreshRootAbsentOnDarwin|ProductionServeLauncherUnsupportedLeavesExistingRootPermissionsOnDarwin|ProductionRecoverCLIUnsupportedLeavesExistingRootEmptyOnDarwin|ConnectAutostartRealUnsupportedHostSurfacesLauncherDiagnosticOnDarwin|DarwinNewFailsClosed|NewNativeRuntimeDarwinUnsupported)' -count=1
  run env AGENTBUS_RUN_STRICT_E2E=1 go test -tags abd_strict_e2e ./internal/served -run TestProductionStrictCLINoDaemonUnsupportedHostDarwinE2B -count=1
}

run_linux_restricted() {
  local stage bin state_root stdout stderr pid code attempts version
  if [[ "$(go env GOOS)" != "linux" ]]; then
    printf 'fail-closed: linux-restricted mode requires GOOS=linux, got %s\n' "$(go env GOOS)" >&2
    exit 2
  fi
  run go test ./internal/cgroup ./internal/served ./internal/agentbusserve -run 'Test(ProbeClassifiesStrictSupportAndUnsupportedConditions|DefaultStrictServeRejectsUnavailableRuntimeBeforeListen|StrictRequestedUnavailableRuntimeFailsStartupWithSupportDiagnostic|BootstrapAdmissionStrictRuntimeFailurePrecedesRepositoryOpen|ProductionServedConfigSelectsNativeStrictRuntime)' -count=1

  stage=$(mktemp -d "${TMPDIR:-/tmp}/agentbus-fail-closed.XXXXXX")
  bin="$stage/agentbus"
  state_root="$stage/state"
  stdout="$stage/stdout.log"
  stderr="$stage/stderr.log"
  version=$(tr -d '[:space:]' <"$ROOT/VERSION")
  run go build -trimpath -ldflags "-X main.version=$version" -o "$bin" ./cmd/agentbus
  mkdir -p -- "$state_root"

  printf '\n==> restricted Linux exact-binary strict unsupported smoke\n'
  set +e
  PATH="$stage/no-backends" AGENTBUS_STATE_ROOT="$state_root" "$bin" serve --foreground >"$stdout" 2>"$stderr" &
  pid=$!
  attempts=0
  while kill -0 "$pid" >/dev/null 2>&1 && ((attempts < 100)); do
    sleep 0.1
    attempts=$((attempts + 1))
  done
  if kill -0 "$pid" >/dev/null 2>&1; then
    kill -TERM "$pid" >/dev/null 2>&1 || true
    wait "$pid" >/dev/null 2>&1
    set -e
    printf 'fail-closed: restricted Linux strict serve unexpectedly stayed up; use the privileged lane for serving checks\n' >&2
    rm -rf -- "$stage"
    return 1
  fi
  wait "$pid"
  code=$?
  set -e
  if [[ "$code" -eq 0 ]] || ! grep -q 'strict admission support unavailable' "$stderr"; then
    printf 'fail-closed: restricted Linux smoke exit=%d, want strict fail-closed diagnostic\nstdout=%s\nstderr=%s\n' "$code" "$(cat "$stdout")" "$(cat "$stderr")" >&2
    rm -rf -- "$stage"
    return 1
  fi
  if ! assert_empty_state_root "$state_root"; then
    rm -rf -- "$stage"
    return 1
  fi
  rm -rf -- "$stage"
}

case "$MODE" in
  darwin) run_darwin ;;
  linux-restricted) run_linux_restricted ;;
  *)
    printf 'fail-closed: unknown mode %s\n' "$MODE" >&2
    exit 2
    ;;
esac

printf '\nfail-closed: ok mode=%s\n' "$MODE"
