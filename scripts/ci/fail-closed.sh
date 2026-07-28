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

assert_process_absent() {
  local pid=$1
  local attempts=0
  while kill -0 "$pid" >/dev/null 2>&1 && ((attempts < 100)); do
    sleep 0.1
    attempts=$((attempts + 1))
  done
  if kill -0 "$pid" >/dev/null 2>&1; then
    printf 'fail-closed: process %s is still present\n' "$pid" >&2
    return 1
  fi
}

run_darwin() {
  run go test ./internal/agentbusserve ./engine/execution/custodian -run 'Test(ProductionServedConfigSelectsNativeStrictRuntime|ProductionStrictServePreflightPassesOnDarwin|ProductionServeLauncherServesOnDarwinFreshRoot|ProductionServeLauncherServesFromExistingRootOnDarwin|ProductionRecoverCLIReportsRootMissingOnDarwin|NewNativeRuntimeDarwinQualifiesAfterSelfTest)' -count=1
  run env AGENTBUS_RUN_STRICT_E2E=1 go test -tags abd_strict_e2e ./internal/served -run 'Test(ProductionStrictServe|ProductionStrictCLINoDaemonAutostartsDarwinE2B|ProductionStrictJobCLIStatusResultCancelE2B|ProductionStrictSIGTERMMidJobGracefulShutdownE2B|ProductionStrictCLIOrphanedExitCodeE2B|ServedStrictCompositionIdentifiedSubmitReplayConformance|ServedStrictCompositionCancellationConformance|ServedStrictCompositionDaemonSIGKILLRestartRecoveryConformance|ServedStrictCompositionReleaseAckLossConformance)' -count=1
}

run_linux_restricted() {
  local stage bin state_root stdout stderr pid code version
  if [[ "$(go env GOOS)" != "linux" ]]; then
    printf 'fail-closed: linux-restricted mode requires GOOS=linux, got %s\n' "$(go env GOOS)" >&2
    exit 2
  fi
  run go test ./engine/execution/custodian ./internal/served ./internal/agentbusserve -run 'Test(LinuxNativeContainmentBackendSelection|NativeCgroupConstructionFallbackClassification|DefaultStrictServeRejectsUnavailableRuntimeBeforeListen|StrictRequestedUnavailableRuntimeFailsStartupWithSupportDiagnostic|BootstrapAdmissionStrictRuntimeFailurePrecedesRepositoryOpen|ProductionServedConfigSelectsNativeStrictRuntime|ActivatedBboltV1RootFailsTypedBeforeSocketBindAndLeavesFileUntouched)' -count=1

  stage=$(mktemp -d "${TMPDIR:-/tmp}/agentbus-fail-closed.XXXXXX")
  bin="$stage/agentbus"
  state_root="$stage/state"
  stdout="$stage/stdout.log"
  stderr="$stage/stderr.log"
  version=$(tr -d '[:space:]' <"$ROOT/VERSION")
  mkdir -p -- "$stage/no-backends" "$stage/home"
  run go build -trimpath -ldflags "-X main.version=$version" -o "$bin" ./cmd/agentbus

  printf '\n==> restricted Linux exact-binary process-group fallback smoke\n'
  set +e
  PATH="$stage/no-backends" HOME="$stage/home" AGENTBUS_STATE_ROOT="$state_root" "$bin" status --job job_linux_fallback --json >"$stdout" 2>"$stderr"
  code=$?
  set -e
  if [[ "$code" -ne 10 ]]; then
    printf 'fail-closed: restricted Linux fallback smoke exit=%d, want unknown-job exit 10 from a serving daemon\nstdout=%s\nstderr=%s\n' "$code" "$(cat "$stdout")" "$(cat "$stderr")" >&2
    rm -rf -- "$stage"
    return 1
  fi
  if grep -q 'strict admission support unavailable' "$stderr"; then
    printf 'fail-closed: restricted Linux fallback smoke reported strict support unavailable\nstdout=%s\nstderr=%s\n' "$(cat "$stdout")" "$(cat "$stderr")" >&2
    rm -rf -- "$stage"
    return 1
  fi
  if [[ ! -s "$state_root/agentbus.pid" || ! -S "$state_root/agentbus.sock" || ! -s "$state_root/token" || ! -s "$state_root/admission.bbolt" || ! -s "$state_root/admission-anchor.json" ]]; then
    printf 'fail-closed: restricted Linux fallback did not leave a serving state root\nstdout=%s\nstderr=%s\nstate entries:\n%s\n' "$(cat "$stdout")" "$(cat "$stderr")" "$(find "$state_root" -mindepth 1 -maxdepth 1 -print | sort)" >&2
    rm -rf -- "$stage"
    return 1
  fi
  pid=$(tr -d '[:space:]' <"$state_root/agentbus.pid")
  if ! kill -0 "$pid" >/dev/null 2>&1; then
    printf 'fail-closed: restricted Linux fallback daemon pid %s is not alive\nstdout=%s\nstderr=%s\n' "$pid" "$(cat "$stdout")" "$(cat "$stderr")" >&2
    rm -rf -- "$stage"
    return 1
  fi
  kill -TERM "$pid" >/dev/null 2>&1 || true
  assert_process_absent "$pid"
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
