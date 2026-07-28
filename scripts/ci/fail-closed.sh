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

restricted_stage=""
restricted_pid=""

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

cleanup_restricted() {
  local cleanup_status=0
  # The autostart daemon's PID lands in the staged state root only after the smoke
  # command triggers autostart; recover it here so a failure BETWEEN daemon start and
  # the explicit restricted_pid capture still tears the daemon down (not just the dir).
  if [[ -z "$restricted_pid" && -n "$restricted_stage" && -s "$restricted_stage/state/agentbus.pid" ]]; then
    restricted_pid=$(tr -d '[:space:]' <"$restricted_stage/state/agentbus.pid" 2>/dev/null || true)
  fi
  if [[ -n "$restricted_pid" ]] && kill -0 "$restricted_pid" >/dev/null 2>&1; then
    kill -TERM "$restricted_pid" >/dev/null 2>&1 || true
    if ! assert_process_absent "$restricted_pid"; then
      cleanup_status=1
    fi
  fi
  restricted_pid=""
  if [[ -n "$restricted_stage" ]]; then
    if ! rm -rf -- "$restricted_stage"; then
      cleanup_status=1
    fi
  fi
  restricted_stage=""
  return "$cleanup_status"
}

cleanup_restricted_trap() {
  local status=$?
  local cleanup_rc=0
  cleanup_restricted || cleanup_rc=$?
  # Preserve a real lane failure; only surface the cleanup failure when the lane
  # itself succeeded (so cleanup never masks the lane's exit code).
  if [[ "$status" -eq 0 ]]; then
    status=$cleanup_rc
  fi
  exit "$status"
}

assert_restricted_cgroup_unavailable() {
  local stage=$1
  local root="${AGENTBUS_RESTRICTED_CGROUP_ROOT:-/sys/fs/cgroup}"
  local probe_dir="$root/agentbus-ci-probe.$$"
  local probe_err="$stage/cgroup-probe.err"
  if [[ ! -d "$root" ]]; then
    printf 'fail-closed: restricted Linux cgroup unavailable: %s is absent\n' "$root"
    return 0
  fi
  if [[ ! -e "$root/cgroup.controllers" ]]; then
    printf 'fail-closed: restricted Linux cgroup unavailable: %s is not a cgroup-v2 root\n' "$root"
    return 0
  fi
  if mkdir "$probe_dir" 2>"$probe_err"; then
    rmdir "$probe_dir" >/dev/null 2>&1 || true
    printf 'fail-closed: restricted Linux cgroup root %s unexpectedly allowed cgroup creation; run linux-restricted in a non-privileged/default-cgroupns container\n' "$root" >&2
    return 1
  fi
  printf 'fail-closed: restricted Linux cgroup unavailable: mkdir %s failed: %s\n' "$probe_dir" "$(cat "$probe_err")"
}

write_restricted_fake_codex() {
  local path=$1
  cat >"$path" <<'EOF'
#!/usr/bin/env sh
if [ "${1:-}" = "--version" ]; then echo "codex-cli 0.143.0"; exit 0; fi
if [ "${1:-}" = "--help" ] || [ "${1:-}" = "help" ]; then echo "codex help"; exit 0; fi
if [ "${1:-}" = "exec" ]; then
  input=$(cat)
  : "$input"
  printf '{"type":"thread.started","thread_id":"restricted-smoke-session"}\n'
  printf '{"type":"turn.started"}\n'
  printf '{"type":"item.completed","item":{"type":"agent_message","text":"restricted smoke ok"}}\n'
  printf '{"type":"turn.completed"}\n'
  exit 0
fi
echo "unexpected fake codex argv: $*" >&2
exit 2
EOF
  chmod 755 "$path"
}

write_restricted_setup_cache() {
  local state_root=$1
  local codex_path=$2
  mkdir -p -- "$state_root"
  cat >"$state_root/setup-probes.json" <<EOF
{
  "version": 3,
  "backends": [
    {
      "backend": "codex",
      "binaryPath": "$codex_path",
      "version": "0.143.0",
      "streamSchema": "codex-json-v1",
      "configMode": {"write": "user", "readOnly": "hermetic"},
      "sandboxModes": ["workspace-write", "read-only"],
      "jsonEventsProbed": true
    }
  ]
}
EOF
  chmod 600 "$state_root/setup-probes.json"
}

write_restricted_submit_program() {
  local path=$1
  cat >"$path" <<'EOF'
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	agentclient "github.com/charlesnpx/agentbus/client"
	"github.com/charlesnpx/agentbus/engine"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "usage: restricted_submit <state-root> <cwd>\n")
		os.Exit(2)
	}
	stateRoot := os.Args[1]
	cwd := os.Args[2]
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := agentclient.Connect(ctx, agentclient.Options{StateRoot: stateRoot, DisableAutoStart: true})
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect daemon: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()
	submitted, err := client.JobSubmit(ctx, agentclient.JobSubmitParams{
		WorkspaceKey: "workspace-linux-restricted-fallback",
		RequestID:    "request-linux-restricted-fallback",
		TaskSpec: agentclient.TaskSpec{
			Backend: "codex",
			CWD:     cwd,
			Write:   false,
			Prompt:  "restricted linux process-group fallback smoke",
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "submit job: %v\n", err)
		os.Exit(1)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		statuses, err := client.JobStatus(ctx, agentclient.JobStatusParams{JobID: submitted.JobID})
		if err != nil {
			fmt.Fprintf(os.Stderr, "status %s: %v\n", submitted.JobID, err)
			os.Exit(1)
		}
		if len(statuses.Jobs) != 1 {
			fmt.Fprintf(os.Stderr, "status %s returned %d jobs\n", submitted.JobID, len(statuses.Jobs))
			os.Exit(1)
		}
		switch statuses.Jobs[0].State {
		case engine.StateCompleted:
			fmt.Println(submitted.JobID)
			return
		case engine.StateFailed, engine.StateCanceled, engine.StateOrphaned:
			fmt.Fprintf(os.Stderr, "job %s terminal state=%s\n", submitted.JobID, statuses.Jobs[0].State)
			os.Exit(1)
		}
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Fprintf(os.Stderr, "job %s did not complete before deadline\n", submitted.JobID)
	os.Exit(1)
}
EOF
}

write_restricted_verify_program() {
  local path=$1
  cat >"$path" <<'EOF'
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
	bboltrepo "github.com/charlesnpx/agentbus/engine/execution/storage/bbolt"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "usage: restricted_verify_group <state-root> <job-id>\n")
		os.Exit(2)
	}
	stateRoot := os.Args[1]
	jobID, err := model.NewJobID(os.Args[2])
	if err != nil {
		fmt.Fprintf(os.Stderr, "job id: %v\n", err)
		os.Exit(1)
	}
	repo, err := bboltrepo.OpenExistingReadOnly(filepath.Join(stateRoot, authority.AdmissionRepositoryFile))
	if err != nil {
		fmt.Fprintf(os.Stderr, "open admission repository: %v\n", err)
		os.Exit(1)
	}
	defer repo.Close()
	var record model.SafetyRecord
	err = repo.View(context.Background(), func(tx repository.ReadTx) error {
		image := tx.LoadJob(jobID)
		if image.Safety.State != repository.RecordValid {
			return fmt.Errorf("authority safety state = %s", image.Safety.State)
		}
		record = image.Safety.Value
		return nil
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "load safety record: %v\n", err)
		os.Exit(1)
	}
	launch, ok := record.Attempt.Launches.Get(model.LaunchOrdinalOne)
	if !ok || launch.Group == nil {
		fmt.Fprintf(os.Stderr, "launch one group missing: %+v\n", record.Attempt.Launches)
		os.Exit(1)
	}
	group := *launch.Group
	if group.RetainedID != "" {
		fmt.Fprintf(os.Stderr, "launch group retainedID=%q, want empty process-group fallback: %+v\n", group.RetainedID, group)
		os.Exit(1)
	}
	if group.RetainedDomainState != model.RetainedDomainNotApplicable {
		fmt.Fprintf(os.Stderr, "launch retained domain state=%v, want not-applicable process-group fallback: %+v\n", group.RetainedDomainState, group)
		os.Exit(1)
	}
	if group.PGID <= 1 || group.Leader.PID != group.PGID {
		fmt.Fprintf(os.Stderr, "launch group identity is not process-group backed: %+v\n", group)
		os.Exit(1)
	}
	fmt.Printf("fail-closed: verified process-group fallback job=%s pgid=%d retainedID=%q retainedDomainState=%v\n", jobID, group.PGID, group.RetainedID, group.RetainedDomainState)
}
EOF
}

run_darwin() {
  run go test ./internal/agentbusserve ./engine/execution/custodian -run 'Test(ProductionServedConfigSelectsNativeStrictRuntime|ProductionStrictServePreflightPassesOnDarwin|ProductionServeLauncherServesOnDarwinFreshRoot|ProductionServeLauncherServesFromExistingRootOnDarwin|ProductionRecoverCLIReportsRootMissingOnDarwin|NewNativeRuntimeDarwinQualifiesAfterSelfTest)' -count=1
  run env AGENTBUS_RUN_STRICT_E2E=1 go test -tags abd_strict_e2e ./internal/served -run 'Test(ProductionStrictServe|ProductionStrictCLINoDaemonAutostartsDarwinE2B|ProductionStrictJobCLIStatusResultCancelE2B|ProductionStrictSIGTERMMidJobGracefulShutdownE2B|ProductionStrictCLIOrphanedExitCodeE2B|ServedStrictCompositionIdentifiedSubmitReplayConformance|ServedStrictCompositionCancellationConformance|ServedStrictCompositionDaemonSIGKILLRestartRecoveryConformance|ServedStrictCompositionReleaseAckLossConformance)' -count=1
}

run_linux_restricted() {
  local bin state_root stdout stderr code version fake_bin_dir fake_codex cwd job_file submit_go verify_go helper_stdout helper_stderr job_id
  if [[ "$(go env GOOS)" != "linux" ]]; then
    printf 'fail-closed: linux-restricted mode requires GOOS=linux, got %s\n' "$(go env GOOS)" >&2
    exit 2
  fi
  run go test ./engine/execution/custodian ./internal/served ./internal/agentbusserve -run 'Test(LinuxNativeContainmentBackendSelection|NativeCgroupConstructionFallbackClassification|DefaultStrictServeRejectsUnavailableRuntimeBeforeListen|StrictRequestedUnavailableRuntimeFailsStartupWithSupportDiagnostic|BootstrapAdmissionStrictRuntimeFailurePrecedesRepositoryOpen|ProductionServedConfigSelectsNativeStrictRuntime|ActivatedBboltV1RootFailsTypedBeforeSocketBindAndLeavesFileUntouched)' -count=1

  restricted_stage=$(mktemp -d "${TMPDIR:-/tmp}/agentbus-fail-closed.XXXXXX")
  # Install the cleanup trap immediately after the stage exists so a failure before
  # the explicit trap line (e.g. reading VERSION) cannot leak the stage directory.
  trap cleanup_restricted_trap EXIT
  bin="$restricted_stage/agentbus"
  state_root="$restricted_stage/state"
  stdout="$restricted_stage/stdout.log"
  stderr="$restricted_stage/stderr.log"
  helper_stdout="$restricted_stage/helper.stdout"
  helper_stderr="$restricted_stage/helper.stderr"
  job_file="$restricted_stage/job-id"
  cwd="$restricted_stage/workspace"
  fake_bin_dir="$restricted_stage/fake-bin"
  fake_codex="$fake_bin_dir/codex"
  submit_go="$restricted_stage/restricted_submit.go"
  verify_go="$restricted_stage/restricted_verify.go"
  version=$(tr -d '[:space:]' <"$ROOT/VERSION")
  restricted_pid=""

  mkdir -p -- "$fake_bin_dir" "$restricted_stage/home" "$restricted_stage/codex-home" "$cwd"
  assert_restricted_cgroup_unavailable "$restricted_stage"
  write_restricted_fake_codex "$fake_codex"
  write_restricted_setup_cache "$state_root" "$fake_codex"
  write_restricted_submit_program "$submit_go"
  write_restricted_verify_program "$verify_go"
  run go build -trimpath -ldflags "-X main.version=$version" -o "$bin" ./cmd/agentbus

  printf '\n==> restricted Linux exact-binary process-group fallback smoke\n'
  set +e
  PATH="$fake_bin_dir:/usr/bin:/bin" HOME="$restricted_stage/home" XDG_CACHE_HOME="$restricted_stage/home/cache" CODEX_HOME="$restricted_stage/codex-home" AGENTBUS_STATE_ROOT="$state_root" "$bin" status --job job_linux_fallback --json >"$stdout" 2>"$stderr"
  code=$?
  set -e
  if [[ "$code" -ne 10 ]]; then
    printf 'fail-closed: restricted Linux fallback smoke exit=%d, want unknown-job exit 10 from a serving daemon\nstdout=%s\nstderr=%s\n' "$code" "$(cat "$stdout")" "$(cat "$stderr")" >&2
    return 1
  fi
  if grep -q 'strict admission support unavailable' "$stderr"; then
    printf 'fail-closed: restricted Linux fallback smoke reported strict support unavailable\nstdout=%s\nstderr=%s\n' "$(cat "$stdout")" "$(cat "$stderr")" >&2
    return 1
  fi
  if [[ ! -s "$state_root/agentbus.pid" || ! -S "$state_root/agentbus.sock" || ! -s "$state_root/token" || ! -s "$state_root/admission.bbolt" || ! -s "$state_root/admission-anchor.json" ]]; then
    printf 'fail-closed: restricted Linux fallback did not leave a serving state root\nstdout=%s\nstderr=%s\nstate entries:\n%s\n' "$(cat "$stdout")" "$(cat "$stderr")" "$(find "$state_root" -mindepth 1 -maxdepth 1 -print | sort)" >&2
    return 1
  fi
  restricted_pid=$(tr -d '[:space:]' <"$state_root/agentbus.pid")
  if ! kill -0 "$restricted_pid" >/dev/null 2>&1; then
    printf 'fail-closed: restricted Linux fallback daemon pid %s is not alive\nstdout=%s\nstderr=%s\n' "$restricted_pid" "$(cat "$stdout")" "$(cat "$stderr")" >&2
    return 1
  fi

  printf '\n==> go run restricted submit smoke\n'
  if ! go run "$submit_go" "$state_root" "$cwd" >"$job_file" 2>"$helper_stderr"; then
    printf 'fail-closed: restricted Linux submit failed\nstdout=%s\nstderr=%s\n' "$(cat "$job_file")" "$(cat "$helper_stderr")" >&2
    return 1
  fi
  job_id=$(tr -d '[:space:]' <"$job_file")
  if [[ -z "$job_id" ]]; then
    printf 'fail-closed: restricted Linux submit did not report a job id\nstderr=%s\n' "$(cat "$helper_stderr")" >&2
    return 1
  fi
  kill -TERM "$restricted_pid" >/dev/null 2>&1 || true
  assert_process_absent "$restricted_pid"
  restricted_pid=""
  printf '\n==> go run restricted group verification\n'
  if ! go run "$verify_go" "$state_root" "$job_id" >"$helper_stdout" 2>"$helper_stderr"; then
    printf 'fail-closed: restricted Linux group verification failed\nstdout=%s\nstderr=%s\n' "$(cat "$helper_stdout")" "$(cat "$helper_stderr")" >&2
    return 1
  fi
  cat "$helper_stdout"
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
