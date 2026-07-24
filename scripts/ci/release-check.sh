#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)
# Pinned multi-arch manifest-list digest for golang:1.26-trixie (Go 1.26.1, Debian trixie);
# runs natively on both amd64 and arm64 (no --platform: emulation breaks pidfd syscalls).
IMAGE="${DOCKER_IMAGE:-golang:1.26-trixie@sha256:4ee9ffa999b4583ce281939cdff828763083610292f252279a0cee77473bd9a7}"

if [[ "${AGENTBUS_STRICT_DOCKER_INSIDE:-0}" != "1" ]]; then
  if ! command -v docker >/dev/null 2>&1; then
    printf 'release-check: docker is required for the strict-capable lane\n' >&2
    exit 127
  fi
  printf 'release-check: entering strict-capable container image=%s\n' "$IMAGE"
  exec docker run --rm -i \
      --privileged \
    --cgroupns=private \
    -e AGENTBUS_STRICT_DOCKER_INSIDE=1 \
    -e CGO_ENABLED="${CGO_ENABLED:-0}" \
    -e GOCACHE="${CONTAINER_GOCACHE:-/tmp/agentbus-go-cache}" \
    -e GOMODCACHE="${CONTAINER_GOMODCACHE:-/tmp/agentbus-gomod-cache}" \
    -v "$ROOT:/workspace" \
    -w /workspace \
    "$IMAGE" bash scripts/ci/release-check.sh
fi

cd "$ROOT"

export CGO_ENABLED="${CGO_ENABLED:-0}"
export GOCACHE="${GOCACHE:-${TMPDIR:-/tmp}/agentbus-go-cache}"
export GOMODCACHE="${GOMODCACHE:-${TMPDIR:-/tmp}/agentbus-gomod-cache}"
mkdir -p -- "$GOCACHE" "$GOMODCACHE"

# Several tests build helper binaries hermetically (GOPROXY=off), so the
# module cache must be warm before any test phase runs.
go mod download

VERSION=$(tr -d '[:space:]' <"$ROOT/VERSION")
STAGE="${RELEASE_CHECK_STAGE:-}"
cleanup_stage=0
if [[ -z "$STAGE" ]]; then
  STAGE=$(mktemp -d "${TMPDIR:-/tmp}/agentbus-release-check.XXXXXX")
  cleanup_stage=1
fi
cleanup() {
  if ((cleanup_stage)); then
    rm -rf -- "$STAGE"
  fi
}
trap cleanup EXIT

BIN="${RELEASE_CHECK_BIN:-$STAGE/agentbus}"

run() {
  printf '\n==> %s\n' "$*"
  "$@"
}

require_python3() {
  if ! command -v python3 >/dev/null 2>&1; then
    printf 'release-check: python3 is required for JSON smoke validation\n' >&2
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

write_fake_backends() {
  local bin_dir=$1
  local codex_home=$2
  mkdir -p -- "$bin_dir" "$codex_home"
  cat >"$bin_dir/codex" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "${1:-}" in
  --version)
    printf 'codex-cli 0.143.0\n'
    exit 0
    ;;
  --help|help)
    printf 'codex release smoke fixture\n'
    exit 0
    ;;
esac
cat >/dev/null || true
printf '%s\n' '{"type":"agent_message","text":"release smoke ok\n","thread_id":"release-smoke"}'
printf '%s\n' '{"type":"turn.completed","last_agent_message":"release smoke ok\n","thread_id":"release-smoke"}'
EOF
  cat >"$bin_dir/claude" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "${1:-}" in
  --version)
    printf '2.1.205\n'
    exit 0
    ;;
  --help|help)
    printf 'claude release smoke fixture\n'
    printf '  --model string\n      model to use (sonnet)\n'
    printf '  --effort string\n      effort to use (low, medium, high)\n'
    exit 0
    ;;
esac
cat >/dev/null || true
printf '%s\n' '{"type":"assistant","message":{"content":[{"type":"text","text":"release smoke ok\n"}]},"session_id":"release-smoke"}'
printf '%s\n' '{"type":"result","result":"release smoke ok\n","session_id":"release-smoke"}'
EOF
  chmod +x "$bin_dir/codex" "$bin_dir/claude"
  cat >"$codex_home/models_cache.json" <<'EOF'
{"fetched_at":"2026-01-01T00:00:00Z","client_version":"0.143.0","models":[{"slug":"gpt-5","visibility":"list","supported_reasoning_levels":[{"effort":"low"},{"effort":"medium"},{"effort":"high"}]}]}
EOF
}

smoke_release_binary() {
  local version_json stdout stderr code
  stdout=$(mktemp "${TMPDIR:-/tmp}/agentbus-release-stdout.XXXXXX")
  stderr=$(mktemp "${TMPDIR:-/tmp}/agentbus-release-stderr.XXXXXX")

  run "$BIN" --help
  version_json=$("$BIN" version --json)
  validate_version_json "$version_json"

  set +e
  "$BIN" >"$stdout" 2>"$stderr"
  code=$?
  set -e
  if [[ "$code" -ne 2 ]]; then
    printf 'release-check: no-args exit=%d, want 2\nstdout=%s\nstderr=%s\n' "$code" "$(cat "$stdout")" "$(cat "$stderr")" >&2
    rm -f -- "$stdout" "$stderr"
    return 1
  fi
  rm -f -- "$stdout" "$stderr"
}

strict_startup_smoke() {
  local smoke_dir bin_dir codex_home home state_root cwd stdout stderr pid code attempts status_json
  smoke_dir="$STAGE/strict-smoke"
  bin_dir="$smoke_dir/bin"
  codex_home="$smoke_dir/codex-home"
  home="$smoke_dir/home"
  state_root="$smoke_dir/state"
  cwd="$smoke_dir/cwd"
  stdout="$smoke_dir/stdout.log"
  stderr="$smoke_dir/stderr.log"
  mkdir -p -- "$home" "$state_root" "$cwd"
  write_fake_backends "$bin_dir" "$codex_home"

  printf '\n==> strict startup smoke against exact release binary\n'
  PATH="$bin_dir:$PATH" \
    HOME="$home" \
    CODEX_HOME="$codex_home" \
    AGENTBUS_STATE_ROOT="$state_root" \
    "$BIN" serve --foreground >"$stdout" 2>"$stderr" &
  pid=$!
  attempts=0
  while kill -0 "$pid" >/dev/null 2>&1 && ((attempts < 100)); do
    set +e
    status_json=$(PATH="$bin_dir:$PATH" HOME="$home" CODEX_HOME="$codex_home" AGENTBUS_STATE_ROOT="$state_root" "$BIN" status --json 2>/dev/null)
    code=$?
    set -e
    if [[ "$code" -eq 0 ]] && [[ "$status_json" == *'"jobs"'* ]] && [[ -S "$state_root/agentbus.sock" ]]; then
      kill -TERM "$pid" >/dev/null 2>&1 || true
      wait "$pid" >/dev/null 2>&1 || true
      printf 'release-check: exact binary strict startup smoke ready via status round-trip\n'
      return 0
    fi
    sleep 0.1
    attempts=$((attempts + 1))
  done
  if kill -0 "$pid" >/dev/null 2>&1; then
    kill -TERM "$pid" >/dev/null 2>&1 || true
    wait "$pid" >/dev/null 2>&1 || true
    printf 'release-check: exact binary strict startup smoke did not become ready after %s ms\nstdout=%s\nstderr=%s\n' "$((attempts * 100))" "$(cat "$stdout")" "$(cat "$stderr")" >&2
    return 1
  fi
  set +e
  wait "$pid"
  code=$?
  set -e
  printf 'release-check: exact binary strict startup smoke exit=%d before readiness\nstdout=%s\nstderr=%s\n' "$code" "$(cat "$stdout")" "$(cat "$stderr")" >&2
  return 1
}

require_python3
printf 'release-check: Linux binaries are the supported production artifacts; non-Linux binaries are non-serving tooling.\n'
# Serial package execution: several Linux tests take the delegated cgroup
# root lease, which parallel package runs contend on inside one container.
run go test -count=1 -p 1 ./...
run go run ./scripts/ci/strict-cgroup-preflight
run go build -trimpath -ldflags "-X main.version=$VERSION" -o "$BIN" ./cmd/agentbus
run smoke_release_binary
run strict_startup_smoke
run env AGENTBUS_E2E_PREBUILT_BINARY="$BIN" AGENTBUS_RUN_STRICT_E2E=1 go test -tags abd_strict_e2e ./internal/served -run TestProductionStrict -count=1

printf '\nrelease-check: ok version=%s binary=%s\n' "$VERSION" "$BIN"
