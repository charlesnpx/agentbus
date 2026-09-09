#!/usr/bin/env bash
set -euo pipefail

NAME="agentbus"
SCHEMA="1"
OPERATION=""
TARGET="all"
INSTALL_ROOT_ARG=""
WARNINGS_JSON=""

die() {
  printf 'agentbus installer: %s\n' "$*" >&2
  exit 2
}

script_dir() {
  local source_dir
  source_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
  printf '%s\n' "$source_dir"
}

REPO_ROOT=$(script_dir)
VERSION_FILE="$REPO_ROOT/VERSION"

if [[ ! -f "$VERSION_FILE" ]]; then
  die "VERSION file not found at $VERSION_FILE"
fi

VERSION=$(tr -d '[:space:]' <"$VERSION_FILE")
if [[ -z "$VERSION" ]]; then
  die "VERSION file is empty"
fi

json_escape() {
  local value=$1
  value=${value//\\/\\\\}
  value=${value//\"/\\\"}
  value=${value//$'\n'/\\n}
  value=${value//$'\r'/\\r}
  value=${value//$'\t'/\\t}
  printf '%s' "$value"
}

add_warning() {
  local escaped
  escaped=$(json_escape "$1")
  if [[ -n "$WARNINGS_JSON" ]]; then
    WARNINGS_JSON="$WARNINGS_JSON,"
  fi
  WARNINGS_JSON="$WARNINGS_JSON\"$escaped\""
}

set_operation() {
  local next=$1
  if [[ -n "$OPERATION" ]]; then
    die "exactly one operation flag is allowed"
  fi
  OPERATION=$next
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --plan)
      set_operation "plan"
      shift
      ;;
    --install)
      set_operation "install"
      shift
      ;;
    --uninstall)
      set_operation "uninstall"
      shift
      ;;
    --target)
      [[ $# -ge 2 ]] || die "--target requires claude, codex, tools, or all"
      TARGET=$2
      shift 2
      ;;
    --json)
      shift
      ;;
    --install-root)
      [[ $# -ge 2 ]] || die "--install-root requires an absolute path"
      INSTALL_ROOT_ARG=$2
      shift 2
      ;;
    -h|--help)
      printf 'Usage: %s [--plan|--install|--uninstall] --target claude|codex|tools|all [--json] [--install-root <abs>]\n' "$0" >&2
      exit 0
      ;;
    *)
      die "unknown flag: $1"
      ;;
  esac
done

if [[ -z "$OPERATION" ]]; then
  OPERATION="install"
fi

case "$TARGET" in
  claude|codex|tools|all) ;;
  *) die "--target must be claude, codex, tools, or all" ;;
esac

if [[ -n "$INSTALL_ROOT_ARG" && "$INSTALL_ROOT_ARG" != /* ]]; then
  die "--install-root must be absolute"
fi

canonical_existing_dir() {
  local dir=$1
  (cd -- "$dir" && pwd -P)
}

trim_trailing_slash() {
  local path=$1
  while [[ "$path" != "/" && "$path" == */ ]]; do
    path=${path%/}
  done
  printf '%s\n' "$path"
}

resolve_root() {
  local root
  if [[ -n "$INSTALL_ROOT_ARG" ]]; then
    root=$INSTALL_ROOT_ARG
    if [[ "$OPERATION" == "install" ]]; then
      mkdir -p -- "$root"
      canonical_existing_dir "$root"
      return
    fi
    if [[ -d "$root" ]]; then
      canonical_existing_dir "$root"
      return
    fi
    trim_trailing_slash "$root"
    return
  fi

  [[ -n "${HOME:-}" ]] || die "HOME is not set"
  [[ "$HOME" == /* ]] || die "HOME must be absolute"
  if [[ -d "$HOME" ]]; then
    canonical_existing_dir "$HOME"
    return
  fi
  trim_trailing_slash "$HOME"
}

ROOT=$(resolve_root)
TOOL_PATH="$ROOT/.local/bin/agentbus"

tools_requested() {
  [[ "$TARGET" == "tools" || "$TARGET" == "all" ]]
}

claude_requested() {
  [[ "$TARGET" == "claude" || "$TARGET" == "all" ]]
}

codex_requested() {
  [[ "$TARGET" == "codex" || "$TARGET" == "all" ]]
}

claude_skills_root() {
  printf '%s\n' "$ROOT/.claude/skills"
}

codex_skills_root() {
  if [[ -n "$INSTALL_ROOT_ARG" ]]; then
    printf '%s\n' "$ROOT/.codex/skills"
    return
  fi

  local codex_home=${CODEX_HOME:-}
  if [[ -n "$codex_home" ]]; then
    [[ "$codex_home" == /* ]] || die "CODEX_HOME must be absolute"
    codex_home=$(trim_trailing_slash "$codex_home")
    printf '%s\n' "$codex_home/skills"
    return
  fi
  printf '%s\n' "$ROOT/.codex/skills"
}

set_skill_dirs_for_target() {
  local target=$1
  case "$target" in
    claude|codex)
      SKILL_DIRS=(agentbus)
      ;;
    *)
      die "unsupported skill target $target"
      ;;
  esac
}

skill_root_for_target() {
  case "$1" in
    claude) claude_skills_root ;;
    codex) codex_skills_root ;;
    *) die "unsupported skill target $1" ;;
  esac
}

skill_file_path() {
  local target=$1
  local escaped=$2
  local root
  root=$(skill_root_for_target "$target")
  printf '%s\n' "$root/$escaped/SKILL.md"
}

install_skills_for_target() {
  local target=$1
  local escaped src dest
  set_skill_dirs_for_target "$target"
  for escaped in "${SKILL_DIRS[@]}"; do
    src="$REPO_ROOT/skills/$escaped/SKILL.md"
    [[ -f "$src" ]] || die "skill source not found: $src"
    dest=$(skill_file_path "$target" "$escaped")
    mkdir -p -- "$(dirname -- "$dest")"
    cp -- "$src" "$dest"
    chmod 0644 "$dest"
  done
}

uninstall_skills_for_target() {
  local target=$1
  local escaped path
  set_skill_dirs_for_target "$target"
  for escaped in "${SKILL_DIRS[@]}"; do
    path=$(skill_file_path "$target" "$escaped")
    rm -rf -- "$(dirname -- "$path")"
  done
}

sha256_file() {
  local path=$1
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$path" | awk '{print $1}'
    return
  fi
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$path" | awk '{print $1}'
    return
  fi
  die "sha256 tool not found; install shasum or sha256sum"
}

build_agentbus() {
  local output=$1
  local err_file
  err_file=$(mktemp "${TMPDIR:-/tmp}/agentbus-build.XXXXXX")
  rm -f -- "$output"
  mkdir -p -- "$(dirname -- "$output")"

  if (
    cd -- "$REPO_ROOT"
    go build -mod=readonly -trimpath -ldflags "-X main.version=$VERSION" -o "$output" ./cmd/agentbus
  ) 2>"$err_file"; then
    rm -f -- "$err_file"
    return
  fi

  # Air-gapped installs resolve through a pre-populated Go module cache. Keep
  # this fallback only for source trees that still carry a vendor/ directory.
  if [[ -d "$REPO_ROOT/vendor" ]]; then
    add_warning "go build -mod=readonly failed; used legacy -mod=vendor fallback; air-gapped installs should pre-populate GOMODCACHE"
    if (
      cd -- "$REPO_ROOT"
      go build -mod=vendor -trimpath -ldflags "-X main.version=$VERSION" -o "$output" ./cmd/agentbus
    ) 2>>"$err_file"; then
      rm -f -- "$err_file"
      return
    fi
  fi

  printf 'agentbus installer: go build failed\n' >&2
  sed 's/^/go build: /' "$err_file" >&2
  rm -f -- "$err_file"
  exit 1
}

live_install_root() {
  [[ -n "${HOME:-}" && "$HOME" == /* && -d "$HOME" ]] || return 1
  canonical_existing_dir "$HOME"
}

restart_live_daemon() {
  [[ "$OPERATION" == "install" ]] || return 0
  tools_requested || return 0

  local live_root
  live_root=$(live_install_root) || return 0
  # mise-en-place invokes delegated installers with a temporary
  # --install-root, then copies the staged file into the live destination.
  # Signalling from that staged invocation would target the wrong binary.
  [[ "$ROOT" == "$live_root" ]] || return 0

  local pids pid command matched=0
  pids=$(pgrep -f '[a]gentbus[[:space:]]+serve([[:space:]]|$)' 2>/dev/null || true)
  for pid in $pids; do
    [[ "$pid" =~ ^[0-9]+$ ]] || continue
    command=$(ps -p "$pid" -o command= 2>/dev/null || true)
    command=${command#"${command%%[![:space:]]*}"}
    if [[ "$command" != "$TOOL_PATH serve" && "$command" != "$TOOL_PATH serve "* ]]; then
      continue
    fi
    matched=1
    if kill -TERM "$pid" 2>/dev/null; then
      add_warning "restarted running agentbus daemon (pid $pid) so the upgraded binary serves new connections; any in-flight jobs are interrupted by the restart and reconciled (not resumed) by the reaper — re-submit them if needed"
    else
      add_warning "could not restart running agentbus daemon (pid $pid) after upgrade"
    fi
  done
  if [[ $matched -eq 0 ]]; then
    add_warning "no running agentbus daemon found at $TOOL_PATH"
  fi
}

TOOL_SHA=""

case "$OPERATION" in
  plan)
    ;;
  install)
    if tools_requested; then
      command -v go >/dev/null 2>&1 || die "go executable not found"
      build_agentbus "$TOOL_PATH"
      TOOL_SHA=$(sha256_file "$TOOL_PATH")
      [[ -n "$TOOL_SHA" ]] || die "failed to compute sha256 for $TOOL_PATH"
      restart_live_daemon
    fi
    if claude_requested; then
      install_skills_for_target claude
    fi
    if codex_requested; then
      install_skills_for_target codex
    fi
    ;;
  uninstall)
    if tools_requested; then
      rm -f -- "$TOOL_PATH"
    fi
    if claude_requested; then
      uninstall_skills_for_target claude
    fi
    if codex_requested; then
      uninstall_skills_for_target codex
    fi
    ;;
  *)
    die "unsupported operation: $OPERATION"
    ;;
esac

print_warnings() {
  printf '[%s]' "$WARNINGS_JSON"
}

print_tool_file() {
  printf '{"path":"%s"' "$(json_escape "$TOOL_PATH")"
  if [[ -n "$TOOL_SHA" ]]; then
    printf ',"sha256":"%s"' "$(json_escape "$TOOL_SHA")"
  fi
  printf '}'
}

print_target() {
  local name=$1
  printf '"%s":{"files":[' "$(json_escape "$name")"
  if [[ "$name" == "tools" ]]; then
    print_tool_file
  else
    local escaped path sha first=1
    set_skill_dirs_for_target "$name"
    for escaped in "${SKILL_DIRS[@]}"; do
      path=$(skill_file_path "$name" "$escaped")
      sha=""
      if [[ "$OPERATION" == "install" && -f "$path" ]]; then
        sha=$(sha256_file "$path")
      fi
      if [[ $first -eq 0 ]]; then
        printf ','
      fi
      first=0
      printf '{"path":"%s"' "$(json_escape "$path")"
      if [[ -n "$sha" ]]; then
        printf ',"sha256":"%s"' "$(json_escape "$sha")"
      fi
      printf '}'
    done
  fi
  printf ']}'
}

print_targets() {
  local names=()
  local first=1
  local name
  case "$TARGET" in
    all) names=(tools claude codex) ;;
    *) names=("$TARGET") ;;
  esac
  printf '{'
  for name in "${names[@]}"; do
    if [[ $first -eq 0 ]]; then
      printf ','
    fi
    first=0
    print_target "$name"
  done
  printf '}'
}

printf '{"schema":%s,"name":"%s","version":"%s","operation":"%s","kind":"delegated"' \
  "$SCHEMA" "$(json_escape "$NAME")" "$(json_escape "$VERSION")" "$(json_escape "$OPERATION")"

if tools_requested; then
  printf ',"setup":[{"kind":"executable","executable":"go","remediation":"Install Go and ensure go is on PATH before installing agentbus."}]'
fi

printf ',"targets":'
print_targets
printf ',"warnings":'
print_warnings
printf '}\n'
