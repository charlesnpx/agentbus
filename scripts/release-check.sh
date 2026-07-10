#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
VERSION=$(tr -d '[:space:]' <"$ROOT/VERSION")
TAG=${1:-${RELEASE_TAG:-}}

if [[ -z "$TAG" ]]; then
  TAG=$(git -C "$ROOT" describe --tags --exact-match 2>/dev/null || true)
fi

if [[ -z "$TAG" ]]; then
  printf 'release-check: no tag supplied and current commit is not exactly tagged\n' >&2
  printf 'usage: scripts/release-check.sh v%s\n' "$VERSION" >&2
  exit 2
fi

EXPECTED_TAG="v$VERSION"
if [[ "$TAG" != "$EXPECTED_TAG" ]]; then
  printf 'release-check: tag mismatch: got %s, want %s from VERSION\n' "$TAG" "$EXPECTED_TAG" >&2
  exit 1
fi

STAGE=$(mktemp -d "${TMPDIR:-/tmp}/agentbus-release-check.XXXXXX")
cleanup() {
  rm -rf -- "$STAGE"
}
trap cleanup EXIT

INSTALL_JSON=$("$ROOT/install-skill.sh" --install --target tools --json --install-root "$STAGE")
INSTALL_VERSION=$(printf '%s\n' "$INSTALL_JSON" | sed -n 's/.*"version":"\([^"]*\)".*/\1/p')
if [[ "$INSTALL_VERSION" != "$VERSION" ]]; then
  printf 'release-check: installer version mismatch: got %s, want %s\n' "$INSTALL_VERSION" "$VERSION" >&2
  exit 1
fi

BIN="$STAGE/.local/bin/agentbus"
if [[ ! -x "$BIN" ]]; then
  printf 'release-check: staged binary missing or not executable: %s\n' "$BIN" >&2
  exit 1
fi

CLI_JSON=$("$BIN" version --json)
CLI_VERSION=$(printf '%s\n' "$CLI_JSON" | sed -n 's/.*"version":"\([^"]*\)".*/\1/p')
if [[ "$CLI_VERSION" != "$VERSION" ]]; then
  printf 'release-check: CLI version mismatch: got %s, want %s\n' "$CLI_VERSION" "$VERSION" >&2
  exit 1
fi

printf 'release-check: ok tag=%s version=%s\n' "$TAG" "$VERSION"
