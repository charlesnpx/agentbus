#!/usr/bin/env bash
set -euo pipefail

# Copy this script to another repository and change only this list.
CONSUMERS=(
  "charlesnpx/delegate@main"
  "charlesnpx/convo-relay@main"
)

MODULE="github.com/charlesnpx/agentbus"
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
STAGE=$(mktemp -d "${TMPDIR:-/tmp}/agentbus-consumers.XXXXXX")
trap 'rm -rf -- "$STAGE"' EXIT

failed=0
for spec in "${CONSUMERS[@]}"; do
  repo=${spec%@*}
  ref=${spec##*@}
  name=${repo##*/}
  dir="$STAGE/$name"
  log="$STAGE/$name.log"

  printf 'consumer-check: checking %s@%s\n' "$repo" "$ref"
  if git clone --depth 1 --branch "$ref" "https://github.com/$repo.git" "$dir" >"$log" 2>&1 &&
    (
      cd "$dir" &&
      go mod edit "-replace=$MODULE=$ROOT" &&
      go mod tidy &&
      go build ./... &&
      go vet ./...
    ) >>"$log" 2>&1; then
    printf 'consumer-check: ok %s@%s\n' "$repo" "$ref"
  else
    printf 'consumer-check: FAILED %s@%s\n' "$repo" "$ref" >&2
    tail -n 40 "$log" >&2
    failed=1
  fi
done

if ((failed)); then
  exit 1
fi
