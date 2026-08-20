# Proposed GitHub Actions workflows

These workflow files are staged here for review and manual installation. After explicit approval to write the hidden GitHub workflow directory, copy the selected `*.yml` files to `.github/workflows/`.

## Workflows

- `test.yml`: PR/push default gate. Runs `scripts/ci/solo-battery.sh` with the race option enabled, so the same committed script performs gofmt, build, vet, `go test ./...`, full `go test -race ./...`, Linux amd64/arm64 cross-builds, and Windows/Darwin embedded-engine cross-builds.
- `vuln.yml`: Go vulnerability scan. Runs `scripts/ci/vuln.sh`, which installs `govulncheck` at the pinned version from the workflow env and scans `./...`.

All workflow actions are pinned to full commit SHAs, and all Go lanes pin `GO_VERSION` to `1.26.0`.

## Merge criterion

Merge only after GitHub Actions billing is restored and every required PR/push check is green on the exact candidate SHA.
