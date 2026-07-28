# Proposed GitHub Actions workflows

These workflow files are staged here for review and manual installation. After explicit approval to write the hidden GitHub workflow directory, copy the selected `*.yml` files to `.github/workflows/`.

## Workflows

- `test.yml`: PR/push default gate. Runs `scripts/ci/solo-battery.sh` with the race option enabled, so the same committed script performs gofmt, build, vet, strict-tag vet, test sweep, full test pass, full `go test -race ./...`, and Linux amd64/arm64 cross-builds.
- `strict-lane.yml`: privileged Linux cgroup-v2 lane. Runs `scripts/ci/docker-cgroup-v2.sh`, which starts `golang:1.26-trixie@sha256:4ee9ffa999b4583ce281939cdff828763083610292f252279a0cee77473bd9a7` with `--privileged --cgroupns=private`, fails before test execution unless strict cgroup-v2 support is usable, partitions build/test/race/conformance work, and uploads the retained gate log artifact. Recommended only on ephemeral or isolated runners because privileged Docker exposes host-level attack surface.
- `product-e2e.yml`: Linux compiled-CLI product lane. Runs `scripts/ci/product-e2e.sh`, which enters the same strict-capable Docker shape, builds the CLI once, smokes the compiled binary, and runs the strict CLI-focused E2E tests against that exact binary through `AGENTBUS_E2E_PREBUILT_BINARY`.
- `fail-closed.yml`: custody platform lane. Runs `scripts/ci/fail-closed.sh` on macOS as a supported process-group custody product lane and on unprivileged Linux as a process-group fallback serving lane. The same script still keeps typed fail-closed coverage for genuine no-basic-supervision and incompatible contract cases.
- `vuln.yml`: Go vulnerability scan. Runs `scripts/ci/vuln.sh`, which installs `govulncheck` at the pinned version from the workflow env and scans `./...`.

All workflow actions are pinned to full commit SHAs, and all Go lanes pin `GO_VERSION` to `1.26.0`. The privileged Docker lanes pin the container image to `golang:1.26-trixie@sha256:4ee9ffa999b4583ce281939cdff828763083610292f252279a0cee77473bd9a7`, which records Docker Hub's `1.26-trixie` digest for Go `1.26.1` on Debian trixie.

## Merge criterion

Merge only after GitHub Actions billing is restored and every required PR/push check is green on the exact candidate SHA. The strict privileged cgroup-v2 gate is not a PR-label-gated check; the required strict evidence is the protected-branch `strict-lane.yml` run on `main` or `abd-authority`, or an explicit `workflow_dispatch` run, on the exact candidate SHA. Local Docker evidence is useful supplementary evidence, but it does not replace remote required strict evidence on the candidate SHA.
