# Proposed GitHub Actions workflows

These workflow files are staged here for review and manual installation. After explicit approval to write the hidden GitHub workflow directory, copy the selected `*.yml` files to `.github/workflows/`.

## Workflows

- `test.yml`: PR/push default gate. Runs `scripts/ci/solo-battery.sh` with the race option enabled, so the same committed script performs gofmt, build, vet, strict-tag vet, test sweep, full test pass, full `go test -race ./...`, and Linux amd64/arm64 cross-builds.
- `strict-lane.yml`: privileged Linux cgroup-v2 lane. Runs `scripts/ci/docker-cgroup-v2.sh`, which starts `golang:1.26` with `--privileged --cgroupns=private`, partitions build/test/race/conformance work, and runs the strict production E2E battery. Recommended only on ephemeral or isolated runners because privileged Docker exposes host-level attack surface.
- `product-e2e.yml`: Linux compiled-CLI product lane. Runs `scripts/ci/product-e2e.sh`, which builds the CLI once, smokes the compiled binary, and runs the strict CLI-focused E2E tests.
- `fail-closed.yml`: unsupported-platform lane. Runs `scripts/ci/fail-closed.sh` on macOS for Darwin typed unsupported behavior and on unprivileged Linux for the strict-unsupported fail-closed path.
- `vuln.yml`: Go vulnerability scan. Runs `scripts/ci/vuln.sh`, which installs `govulncheck` at the pinned version from the workflow env and scans `./...`.

All workflow actions are pinned to full commit SHAs, and all Go lanes pin `GO_VERSION` to `1.26.0`. The privileged Docker lane pins the container image to `golang:1.26`.

## Merge criterion

Merge only after GitHub Actions billing is restored and every required check is green on the exact candidate SHA. The privileged cgroup-v2 lane's logs/artifacts should be retained with the release evidence. Local Docker evidence is useful supplementary evidence, but it does not replace remote required checks on the candidate SHA.
