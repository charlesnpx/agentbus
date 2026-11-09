# Linux per-path Unix socket access

## Current state

Linux `localIPC` currently sets Codex's `network.dangerously_allow_all_unix_sockets=true`. This allows a job to connect to any host Unix socket at a path it can read, except masked paths. The masks cover `/run/user/<uid>`, `/run/docker.sock`, `/var/run/docker.sock`, the SSH agent directory or its socket file when that directory is shared, and the Agentbus daemon socket and token file. Agentbus resolves mask paths before passing them to the sandbox and drops paths it cannot resolve. Abstract sockets, host loopback TCP and external network connections are blocked.

This was tested with Codex 0.167.0 and sccache 0.19.0 in a Docker lab running Debian bookworm on aarch64 with bubblewrap.

- **repro1:** The ordinary `--write` sandbox makes sccache fail at `bind(AF_UNIX, /tmp/sccacheXXXXXX/sock) = EPERM` in seccomp Restricted mode. The macOS-style per-path profile on Linux fails earlier at `socket(AF_UNIX) = EPERM` in ProxyRouted mode. TCP loopback bind works, while external connect is unreachable.
- **repro2:** Setting `network_access=true` makes Unix sockets work, but also allows external network access. That is not acceptable.
- **repro3:** With `dangerously_allow_all_unix_sockets=true`, binding in the IPC directory and workspace works. Binding in read-only paths fails with `EROFS`. Connecting to host path sockets such as `~/hostsock` and `/tmp/host.sock` works. Abstract sockets are refused in a separate network namespace. Host loopback TCP is refused, external TCP is unreachable, and writes outside the granted roots fail. `cargo test` through sccache passes, and no sccache process remains after the sandbox exits.
- **repro4:** Local crates in dev builds are not cacheable; sccache reports `incremental`. A cache hit requires a dependency crate or a release build.
- **repro5:** A deny on the Agentbus state directory blocks the daemon socket and token with `EACCES`. A deny on `/tmp` hides `/tmp` sockets with `ENOENT`, while a more specific writable IPC directory under `/tmp` remains usable.
- **Follow-up checks:** Denies on `/run/user/<uid>`, `/run/docker.sock` and the SSH agent directory block access with `EACCES` while IPC bind and `sccache --start-server` still work. An unmasked `~/other.sock` remains reachable. A deny path through a symlink, such as `/var/run/...`, makes bubblewrap fail at startup, so Agentbus resolves each path and drops unresolvable ones. File-level denies on `agentbus.sock` and `token` work while job scratch under the same state root remains writable. With the allow-all profile, the proxy returns HTTP 403 for outbound HTTP and direct connections fail at DNS.
- **End-to-end check:** A `delegate task --write --local-ipc` job in the same Docker lab, using Codex 0.167.0 with real authentication, ran `cargo test` through sccache and got a Rust cache hit on a second build into a fresh target directory. The masked sockets returned `EACCES`, the unmasked `~/other.sock` was reachable, abstract sockets and host loopback TCP were refused, and `curl https://example.com` was blocked. Two concurrent jobs got separate IPC directories; the canceled job's IPC directory was removed and the other job completed. Posture reported `localIPC: "all-unix-sockets"`.

## Why allow-all is needed

Codex's Linux seccomp ProxyRouted mode denies `socket(AF_UNIX)` unless `network.dangerously_allow_all_unix_sockets` is set. Linux ignores the per-path `network.unix_sockets` grants that work on macOS.

## Accepted risk

A Linux local-IPC job can connect to any readable host path socket that is not masked. The masks in place are `/run/user/<uid>`, `/run/docker.sock`, `/var/run/docker.sock`, the SSH agent directory or its socket file when that directory is shared, and the Agentbus daemon socket and token file. Gaps include sockets under the home directory, `/tmp`, or other parts of `/run`, as well as X11 or Wayland sockets and database sockets.

The sandbox protects one operator from agent mistakes. It does not separate users. A possible future change is to deny all of `/run` or `/tmp` and add specific writable paths. Those broader denies have not been tested for side effects and are not in place.

## Upstream requirement

Codex needs to allow AF_UNIX in seccomp while restricting `bind` and `connect` to granted paths. One possible approach is Landlock: `LANDLOCK_ACCESS_FS_MAKE_SOCK` controls bind, and Landlock's scoped Unix socket restrictions on kernels 6.12 or later with ABI 6 cover abstract sockets. Path-based `connect` would still need separate control. Another approach is a seccomp user-notify supervisor that checks `sun_path` on `bind` and `connect`.

## Trigger to revisit

Revisit this when a Codex release or configuration key honors per-path `network.unix_sockets` on Linux. To check a candidate release, run the existing capability probe with the per-path profile and without allow-all. Confirm that binding in the IPC directory succeeds and connecting to a host socket outside it fails. Codex release notes and the installed version can also show when support becomes available.

## Migration steps when it lands

1. Switch the Linux profile to per-path `unix_sockets={<ipcRoot>="allow"}`.
2. Remove `dangerously_allow_all_unix_sockets` from the Linux profile.
3. Change the Linux posture to `private-dir`.
4. Remove the Linux risk notes from README, `docs/protocol.md` and `docs/operations.md`.
5. Add a lab test proving host sockets outside the IPC directory are unreachable.

## Owner/status

Open. Tracked here until Codex supports it.
