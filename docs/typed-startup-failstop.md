# Typed Startup Fail-Stop

When foreground daemon startup is refused before the Unix socket opens, `agentbus serve --foreground` exits with a stable authority-refusal code: `15` for a fail-stopped authority root and `16` for a sealed authority root. Exit code `14` is reserved for orphaned jobs, and these foreground startup codes are distinct from the existing connected-client authority failure exit code.

Client autostart does not classify this path from process exit status. The daemon reports cold-start refusal through the launch readiness handshake by writing a Failed frame. `internal/daemonlaunch` returns that frame as `*daemonlaunch.StartupError` with code `agentbus authority root fail-stopped` or `agentbus authority root sealed`; the client maps those codes to `*client.StartupRefusedError` matching `client.ErrRootFailStopped` or `client.ErrRootSealed`. Other startup-failure codes keep the existing startup error behavior.
