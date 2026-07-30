# Typed Startup Fail-Stop

When foreground daemon startup is refused before the Unix socket opens, `agentbus serve --foreground` exits with a stable authority-refusal code: `14` for a fail-stopped authority root and `15` for a sealed authority root. These codes are distinct from the existing connected-client authority failure exit code.

Client autostart observes child process exit when the configured `DaemonStarter` supplies `StartResult.Wait`. Exit code `14` is returned as `*client.StartupRefusedError` matching `client.ErrRootFailStopped`; exit code `15` matches `client.ErrRootSealed`. Starters that leave `StartResult.Wait` nil keep the previous connect-polling timeout behavior.
