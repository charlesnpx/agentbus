# Fresh deployment: Agentbus 0.18

Agentbus 0.18.0, Delegate 0.15.0, and the updated Convo Relay form a
coordinated release. Agentbus uses protocol 4 and fresh store format 2. There
is no migration command or import of earlier jobs, request IDs, results, or
sessions; an older binary is not guaranteed to understand format-2 state.
Existing credentials and provider configuration are deliberate inputs for the
new deployment, not a source of old job history.

1. Build and validate the exact coordinated candidates. Record their revisions,
   selected state root, Agentbus and provider executable paths, configuration
   source, and intended model and effort defaults. Check each consumer against
   the candidate Agentbus API before release; do not add compatibility shims
   for an unupdated consumer.
2. Stop the previous daemon and its jobs before switching. Resolve any surviving
   provider processes separately: starting a new daemon does not retire them.
3. Choose the new default root, `$XDG_STATE_HOME/agentbus/v2` or
   `~/.local/state/agentbus/v2`, or set `AGENTBUS_STATE_ROOT` to an explicitly
   chosen fresh root. Do not point format 2 at an earlier database. If earlier
   state will be archived or removed, identify and authorize that action
   separately; it is not performed by the runtime.
4. Install and use the coordinated versions. A staged installer does not restart
   the daemon. Start the new daemon, then use `agentbus version --json` and
   `agentbus backends --json` to check the client release and the connected
   daemon's release, executable, protocol, and backend configuration. Confirm
   the fresh store opens at the intended canonical root and that its format-2
   owner metadata matches that root; the CLI has no store-metadata display, so
   inspect a stopped copy with suitable read-only tooling when direct metadata
   evidence is required. Startup rejects unsupported or mismatched ownership.
   To check a stopped scratch daemon without starting it, set
   `AGENTBUS_NO_AUTOSTART=1` and the intended scratch `AGENTBUS_STATE_ROOT`, run
   a client command, and confirm it fails without starting a daemon. Explicit
   `agentbus serve` still starts one.
5. Qualify the supported hosts and providers with their actual binaries,
   credentials, workspaces, and relevant repository gates. Exercise inspect,
   ordinary caches and local services, repeated and concurrent work in separate
   workspaces, retained Codex continuation and disposal, process cleanup,
   linked-worktree staging and Go VCS stamping, directed evidence, and daemon
   handoff. Record versions, commands, exit statuses, tests actually executed,
   and results. Application-specific access, network, watcher limits, and
   provider failures require their own evidence; a generic smoke test does not
   establish that every such issue is fixed.

**The new default v2 directory is nested inside the previous default
`agentbus` directory.** Archive or remove that parent only before creating the
new v2 state, or target explicitly identified old content without touching the
new child. Removing the parent after v2 is in use also removes the new state.

Tagging, publication, installed hidden-file changes, and old-state removal are
separate delivery actions. This guide performs none of them.
