# ADR-15: Explicit intent and format-2 ownership

The coordinated release uses protocol 4 with exact version matching and fresh
store format 2. There is no compatibility or data-import layer. A canonical
state-root owner and boot, directory, and PID-namespace scoped process claims
guard recovery against accidentally copied or stale claims.

Every task and turn supplies inspect or edit intent. Both intents use one host
execution policy, with an instruction prefixed once at the adapter boundary.
Inspect is a work instruction, not an enforcement or credential boundary.
The caller owns workspace isolation for concurrent editing.

Agentbus uses normal inherited tool caches, temporary paths, and networking.
It does not create private build caches, frozen workspaces, or IPC profiles.
Daemon Codex conversations have owned homes, separate from normal tool caches.
The coordinated lifecycle design retains conversations until their eligibility
ends or an operator explicitly disposes of them; diagnostic retention applies
to logs rather than provider assets. That lifecycle work is integrated with the
remaining release components.

Invocation selection is committed with the starting marker before launch.
Requested values remain in the immutable task; reported values are independent
provider observations.
