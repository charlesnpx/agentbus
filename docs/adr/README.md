# Agentbus ADR Index

These records summarize the evolution of the reliable-admission decisions from
the implementation spec through the AB-E hardening units. ADR-14 is the sole
normative contract. Every entry marked superseded is historical only.

| ADR | Decision |
| --- | --- |
| [ADR-0](ADR-0.md) | Superseded by ADR-14: durable admission authority is replaced by a generic identified job service. |
| [ADR-0B](ADR-0B.md) | Superseded by ADR-14: compound identity remains, but recorded fingerprint-version replay is deleted. |
| [ADR-1](ADR-1.md) | Superseded by ADR-14: AdmissionStore and fenced coordinator admission are replaced by one bbolt job store. |
| [ADR-1A](ADR-1A.md) | Superseded by ADR-14: authority-root integrity semantics are replaced by typed pre-bind bbolt corruption errors. |
| [ADR-1B](ADR-1B.md) | Superseded by ADR-14: the external admission-authority anchor is deleted. |
| [ADR-1C](ADR-1C.md) | Superseded by ADR-14: aggregate quarantine and authority preservation semantics are deleted. |
| [ADR-1D](../history/ADR-1D.md) | AdmissionStore contains durable state transitions; coordinator-only phase tracking stays outside it. |
| [ADR-2](ADR-2.md) | Superseded by ADR-14: direct job-service launch replaces current-boot coordinator ownership. |
| [ADR-2A](ADR-2A.md) | Superseded by ADR-14: coordinator linearization and fail-stop are deleted with durable authority. |
| [ADR-2B](../history/ADR-2B.md) | Superseded by ADR-12: only identified fenced submission exists in v2. Historically defined IdentifiedFenced, LegacyFenced, and LegacyUnfenced modes. |
| [ADR-3](ADR-3.md) | Superseded by ADR-14: one job record carries six states and nine failure classes. |
| [ADR-4](../history/ADR-4.md) | Superseded by ADR-12: reads are authority-only; the historical JSON dual-read migration path was deleted. |
| [ADR-5](ADR-5.md) | Superseded by ADR-14: parked workers and monitors are replaced by direct process groups. |
| [ADR-5A](ADR-5A.md) | Superseded by ADR-14: recovery terminalizes active jobs without relaunch. |
| [ADR-5B](ADR-5B.md) | Superseded by ADR-14: exact start-token equality replaces GroupRef custody proof. |
| [ADR-6](ADR-6.md) | Superseded by ADR-14: permit/cancel authority CAS is deleted with the authority subsystem. |
| [ADR-6A](ADR-6A.md) | Superseded by ADR-14: one read-only correction replaces corrective launch ordinals. |
| [ADR-6B](ADR-6B.md) | Superseded by ADR-14: cleanup is an independent clean-or-uncertain axis. |
| [ADR-7](ADR-7.md) | Superseded by ADR-14: startup recovers to failed or unknown and never relaunches. |
| [ADR-8](ADR-8.md) | Superseded by ADR-14: first-terminal-wins defines result publication. |
| [ADR-8A](ADR-8A.md) | Superseded by ADR-14: only large prompt, log, and result artifacts are garbage-collected. |
| [ADR-9](../history/ADR-9.md) | Superseded by ADR-12: job.status{all:true} lists authority jobs only; the legacy global listing was deleted. |
| [ADR-10](ADR-10.md) | Superseded by ADR-14: coordinator ownership is deleted with durable admission authority. |
| [ADR-11](ADR-11-admission-authority.md) | Superseded by ADR-14: AdmissionAuthority is deleted; Agentbus is a generic job service. |
| [ADR-12](ADR-12-strict-only-contract.md) | Superseded by ADR-14: protocol v3 replaces the strict-only authority contract. |
| [ADR-13](ADR-13-custody-deescalation.md) | Superseded by ADR-14: its deferred parked-worker deletion is completed; cgroup custody is deleted. |
| [ADR-14](ADR-14-simplified-core.md) | Normative simplified core: one bbolt job service, four RPC methods, six public states, inline schema validation, and process-group supervision. |
