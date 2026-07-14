# Agentbus AB-D ADR Index

These records summarize the frozen AB-D reliable-admission decisions from the implementation spec.
They are constraints for the executable lifecycle model and later production units.

| ADR | Decision |
| --- | --- |
| [ADR-0](ADR-0.md) | Reliable admission is exactly-once by request key and fail-closed for execution. |
| [ADR-0B](ADR-0B.md) | Replays use a stable client-supplied workspace key and recorded fingerprint version. |
| [ADR-1](ADR-1.md) | Fenced coordinator admissions use one atomic root AdmissionStore. |
| [ADR-1A](ADR-1A.md) | Whole-store integrity failures are fatal and never silently recreated. |
| [ADR-1B](ADR-1B.md) | An external admission-authority anchor detects missing, mismatched, or rolled-back DBs. |
| [ADR-1C](ADR-1C.md) | Corrupt aggregate values preserve request bindings and quarantine or fail visibly. |
| [ADR-1D](ADR-1D.md) | AdmissionStore contains durable state transitions; coordinator-only phase tracking stays outside it. |
| [ADR-2](ADR-2.md) | The current-boot coordinator owns OS effects; the store owns durable CAS transitions. |
| [ADR-2A](ADR-2A.md) | Coordinator obligations are linearized before commit and the coordinator is fail-stop. |
| [ADR-2B](ADR-2B.md) | Submission modes are IdentifiedFenced, LegacyFenced, and LegacyUnfenced. |
| [ADR-3](ADR-3.md) | After acceptance, failures become durable job states, not application-level rejection. |
| [ADR-4](ADR-4.md) | Reads route through bbolt first with historical JSON dual-read during migration. |
| [ADR-5](ADR-5.md) | Fenced attempts run through a parked exec worker and monitor. |
| [ADR-5A](ADR-5A.md) | Live supervisor loss is reconciled by the daemon before terminalization. |
| [ADR-5B](ADR-5B.md) | GroupRef identity and containment proof fence PID reuse and death races. |
| [ADR-6](ADR-6.md) | Permit and cancel are mutually exclusive durable CAS decisions. |
| [ADR-6A](ADR-6A.md) | Corrective launches use one-use launch ordinals and terminal proofs. |
| [ADR-6B](ADR-6B.md) | Terminal proof requires supervisor retirement. |
| [ADR-7](ADR-7.md) | Startup reconciliation is a hard socket barrier. |
| [ADR-8](ADR-8.md) | Result publication has durable ordering and no historical result migration. |
| [ADR-8A](ADR-8A.md) | Result cleanup excludes live, publishing, and nonterminal jobs. |
| [ADR-9](ADR-9.md) | Legacy job.status returns to global listing. |
| [ADR-10](ADR-10.md) | Coordinator ownership blocks idle shutdown and binary replacement. |
