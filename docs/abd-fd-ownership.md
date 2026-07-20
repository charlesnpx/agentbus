# AB-D FD Ownership Matrix

R2A is contract-only. R2B/R3B enforce these with real descriptor operations.

| State | Daemon | Monitor | Worker | Backend |
| --- | --- | --- | --- | --- |
| preparing | Creates control/bootstrap/stdio parent ends; marks control write CLOEXEC for unrelated daemon children. | Not bound yet. | Not bound yet. | Not started. |
| prepared | Owns control write-end, stdio parent ends, and the durable `GroupRef` binding if committed. | Owns only monitor/control handles needed to observe the target group. | Owns parked control read-end and stdio child ends; has not exec'd backend. | Not started. |
| releasing | Writes the one release frame containing the custodian `ReleaseSecret`; never sends the authority grant nonce. | Keeps containment observation independent of release ack. | Validates release secret and strips control metadata before backend exec. | Not started until release acceptance; execution may be possible once frame write begins. |
| running | Owns stdin writer and stdout/stderr readers so EOF remains observable. | Owns containment handle/proof path only. | Replaced by backend. No control FDs survive exec. | Owns stdin reader and stdout/stderr writers only; no control metadata, bootstrap FD, or daemon control write-end. |
| finalized | Closes custody FDs after abort, contain, or verified wait. | Releases monitor resources after quiescence proof. | Gone or proven absent. | Gone or proven absent. |

Rules:
- Authority owns the logical grant nonce; custodian owns the physical release secret.
- `ReleaseDefinitelyNotSent` permits abort because no release frame crossed the worker boundary.
- `ReleaseOutcomeUnknown` assumes execution is possible, contains by `GroupRef`, and never resends.
- Daemon child launches unrelated to held launch must not inherit the held-launch control write-end.
- Backend exec strips park/control metadata and preserves only backend stdio descriptors.
