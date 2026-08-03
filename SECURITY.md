# Security Policy

## Supported versions

Security fixes are applied to the latest released version. Older tags are not
maintained. Please upgrade to the most recent release before reporting.

## Reporting a vulnerability

Please report suspected vulnerabilities **privately** — do not open a public
issue or pull request for a security problem.

- Preferred: use GitHub's private vulnerability reporting on this repository's
  **Security** tab → **Report a vulnerability**.
- Alternatively, email `charles.anderson@npxinnovation.ca` with details and, if
  possible, a minimal reproduction.

Please include the affected version or commit, the impact, and reproduction
steps. You can expect an acknowledgement within a reasonable timeframe; fixes
are released as new tagged versions once validated.

## Scope

agentbus is a supervised local execution runtime. It assumes a same-user trust
boundary: a backend it launches runs with the invoking user's own permissions
and can read files that user can read. Its sanitization heuristics
(secret-path and content redaction) are accident-prevention aids, not an
adversarial security boundary against a hostile backend. Reports that depend on
already having the user's local privileges are considered out of scope.
