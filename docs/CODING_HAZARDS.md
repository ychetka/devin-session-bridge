# CODING HAZARDS — universal reliability rules

Shared source of truth for writer, reviewer and judge. Rules are
language-neutral: apply them through the project's own stack, idioms
and native tooling. If a rule genuinely cannot apply to the stack,
state that explicitly instead of skipping it silently.

Load this file when the change touches any trigger area:

- event loops, async code, callbacks, timers or scheduled jobs
- calls to external systems: network, APIs, databases, message buses,
  subprocesses, file I/O on hot paths
- persistence, transactions, schema or data migrations
- money, prices, quantities or any numeric precision-critical math
- timestamps, scheduling, durations, time comparisons
- queries, caches, unbounded collections, resource lifecycles
- error handling, retries, cancellation paths
- access control, ownership checks, authentication
- secrets, credentials, personal data, logging of sensitive fields
- new external APIs, libraries or dependencies

## The rules

1. **Nonblocking event handling.** Never run blocking/synchronous
   work (network calls, file I/O, sleeps, heavy CPU) inside event
   loops, async functions or event handlers. Offload to the stack's
   mechanism (worker pool, thread, queue) or make the call properly
   nonblocking.
2. **Bounded external deadlines.** Every call that can block on an
   external party carries an explicit timeout or deadline. An
   unbounded wait is a defect.
3. **Bounded, justified retries.** Retry only operations that are
   safe to repeat (idempotent or explicitly retryable). Retries are
   bounded in count, use backoff, and target retryable error classes
   only. No retry loops without an exit.
4. **Transaction and isolation correctness.** Writes that must
   succeed or fail together run in one atomic transaction. No calls
   to external systems inside an open transaction. External writes
   are idempotent or carry a reconciliation/repair path. Isolation
   assumptions are stated where read-modify-write races are possible.
5. **Exact financial arithmetic.** Money, prices and quantities use
   exact types — decimal or integer smallest-units. Binary
   floating-point for financial values is a defect.
6. **Timezone-explicit timestamps.** Stored and compared timestamps
   are timezone-aware; the canonical zone (default UTC) is explicit.
   Naive or implicitly-local timestamps are a defect.
7. **Bounded fanout and resource lifetime.** Queries and read paths
   are limited; caches and long-lived collections have caps and
   eviction; handles, connections, timers, tasks and subscriptions
   are released or cancelled on every path including errors.
8. **No swallowed errors or cancellation.** Errors are handled
   deliberately or propagated with context. Empty catch/except
   blocks, ignored results and conversions of cancellation into
   success are forbidden.
9. **Ownership checks.** Access keyed by an identifier (user,
   tenant, resource id) verifies the caller owns or is authorized
   for that specific resource. Authentication alone is not
   authorization (BOLA/IDOR).
10. **No secrets or PII.** No credentials, tokens, keys or personal
    data in code, tests, fixtures, logs, error messages or
    screenshots. Reference secrets by name, never by value.
11. **Verified APIs and provenance.** Call only APIs whose real
    signature you have read in the codebase or vendor sources. Add a
    dependency only after verifying its exact name, registry and
    version against an authoritative source — a plausible-looking
    name is not proof of existence (slopsquatting). Record the
    evidence in the report.
