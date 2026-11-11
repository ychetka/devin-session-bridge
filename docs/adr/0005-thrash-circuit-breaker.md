# ADR-0005: thrash circuit breaker on repeated task failures

- Status: accepted
- Date: 2026-11-12
- Refs: issue #153

## Context

`tasks.attempt` counts wedge folds only - lease expiry and dead-worker
requeues - and `bridge retry` resets it, so a task that repeatedly
fails its gate or tests cycles `finish --fail` > `retry` forever with
no bound visible to the bus. The failure mode is real work failing
repeatedly, not transport wedging: each cycle looks like a fresh,
healthy attempt budget.

## Decision

Track consecutive task-level failure outcomes in a second counter,
`tasks.fail_streak`, and refuse `bridge retry` while it is at or above
`SB_THRASH_LIMIT` (env-resolved once at import, default 3, `<=0`
disables the breaker). A trip is journaled atomically: a
`thrash_breaker` `decision_events` row on every observation, one
deduplicated open ESCALATION `audit_events` row
(`doa_trigger='thrash_breaker'`) per task, and a NOTICE to the
registered orchestrator when one exists. The manual unpark is
`bridge escalate reply` plus `bridge retry --force`, which skips the
guard and zeroes the streak in the same CAS write.

Streak bookkeeping: `finish --fail` and both terminal wedge folds
increment it, a `done` resets it, and a plain retry preserves it.
Operator verdicts (`kill`, `withdraw`) do not touch it.

## Consequences

- A thrashing task parks on a human escalation instead of burning
  cycles; the `bridge status` escalation banner surfaces it even with
  no orchestrator registered.
- The streak deliberately survives a plain retry - a requeue is not
  an outcome. Resetting it on retry would reproduce the exact
  infinite loop `attempt` already permits (retry is the only exit
  from `failed`, so the streak could never exceed 1).
- Below the threshold nothing changes: `retry` behaves exactly as
  before, and `SB_THRASH_LIMIT=0` removes the feature entirely.
- New schema column is additive via the existing idempotent
  `_TASK_COLUMN_MIGRATIONS` mechanism - no migration file, no
  rollback concern.

## Alternatives considered

- Bound `attempt` across retries: rejected - `attempt` semantics are
  wedge-fold accounting consumed by `MAX_ATTEMPTS`; folding failure
  outcomes into it would conflate two different counters and break
  the requeue budget contract.
- Reset `fail_streak` on every successful retry (literal reading of
  the original task text): rejected - the breaker could never trip
  because retry is the only exit from `failed`; the streak would cap
  at 1 and the loop would stay infinite.
- Daemon-side detection of repeated finish--fail cycles: rejected -
  a passive monitor races the CLI actors; enforcing inside the retry
  CAS makes the refusal atomic and crash-safe.
