# ADR-0003: waiting_quota is a durable step state, not a task status

- Status: accepted
- Date: 2026-11-09
- Refs: issue #103, task #216

## Context

A worker that hits a model rate limit used to die of infrastructure
causes: the lease expired while the provider's reset window was still
open, `recover_stale` folded the task, attempts burned, and at
`MAX_ATTEMPTS` the task failed for a reason that was never a task
failure. A second claimant could then pick the work up mid-step and
redo what the first claimant had already saved.

## Decision

Add an orthogonal `step_state` column to `tasks` (values:
`running`/`interrupted`/`waiting_quota`/`saved`/`paused`, NULL for no
parked step) plus `quota_reset_at`, `quota_note`, `quota_source`,
`quota_model`, `quota_checkpoint`. `waiting_quota` keeps the lifecycle
status and the claimant lease: the park, not the claimant, owns the
wait. `recover_stale`, `requeue_dead_worker` and the lease-renew pass
skip parked rows; the wake legs (file-pending and liveness probe) and
manual `bridge wake` suppress while `quota_reset_at` is in the future.
The daemon's quota pass clears expired parks, re-arms a full lease and
sends exactly one control probe; a still-limited provider re-parks via
self-report, extending the window. HALT is senior: parked tasks on
halted sessions sweep only after `bridge resume`.

Detection is dual-channel by spec: preferred structured ACP/transport
error code, fallback text parsing. Verified fact at implement time:
the ACP surface (`session_bridge.acp.contract`) exposes error
classes/reasons (rpc_error, timeout, eof, no_session, ...) but **no
structured rate-limit code** - provider limit text arrives embedded in
free-text notice/event bodies. The auto channel is therefore the
conservative text parser in `session_bridge.quota` (`marker AND
parseable reset` to park; marker alone is journaled but does not
park), and `bridge quota-park --reset-at` is the manual escape for
signals without a parseable window. `rpc_error`, `busy`, `timeout`,
`dead` and `lease expired` carry no quota marker and can never park.
If a structured quota error lands in the ACP contract later, it plugs
into the same `db.quota_park` call without touching the park/sweep
machinery.

## Consequences

- Quota waits no longer consume attempt budget or fold on lease
  expiry; the blocked step resumes in place after the reset.
- Duplicate nudges collapse structurally: an unparked episode is
  spent, so only a fresh self-report park can trigger a new probe.
- Terminal and requeue writers must all clear the step slot
  (`_STEP_CLEAR_SQL`); a stale `waiting_quota` on a queued task would
  suppress wakes it does not own.
- Non-quota transient errors stay on the existing retry/recovery
  paths - parking is never their outcome.

## Alternatives considered

- A new `TaskStatus.WAITING_QUOTA`: rejected - it would fork every
  live-status query, break the lease/claim CAS invariants, and lose
  the distinction between "task is running, step asleep" and a
  lifecycle state.
- Retry-with-backoff on the task row: rejected - it still burns
  attempts, and a reset window can be hours; sleeping inside the
  transport holds a worker slot for nothing.
- Wake-leg-only suppression (no probe-leg exclusion): rejected - a
  parked task would keep feeding the no_session dead-streak for a
  session that is deliberately not working.
