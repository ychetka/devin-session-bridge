# ADR-0002: fleet-metrics data sources and honest approximations

- Status: accepted
- Date: 2026-11-09
- Refs: issue #98, directive 446, PR #101 review r1

## Context

Directive 446 asks for a fleet-wide metrics report spanning per-session
counters, per-repo cycle data and orchestrator self-metrics. The
sources are heterogeneous: the bridge SQLite state (authoritative for
tasks/questions/decision_events) and each repo's
`.devin/cycle_log.jsonl` (agent-side ops log). Some requested metrics
have no direct instrumentation today - the report must show facts
without inventing data.

## Decision

`bridge fleet-metrics [--window 24h|7d]` (7d default) is a read-only
command rendering a markdown report to stdout. It skips `db.init()`
and opens the state file with `mode=ro`, so the no-write claim is
kernel-enforced (a missing file is an error, never an empty db
created as a side effect).

- Per-session: done/failed tasks window-filtered by `finished_at`
  (the terminal stamp added in this change - `updated_at` is bumped
  by post-completion writers like `set_task_note`/`mark_notified`, so
  it is not a completion stamp), in-flight as a current snapshot,
  exact-kind `plan_delta` decision_events, questions asked, and
  median `finished_at - created_at` cycle time. `mail_stall` decision
  events collapse into episodes: the daemon re-journals every
  `SB_STALL_REALERT_S` inside one stall, so events spaced at most one
  cadence apart count once.
- Per-repo (unique normalized session `cwd` values = the
  session-to-repo registry): incidents grouped by `failure_class`
  (failures and escalations both count; an escalated row without a
  class lands under `escalated`), `verify_constraints` failures,
  `escalated` verdicts, and a review-rounds proxy - exact-match
  fix-round actions (`commit`/`push`/`commit_push`) per cycle_log
  `item`, reported as avg/max. Repo keys are full paths internally;
  two repos sharing a directory basename never merge (display falls
  back to the full path on collision). cycle_log `ts` parses as
  UTC (`fromisoformat`), never host-local - mktime-based parsing
  shifts window boundaries by the host offset.
- Orchestrator: global median `finished_at - created_at` task cycle
  (enqueue→done - the schema has no separate dispatch stamp, so the
  interval includes queue time; labeled as such), current
  hungry-worker snapshot (managed+active sessions with no live task),
  total `escalated` verdicts, mean `answered_at - asked_at` across
  answered questions.
- Incident log: all `failure`/`escalated` cycle_log rows in window as
  a flat table.

A state.db created before `finished_at` existed and never re-init'ed
still reports: the command detects the missing column and degrades to
`updated_at` rather than erroring.

Metrics with no instrumentation anywhere (reviewer overturns,
historical hungry-worker episodes, true dispatch_decided→done) are
reported as their closest honest proxy or omitted - never
synthesized.

## Consequences

- The command never writes: `mode=ro` connection, no schema change,
  no daemon pass, no notifications - safe to run on the live state
  db. The new `finished_at` column and its stamps are written by the
  normal lifecycle writers (finish/abort/requeue-cap/retry), not by
  this command.
- `finished_at` becomes the terminal stamp for tasks: every writer
  landing a task in `done`/`failed` sets it, `retry` clears it, and
  init() backfills pre-existing terminal rows from `updated_at`
  (documented approximation).
- Approximations are labeled in the report and this ADR so the
  gate-architecture review consumes facts, not embellished numbers.
- If dedicated instrumentation lands later (overturn verdicts,
  per-pass hungry snapshots, real dispatch stamps), the same command
  surface can source them without format changes.

## Alternatives considered

- Reporting over `updated_at` only: rejected in review - post-
  completion writers (`set_task_note`, `mark_notified`, lease renewals)
  bump `updated_at`, re-entering old rows into windows and inflating
  durations; `finished_at` is the dedicated stamp.
- JSON-only output: rejected - the directive's deliverable is the
  markdown report itself; `--json` additionally returns the raw
  aggregates for scripting.
- Counting raw `mail_stall` events as episodes: rejected in review -
  the daemon's re-alert cadence journals repeatedly inside one stall,
  so raw counts correlate with stall length, not stall frequency.
