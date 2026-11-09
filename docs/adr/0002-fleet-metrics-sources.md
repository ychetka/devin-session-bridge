# ADR-0002: fleet-metrics data sources and honest approximations

- Status: accepted
- Date: 2026-11-09
- Refs: issue #98, directive 446

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
command rendering a markdown report to stdout:

- Per-session: task counts (window-filtered by `created_at`, in-flight
  is a current snapshot), `plan_delta`-kind decision_events,
  `mail_stall` episode rows, questions asked, and median
  `updated_at - created_at` for done tasks (labeled as cycle time -
  the schema records no separate dispatch stamp, so the interval
  includes queue time before dispatch).
- Per-repo (unique session `cwd` values = the session-to-repo
  registry): incidents grouped by `failure_class`,
  `verify_constraints` failures, `escalated` verdicts, and a
  review-rounds proxy - fix-round actions (`commit`/`push` verbs) per
  cycle_log `item`, reported as avg/max.
- Orchestrator: global median dispatch-to-done latency, current
  hungry-worker snapshot (managed+active sessions with no live task),
  total `escalated` verdicts, mean `answered_at - asked_at` across
  answered questions.
- Incident log: all `failure`/`escalated` cycle_log rows in window as
  a flat table.

Metrics with no instrumentation anywhere (reviewer overturns,
historical hungry-worker episodes) are reported as their closest
honest proxy or omitted - never synthesized.

## Consequences

- The command never writes: no schema change, no daemon pass, no
  notifications - safe to run on the live state db.
- Approximations are labeled in the report and this ADR so the
  gate-architecture review consumes facts, not embellished numbers.
- If dedicated instrumentation lands later (overturn verdicts,
  per-pass hungry snapshots), the same command surface can source
  them without format changes.

## Alternatives considered

- Extending the schema (dispatch stamps, overturn event kinds) first:
  deferred - the directive asks for a report over existing data;
  schema extensions belong to the instrumentation backlog that this
  report helps justify.
- JSON-only output: rejected - the directive's deliverable is the
  markdown report itself; `--json` additionally returns the raw
  aggregates for scripting.
