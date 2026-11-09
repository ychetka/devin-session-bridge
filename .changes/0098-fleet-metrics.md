---
type: feat
scope: cli
user_visible: true
breaking: false
user_summary: New read-only `bridge fleet-metrics [--window 24h|7d]`
  renders a per-session/repo/orchestrator markdown report from bridge
  state and repo cycle_logs.
dev_summary: Adds commands/metrics.py + a `fleet-metrics` subcommand
  aggregating tasks/questions/decision_events per session and each
  repo's .devin/cycle_log.jsonl (paths from session cwd registry)
  into a markdown report; orchestrator self-metrics and an incident
  table included.
why: The orchestrator needed a fleet-level fact report for
  gate-architecture review without sessions ever seeing it;
  cycle-time and incident data existed but had no single read path.
refs: issue #98, directive 446, ADR-0002
---