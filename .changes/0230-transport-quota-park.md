type: fix
scope: daemon,cli
user_visible: true
breaking: false
user_summary: "Rate-limit errors surfacing through wake/prompt transport now park the affected task as waiting_quota instead of burning the retry streak or failing it outright."
dev_summary: "daemon._wake_pass and cli._acp_failure sniff rpc_error text through parse_quota_signal and CAS the session's live task into waiting_quota (sources wake/acp-<phase>); a marker without a reset parks for the bounded DEFAULT_PARK_SECONDS=900 window, and a quota signal with no live task journals quota_signal_seen. quota.py gains the 'reset in N <unit> (at HH:MM UTC)' combined form where the provider's absolute time wins."
why: "Live incident 2026-11-09: a fleet-wide free-tier rate limit reached the bridge as 'rpc_error - session/prompt: Reached free model rate limit ... reset in 58 minutes (at 22:01 UTC)' - the marker and reset were in the body but classification stopped at rpc_error, so parked nothing and burned wake retries plus prompt attempts against a dead pool. The transport layer is the earliest surface that sees the provider's text, so it now feeds the same conservative parser the self-report channel uses; the 900s default window for marker-without-reset mirrors free-tier windows and self-heals via the quota pass re-probe."
refs: "issue #111, task 230"
---

## Spec impact
- modifies: docs/specs/quota.md#auto-park-needs-an-explicit-limit-signal-plus-reset
