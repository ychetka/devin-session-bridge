---
type: feat
scope: tooling
user_visible: false
breaking: false
user_summary: "-"
dev_summary: "Living spec pilot: `.changes` entries now carry a required `## Spec impact` section (adds/modifies/removes anchors into docs/specs), docs/specs/ seeds the requirement registry for mailbox/tasks/wake/sessions/quota in WHEN/THEN form, and tools/check_spec_anchors.sh (run by verify_constraints.sh) fails PRs whose anchors do not resolve post-change."
why: "Nothing answered 'how does the system behave now' - roadmap tracks work, docs drift; per the approved spec_lifecycle design each PR carries its own spec delta so behavior and documentation cannot diverge."
refs: task 226, issue #107, ADR-0004
---

## Spec impact
- adds: docs/specs/mailbox.md#notification-delivery-lands-a-queue-file
- adds: docs/specs/mailbox.md#drain-protocol-is-file-driven
- adds: docs/specs/mailbox.md#undrained-mail-escalates-once-per-episode
- adds: docs/specs/tasks.md#lifecycle-follows-the-declared-transitions
- adds: docs/specs/tasks.md#claims-are-lease-owner-cas-writes
- adds: docs/specs/tasks.md#leases-expire-on-the-configured-ttl
- adds: docs/specs/tasks.md#auto-renew-covers-busy-managed-sessions
- adds: docs/specs/tasks.md#external-waits-renew-the-lease-deliberately
- adds: docs/specs/wake.md#mailbox-files-wake-the-session
- adds: docs/specs/wake.md#wake-policy-bounds-retries
- adds: docs/specs/wake.md#quota-parked-tasks-suppress-wakes
- adds: docs/specs/wake.md#halt-suppresses-every-wake-leg
- adds: docs/specs/sessions.md#managed-sessions-are-orchestrator-owned
- adds: docs/specs/sessions.md#halt-pauses-the-fleet-semantically
- adds: docs/specs/sessions.md#kill-removes-the-session-not-its-work
- adds: docs/specs/sessions.md#stale-heartbeats-mark-sessions-dead
- adds: docs/specs/quota.md#quota-park-is-orthogonal-to-lifecycle
- adds: docs/specs/quota.md#auto-park-needs-an-explicit-limit-signal-plus-reset
- adds: docs/specs/quota.md#reset-releases-the-park-with-one-control-probe
- adds: docs/specs/quota.md#halt-is-senior-to-a-quota-reset
