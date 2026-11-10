---
type: feat
scope: daemon
user_visible: true
breaking: false
user_summary: "dead managed sessions (incl. the orchestrator) now revive themselves when pending mail + session/list presence coincide - no more manual re-register after a tab restart"
dev_summary: "daemon resurrect leg: dead+managed sids with pending mail get a batched presence probe; liveness-marked rows CAS dead>active via db.revive_dead_managed (session_killed markers refuse); dead paths dual-journal into decision_events and the managed orchestrator dead edge gains notify-send parity."
why: "the deferred orchestrator_dead auto-recovery gap: a dead-flagged managed session stayed dead until someone manually re-registered - the exact human-in-the-loop the bus exists to remove. Demand-driven probing (mail pending) keeps the probe cheap and operator-kills senior."
refs: issue #115
---

## Spec impact
- adds: docs/specs/sessions.md#dead-managed-sessions-revive-on-proven-return
