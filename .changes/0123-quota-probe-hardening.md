---
type: fix
scope: daemon
user_visible: true
breaking: false
user_summary: "Quota probes can no longer be starved by mail-wake cooldowns, a parked task whose owner is still working is held+escalated instead of released to lease expiry, and every real park is now durable-journaled with its original marker."
dev_summary: "db.quota_park commits a durable `quota_park` decision_events row (reset, source, marker note) atomically with the CAS for real parks; the daemon quota pass paces control probes on a dedicated WakePolicy instance; give-up past the defer limit on a verifiably-busy owner re-parks a bounded hold (`SESSIONS_BRIDGE_QUOTA_BUSY_HOLD_SECONDS`, default 3600s, source `busy-hold`) and escalates once per episode to the orchestrator as a `[QUOTA_PROBE]` notice (notify-send fallback), bounded at `_QUOTA_BUSY_HOLD_LIMIT` (3) holds so a wedged turn cannot park forever; defer re-parks preserve the original `quota_note`; `quota_signal_seen` also lands in decision_events."
why: "Task 249 postmortem of the task 224 misclassification: the park journal lived only in the daemon jsonl log so decision_events could never explain the episode; the shared 300s wake cooldown starved all 10 control probes (`paced` without a single attempt); and gave_up folded a probe-delivery problem into a worker failure on a verifiably mid-turn session (lease expiry -> attempts exhausted -> failed)."
refs: issue #123
---

## Spec impact
- adds: docs/specs/quota.md#every-real-park-is-durable-journaled-with-its-marker
- adds: docs/specs/quota.md#give-up-on-a-busy-owner-holds-and-escalates
- modifies: docs/specs/quota.md#reset-releases-the-park-with-one-control-probe
