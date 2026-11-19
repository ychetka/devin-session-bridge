---
type: chore
scope: canon
user_visible: false
breaking: false
user_summary: "-"
dev_summary: "untrack the deployed canonical harness: .windsurfrules, .devin/agents, .devin/scripts, .devin/config.json, controller-manager/, docs/specs/quota.md and install_hooks.sh leave the tree; .gitignore gains the canon block; deployed copies live under .devin/bin|agents|specs (canon_manifest v0.0.8)"
why: canon is orchestrator-owned and deployed per-workspace - tracking it in the repo double-sources it and check_setup.sh rejects a deployed tree whose harness paths are gitignored elsewhere.
refs: issue #181
---

## Spec impact
- removes: docs/specs/quota.md#quota-park-is-orthogonal-to-lifecycle
- removes: docs/specs/quota.md#auto-park-needs-an-explicit-limit-signal-plus-reset
- removes: docs/specs/quota.md#every-real-park-is-durable-journaled-with-its-marker
- removes: docs/specs/quota.md#reset-releases-the-park-with-one-control-probe
- removes: docs/specs/quota.md#give-up-on-a-busy-owner-holds-and-escalates
- removes: docs/specs/quota.md#halt-is-senior-to-a-quota-reset
