bump: patch
user_visible: false
issue: 181

Canon untrack v0.0.8: the orchestrator-owned canonical harness stops
being tracked - `.windsurfrules`, `.devin/agents/`, `.devin/scripts/`,
`.devin/config.json`, `controller-manager/` and `docs/specs/quota.md`
leave the tree; deployed copies live under `.devin/bin|agents|specs/`
stamped by `.devin/canon_manifest.json`. `.gitignore` gains the
canonical-harness block (required by check_setup.sh on a deployed
tree). `install_hooks.sh` stays tracked - unit tests execute it from
the repo root. Tracked references repointed to `.devin/bin/` and the
quota spec marked canon-deployed.

## Spec impact
- removes: docs/specs/quota.md#quota-park-is-orthogonal-to-lifecycle
- removes: docs/specs/quota.md#auto-park-needs-an-explicit-limit-signal-plus-reset
- removes: docs/specs/quota.md#every-real-park-is-durable-journaled-with-its-marker
- removes: docs/specs/quota.md#reset-releases-the-park-with-one-control-probe
- removes: docs/specs/quota.md#give-up-on-a-busy-owner-holds-and-escalates
- removes: docs/specs/quota.md#halt-is-senior-to-a-quota-reset
