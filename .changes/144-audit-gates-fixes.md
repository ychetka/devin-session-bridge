---
# type: fix | feat | refactor | chore | docs
type: fix
scope: tooling
user_visible: false
breaking: false
user_summary: "-"
dev_summary: Post-incident audit fixes for the fleet commit-wrapper gates - anchored sk- secret pattern, last-sentinel split, lockfile added-line skip, migration deletion coverage via explicitly named roll-forward, tightened rollback regex, single-use migration stamp, project.json dry_run_cmd, and reviewer worktree isolation with untrusted-PR-content rules and head-bound verdict dismissal.
why: Issue #144 audit found fail-open paths the original rollout missed (unanchored secrets regex, first-sentinel split injection into exempt paths, uncovered staged deletions, reusable stamp); the fixes close them mechanically at the gate rather than by convention.
refs: "issue #144"
---

## Spec impact
-
