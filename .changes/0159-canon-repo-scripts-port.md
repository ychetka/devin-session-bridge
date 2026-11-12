---
type: chore
scope: canon
user_visible: false
breaking: false
user_summary: "-"
dev_summary: Port repo_issue_comment.sh and repo_pr_review.sh wrappers from devin-telegram into the canonical .devin/scripts set.
why: Sessions had no sanctioned path to post issue comments or submit PR reviews without raw gh - devin-telegram carried two local wrappers the canonical set lacked. Both are hygiene-gated (hygiene_check.sh) and portable as-is.
refs: orchestrator directive 2026-11-12
---

## Spec impact
-
