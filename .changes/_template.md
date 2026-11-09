---
# NNNN-slug.md - one file per PR, public-safe content only.
# type: fix | feat | refactor | chore | docs
type: fix
# scope: module/subsystem touched (cli, daemon, state, acp, ...)
scope: cli
user_visible: false
breaking: false
# user_summary: one sentence for the User-facing section; required
# when user_visible is true, "-" otherwise.
user_summary: "-"
# dev_summary: one sentence for the Codebase section.
dev_summary: what changed technically, in one sentence.
# why: motivation/decision in one or two sentences. For fix/docs/
# chore this file serves as the micro-ADR - make why carry it.
why: why this change exists and why it was done this way.
# refs: task IDs, issue/PR, directive/decision links.
refs: issue #N
---

## Spec impact
# Required (task 226 / issue #107): list every docs/specs requirement
# anchor this change touches - the PR applies its own spec delta, so
# adds/modifies anchors must exist in docs/specs after the change and
# removes anchors must be gone. Use a bare "-" when the change touches
# no documented behavior. Anchors are kebab-case slugs of
# "### Requirement: <name>" headings; tools/check_spec_anchors.sh
# validates them (the CI lint job runs it).
- adds: docs/specs/<domain>.md#<requirement-anchor>
- modifies: docs/specs/<domain>.md#<requirement-anchor>
- removes: docs/specs/<domain>.md#<requirement-anchor>
