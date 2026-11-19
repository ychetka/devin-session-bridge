# <slug>.md - one file per PR, public-safe content only.
# Field lines first (no --- frontmatter), then a blank line, then the
# prose body - the prose IS the changelog entry.
bump: patch
user_visible: false
issue: <N>

One or two sentences of prose: what changed and why, phrased as a
release-notes bullet. fix/docs/chore entries serve as the micro-ADR -
the body must carry the decision.

## Spec impact
- adds: docs/specs/<domain>.md#<requirement-anchor>
- modifies: docs/specs/<domain>.md#<requirement-anchor>
- removes: docs/specs/<domain>.md#<requirement-anchor>
