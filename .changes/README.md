# .changes/ - per-PR change entries

Every PR adds ONE file here: `NNNN-slug.md` (issue/PR number + short
slug). Copy `_template.md`, fill the frontmatter, keep the content
public-safe - this repository is public and these entries feed the
published CHANGELOG/release notes.

Required frontmatter fields (indented continuation lines fold into
the previous field's value):

- `type`: fix | feat | refactor | chore | docs
- `scope`: the module/subsystem the change touches - one of
  `cli`, `daemon`, `state`, `acp`, `mailbox`, `wake`, `ingress`,
  `tooling`, `docs`, `deploy` (extend the list here when a new
  module lands)
- `user_visible`: true | false - does an operator/integrator notice?
- `breaking`: true | false - does anything consuming the CLI/API
  surface need to change?
- `user_summary`: one sentence for the User-facing release section
  (required when user_visible is true; "-" otherwise)
- `dev_summary`: one sentence for the Codebase section
- `why`: motivation / decision link in one or two sentences
- `refs`: task IDs, issue/PR numbers, directive or decision links

ADR rule: `type: feat`, `breaking: true`, or a cross-module refactor
requires a real ADR under `docs/adr/` (see its README). For
fix/docs/chore this file IS the micro-ADR - make `why` carry the
decision.

Required body section (issue #107, living spec): after the
frontmatter, every entry carries `## Spec impact` listing the
requirement anchors it touches - `adds:` / `modifies:` / `removes:`
lines of the form `docs/specs/<domain>.md#<requirement-anchor>`, or a
bare `-` when the change touches no documented behavior. The PR
applies its own spec delta in the same commit, so at merge time
`adds:`/`modifies:` anchors resolve in `docs/specs/` and `removes:`
anchors are gone; `tools/check_spec_anchors.sh` (run by
`verify_constraints.sh`) enforces it. Release notes and the spec
delta come from the same file - no double bookkeeping.

At release time `tools/release_notes.sh --apply` folds all pending
entries into `CHANGELOG.md` and deletes the consumed files, so this
directory only ever holds unreleased changes.
