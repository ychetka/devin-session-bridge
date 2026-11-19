# .changes/ - per-PR change entries

Every PR adds ONE file here: `<slug>.md` (short slug). Copy
`_template.md`, fill the fields, keep the content public-safe - this
repository is public and these entries feed the published
CHANGELOG/release notes.

## Enforcement (issue #135)

The entry is gated, not just conventional: `tools/check_changes_entry.sh`
runs in the CI lint job on every pull request and as a pre-merge guard
inside `.devin/bin/repo_merge.sh`. A PR that changes files merges
only when it carries ONE of:

- a new `.changes/<slug>.md` fragment (tooling/chore changes use
  `user_visible: false` - the record still lands in the Codebase
  section);
- a `CHANGELOG.md` change - release-prep branches fold fragments into
  the changelog directly instead of carrying a pending file;
- the `no-changes-entry` marker in the PR title, body, or a label -
  the documented opt-out for pure docs/ops changes that produce no
  release notes.

A version bump is NOT made on the branch (Release-on-Merge, canon
v0.0.8): each fragment declares `bump:` and the release commit
computes `next = bump(max(latest v* tag, version at base head))`,
so parallel PRs cannot collide on the version number.

## Required fields

Field lines at the top of the file, then a blank line, then the
prose body (the prose IS the changelog entry - write it as a bullet
worth reading in release notes):

- `bump`: patch | minor | major - release severity consumed by
  `.devin/bin/repo_release.sh`; max severity wins when several
  fragments pend (one functional feature - minor; one large or
  breaking change - major; fixes/tooling/docs - patch)
- `user_visible`: true | false - does an operator/integrator notice?
- `issue`: the issue number this PR closes

ADR rule: `feat`-sized changes, breaking changes, or a cross-module
refactor require a real ADR under `docs/adr/` (see its README). For
fix/docs/chore this file IS the micro-ADR - make the body carry the
decision.

Required body section (issue #107, living spec): after the fields
and prose, every entry carries `## Spec impact` listing the
requirement anchors it touches - `adds:` / `modifies:` / `removes:`
lines of the form `docs/specs/<domain>.md#<requirement-anchor>`, or a
bare `-` when the change touches no documented behavior. The PR
applies its own spec delta in the same commit, so at merge time
`adds:`/`modifies:` anchors resolve in `docs/specs/` and `removes:`
anchors are gone; `tools/check_spec_anchors.sh` enforces it in the
CI lint job (and in the orchestration layer's verify gate). Release
notes and the spec delta come from the same file - no double
bookkeeping.

At release time `.devin/bin/repo_release.sh` folds all pending
entries into `CHANGELOG.md` and deletes the consumed files, so this
directory only ever holds unreleased changes.
