# Contributing

## Flow

issue -> branch `issue_<N>_v<X.Y.Z>` -> implementation -> tests ->
independent review -> PR -> merge -> release.

Everything that lands in the repository - commits, PRs, issues, code,
change entries, ADRs - is written in English and is public-safe.

## Change entries (`.changes/`)

Every PR adds exactly one `.changes/NNNN-slug.md` file - see
`.changes/README.md` for the required frontmatter
(`type`/`scope`/`user_visible`/`breaking`/`user_summary`/
`dev_summary`/`why`/`refs`). This file is the reviewed source of the
changelog; keep it accurate and public-safe.

ADR rule: `type: feat`, `breaking: true`, or a cross-module refactor
requires an ADR under `docs/adr/` (copy `_template.md`, next number).
For `fix`/`docs`/`chore` the `.changes` file itself is the micro-ADR -
its `why` must carry the decision. The PR template's "ADR required"
checkbox declares which path was taken.

## Release notes

Pending `.changes/` entries ARE the "since last tag" set - the
directory is emptied at every release, so no tag arithmetic exists.

- `tools/release_notes.sh` prints the generated notes:
  `### User-facing` (only `user_visible: true`, grouped by type) and
  `### Codebase` (all entries grouped by scope with `dev_summary` +
  `why`).
- `tools/release_notes.sh --version X.Y.Z --apply` is the release-prep
  step: it inserts the generated `## [X.Y.Z] - <date>` section into
  `CHANGELOG.md` under the title and deletes the consumed entry files.
  Run it on the release branch (or the last PR before tagging) and
  commit the result together with the version bump -
  `.devin/scripts/repo_release.sh` then tags the already-prepared
  head and passes `--generate-notes` or hand-fed notes to
  `gh release create`.

## Fleet adoption (spec for other orchestrated repos)

The mechanism is copyable verbatim - no repo-specific code:

1. Copy `.changes/` (`README.md` + `_template.md` only; do not copy
   pending entries) into the target repo.
2. Copy `tools/release_notes.sh`; it resolves paths via
   `git rev-parse --show-toplevel`, so no edits are needed. A symlink
   to a shared checkout also works.
3. Copy `.github/pull_request_template.md` (at minimum the
   "Change entry" checklist block).
4. Create `docs/adr/` with `README.md` + `_template.md` copied from
   this repo; number ADRs per-repo starting at 0001.
5. Create `CHANGELOG.md` with a `# Changelog` title if absent -
   `--apply` inserts under it.
6. Add the release-prep step (`tools/release_notes.sh --version X.Y.Z
   --apply` on the release branch) to the repo's release runbook.
7. Port-specific adjustments are limited to: scope vocabulary in
   `.changes/README.md`, the repo's own release script name in
   CONTRIBUTING, and the ruff/pytest lines in the PR template's
   test-plan block.

## Tests

`pytest` must stay green; behavior changes land with G/W/T tests.
`uvx ruff@0.17.3 check src/` and `format --check` must pass.
