# Contributing

## Flow

issue -> branch `issue_<N>-<slug>` -> implementation -> tests ->
independent review -> PR -> merge -> release.

Everything that lands in the repository - commits, PRs, issues, code,
change entries, ADRs - is written in English and is public-safe.

## Change entries (`.changes/`)

Every PR adds exactly one `.changes/<slug>.md` file - see
`.changes/README.md` for the required fields
(`bump`/`user_visible`/`issue` + a prose body). This file is the
reviewed source of the changelog; keep it accurate and public-safe.

ADR rule: `type: feat`, `breaking: true`, or a cross-module refactor
requires an ADR under `docs/adr/` (copy `_template.md`, next number).
For `fix`/`docs`/`chore` the `.changes` file itself is the micro-ADR -
its prose body must carry the decision. The PR template's "ADR required"
checkbox declares which path was taken.

## Release notes (Release-on-Merge, canon v0.0.8)

Branches carry NO version bump - each PR's `.changes/<slug>.md`
fragment declares `bump: patch|minor|major` instead. After a merge,
`.devin/bin/repo_release.sh` computes
`next = bump(max(latest v* tag, version at base head))` from the
pending fragments' `bump:` fields, writes the release commit
(version file + folds `.changes` prose bodies into `CHANGELOG.md` +
consumes the fragments), tags `v<next>` and publishes the GitHub
release. Parallel branches never compete for a number - merge
order decides the final version automatically.

`tools/release_notes.sh` is the legacy manual release-prep tool -
it parses the OLD 8-field frontmatter fragment format and is NOT
part of the RoM flow (no CI or canon script calls it). Kept for
ad-hoc note previews on old-format fragments only.

## Fleet adoption (spec for other orchestrated repos)

The mechanism is copyable verbatim - no repo-specific code:

1. Copy `.changes/` (`README.md` + `_template.md` only; do not copy
   pending entries) into the target repo.
2. Copy `.github/pull_request_template.md` (at minimum the
   "Change entry" checklist block).
3. Create `docs/adr/` with `README.md` + `_template.md` copied from
   this repo; number ADRs per-repo starting at 0001.
4. Create `CHANGELOG.md` with a `# Changelog` title if absent -
   `repo_release.sh` inserts the release section under it.
5. Release runs through the deployed canon scripts
   (`.devin/bin/repo_merge.sh` -> `repo_release.sh`); no per-repo
   release-prep step is needed under RoM.
6. Port-specific adjustments are limited to: scope vocabulary in
   `.changes/README.md`, the repo's own release script name in
   CONTRIBUTING, and the ruff/pytest lines in the PR template's
   test-plan block.

## Tests

`pytest` must stay green; behavior changes land with G/W/T tests.
`uvx ruff@0.17.3 check src/` and `format --check` must pass.
