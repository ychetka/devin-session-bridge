# ADR-0001: per-PR .changes entries as the changelog source of truth

- Status: accepted
- Date: 2026-11-09
- Refs: issue #96, orchestrator directive 444

## Context

Release notes and `CHANGELOG.md` were assembled by hand at tag time
from memory and PR titles. Three failure modes recurred: entries
described intent rather than what shipped, developer-facing rationale
never reached the release notes at all, and the format drifted per
release. The fleet needs the same mechanism in every orchestrated
repository, so the convention must be copyable, not bespoke.

## Decision

Every PR carries one machine-readable file `.changes/NNNN-slug.md`
with fixed frontmatter (`type`, `scope`, `user_visible`, `breaking`,
`user_summary`, `dev_summary`, `why`, `refs`).
`tools/release_notes.sh` aggregates all pending entries into two
release-note sections - User-facing (`user_visible: true` only,
grouped by type) and Codebase (everything, grouped by scope, carrying
`dev_summary` and `why`) - and `--apply` folds them into
`CHANGELOG.md` and deletes the consumed files in the release-prep
commit. ADRs under `docs/adr/` are required only for `feat`,
`breaking`, or cross-module refactors; for `fix`/`docs`/`chore` the
`.changes` file itself is the micro-ADR.

## Consequences

- Changelog content is reviewed inside its own PR instead of being
  reconstructed later; release notes become generated, not written.
- `.changes/` only ever holds unreleased work - "pending entries" is
  synonymous with "since last tag", so no tag arithmetic is needed.
- Release flow gains one explicit step (`--apply` in the release-prep
  commit); skipping it leaves stale entries that would double-publish
  on the next release - the script is idempotent-safe because it
  deletes what it consumes.
- Entries and ADRs are public artifacts: internal session mechanics,
  paths and credentials must never appear in either.

## Alternatives considered

- Conventional-commits parsing (git log -> changelog): rejected -
  commit messages cannot carry `user_visible`/`breaking` flags or a
  reviewed `why`, and squash-merge titles drift.
- Hand-edited CHANGELOG only: rejected - the status quo; drift and
  missing rationale are the problem being fixed.
- External changelog tools (changesets, towncrier): rejected - the
  directive requires zero added dependencies; bash + python3 covers it.
