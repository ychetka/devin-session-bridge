# ADR-0004: living spec - docs/specs + Spec impact deltas in .changes

- Status: accepted
- Date: 2026-11-10
- Refs: issue #107, task 226, controller-manager/spec_lifecycle.md

## Context

The roadmap answers "what work was planned and done"; nothing answers
"how does the system behave now". `docs/` is edited on request and
drifts, and new sessions reconstruct current behavior from roadmap
history plus code. The approved spec_lifecycle design adopts two
OpenSpec ideas without the tool: a living requirement registry and an
`exploration` roadmap item type - this PR implements only the
registry half in this repository.

## Decision

- `docs/specs/<domain>.md` files hold the current requirement set in
  scenario form (`### Requirement: <stable name>` + WHEN/THEN/AND
  blocks), committed in English because this is public system
  documentation, not process scaffolding. Requirement names are stable
  identifiers - renames are `removes:` + `adds:` deltas.
- Every `.changes/<slug>.md` carries a required `## Spec impact`
  section after the frontmatter: `adds:` / `modifies:` / `removes:`
  lines of the form `docs/specs/<domain>.md#<anchor>` (kebab-case
  slugs of the requirement heading), or a bare `-` when no documented
  behavior changes. The PR applies the spec delta itself in the same
  commit, so spec and code cannot diverge.
- `tools/check_spec_anchors.sh` validates the post-change invariant:
  `adds:`/`modifies:` anchors must exist in the referenced file,
  `removes:` anchors must not; a missing `## Spec impact` section is
  an error. The CI lint job runs it on every PR, the orchestration
  layer's verify gate runs it locally, and the PR template gained a
  Spec impact checklist line.
- Release notes and spec deltas come from one file - no double
  bookkeeping. `docs/specs/` describes only what already ships:
  backlog/roadmap candidates never enter it.

## Alternatives considered

- OpenSpec CLI / `openspec/` directory layout: rejected - the design
  fixes a minimal native format; tool compatibility does not pay for
  the dependency or a foreign structure.
- Tool-assisted delta application: rejected - applying a delta is an
  ordinary file edit in the same PR; discipline carries it, the gate
  only checks anchor resolution.
- Spec inside `.devin/`: rejected - specs are public documentation
  and must be visible and reviewable outside the orchestration layer.

## Consequences

- A PR without a `## Spec impact` section fails verify - behavior
  changes now force a documentation decision at authoring time.
- The seed covers mailbox, tasks/leases, wake, sessions and quota;
  uncovered behavior is a doc gap fixed by the next relevant delta.
