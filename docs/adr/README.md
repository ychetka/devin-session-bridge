# Architecture Decision Records

ADRs capture cross-cutting decisions that a per-PR `.changes` entry
cannot hold. Numbered `NNNN-slug.md`, immutable once merged (supersede
with a new ADR, never edit history).

## When an ADR is required

- `.changes` entry `type: feat` - a new capability or contract
- `.changes` entry `breaking: true` - anything a consumer must adapt to
- A refactor spanning two or more modules (`cli/`, `daemon/`, `state/`,
  `acp/`, `mailbox/`, `wake/`, `ingress/`)

Everything else (`fix`, `docs`, `chore`, single-module work) is covered
by the `.changes` file itself - that file IS the micro-ADR, so its
`why` must carry the decision.

## Process

1. Copy `_template.md` to `NNNN-slug.md` (next free number, zero-padded).
2. Fill Status/Context/Decision/Consequences/Alternatives.
3. Reference the ADR in the `.changes` entry `refs`.
4. The PR template's "ADR required: yes" checkbox points reviewers at it.

Public-safe content only - this repository is public.
