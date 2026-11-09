---
type: feat
scope: tooling
user_visible: false
breaking: false
user_summary: "-"
dev_summary: Establishes the .changes/ per-PR entry convention,
  tools/release_notes.sh aggregation (User-facing + Codebase
  sections), the PR-template ADR checkbox, docs/adr/ rules and the
  fleet-adoption spec.
why: Hand-assembled changelogs drifted from what each PR actually
  shipped; a machine-readable entry per PR makes release notes
  generated, reviewable and identical across orchestrated repos.
refs: issue #96, directive 444, ADR-0001
---
