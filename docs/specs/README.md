# Living spec - `docs/specs/`

This directory is the requirement registry: **how the system behaves
now**, not how it was planned. Roadmap tracks work; specs describe
behavior. When they disagree, the code wins and the spec gets a
correcting delta in the next PR.

## Format

One file per domain. Each requirement is a `### Requirement:` heading
with one or more `#### Scenario:` blocks in WHEN/THEN/AND form:

```markdown
### Requirement: example requirement name
The bridge SHALL do the thing.

#### Scenario: the happy path
- **WHEN** the trigger condition holds
- **THEN** the observable outcome happens
- **AND** a second invariant holds
```

Requirement names are stable identifiers - `.changes/` Spec impact
sections and coverage checks link to them as kebab-case anchors
(`#example-requirement-name`), the same slug rule GitHub applies to
markdown headings. Renaming a requirement is a `removes:` + `adds:`
delta, not an edit.

## Delta discipline

A `.changes` entry's `## Spec impact` section declares the anchors it
touches (`adds:` / `modifies:` / `removes:`), and the PR applies the
delta to these files in the same commit. `tools/check_spec_anchors.sh`
verifies every declared anchor resolves (or is gone, for removes).

## Domains

- [`mailbox.md`](mailbox.md) - notification outbox, queue files,
  drain/ack protocol, stall escalation
- [`tasks.md`](tasks.md) - task lifecycle, claims, leases, retries,
  recovery, waits, questions
- [`wake.md`](wake.md) - daemon wake legs, suppression, policy
- [`sessions.md`](sessions.md) - registration, liveness, HALT/RESUME,
  kill/unregister
- [`quota.md`](quota.md) - `waiting_quota` step state, quota parks,
  reset sweep and probes
