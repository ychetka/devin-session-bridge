# REVIEW HAZARDS — independent reviewer enforcement

Load this file when the diff touches any trigger area listed at the
top of `CODING_HAZARDS.md`. The numbered rules there are the single
source of truth — this file adds review duties, it does not restate
or fork them.

## Independence duties

- **Do not trust the author's report.** Quoted commands, exit codes
  and output tails are claims, not proof. Verify against the actual
  PR head.
- **Verify baseline.** Rerun safe native checks (lint, typecheck,
  tests — as declared by the project's own manifests/configs) against
  the exact PR head yourself when the author's BASELINE or GATES
  evidence is doubtful or missing.
- **Verify AC_MAPPING.** Every acceptance criterion must map to a
  real test that exists in the diff and exercises the claim. A
  criterion mapped to a test that does not exist, does not run, or
  does not test the criterion is a finding.
- **Verify scope.** Changed files must match the task's file
  whitelist. Out-of-whitelist edits are findings regardless of code
  quality.
- **Honest gate reporting.** Prompt-level rules are not CI
  enforcement. Report which level each gate actually runs at
  (prompt / commit-wrapper / CI) and never describe a prompt rule as
  enforced CI.

## Per-rule checks (see CODING_HAZARDS.md for full text)

When a trigger area applies, check the diff itself for:

- Rule 1: blocking calls newly introduced inside async/event
  contexts.
- Rule 2: external calls added without a timeout/deadline.
- Rule 3: retries on non-idempotent operations, unbounded retry
  loops, missing backoff.
- Rule 4: logically-atomic writes split across statements; external
  calls inside transactions; non-idempotent external writes;
  unstated isolation assumptions on read-modify-write.
- Rule 5: floating-point arithmetic on money, prices or quantities.
- Rule 6: naive or unzoned timestamps stored or compared.
- Rule 7: unbounded queries, caches or collections; handles, timers,
  tasks or subscriptions leaked on any path.
- Rule 8: empty catch/except, ignored results, swallowed
  cancellation or abort signals.
- Rule 9: identifier-keyed access without an ownership/authorization
  check.
- Rule 10: secrets, tokens or PII in code, tests, fixtures or logs.
- Rule 11: calls to APIs that do not exist in the codebase or vendor
  docs; dependencies added that are absent from the manifest or have
  unverified provenance — confirm exact package names and registries
  yourself, do not take the name's plausibility as evidence
  (slopsquatting).

## Findings

- Quote the offending code verbatim (file, lines, minimal block).
- Map every finding to its rule number.
- Missing or fabricated gate evidence is itself a finding — absence
  of proof is not proof of absence.
