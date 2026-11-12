---
type: fix
scope: cli
user_visible: false
breaking: false
user_summary: "-"
dev_summary: hygiene_check.sh exempts .devin/ paths from the attribution class in diff mode, fixing a self-trip on canon rule-source files.
why: Canon files (agents/*.md, session_audit.sh) must carry attribution pattern examples as rule definitions; shipping them to a worker repo tripped verify_constraints and forced a documented ship_commit bypass. Secret class intentionally NOT exempted - a real key in .devin must keep failing.
refs: shared_person notice 994, orchestrator decision 2026-11-12
---

## Spec impact
-
