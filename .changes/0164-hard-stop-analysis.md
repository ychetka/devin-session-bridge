# docs: SESSION_HARD_STOP analysis + implementation plan

## Changes
- `docs/SESSION_HARD_STOP.md`: root-cause analysis of the 2026-11-13 orchestrated-session stop incident (bridge kill is bus-state only; session/cancel wired solely to withdraw --stop-turn; all managed sessions share one `devin acp` backend PID so there is no per-session OS process) plus the layered hard-stop design (session/cancel -> session/close --force -> attributed orphan sweep -> bus teardown) and implementation plan.

## Spec impact
Analysis/design document only - no code or spec-anchor changes.
