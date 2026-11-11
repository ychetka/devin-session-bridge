# Harness - session operating model

Canonical process documentation for the director session model.
Source of truth: `ai_controller/controller-manager/harness.md`
(orchestration project); this file carries the sections adopted by
this repository.

## Director session model (event-driven, approved 2026-11-11)

A worker session is the project director: it does not write code
itself - it delegates to background executors and lives in SHORT
event-driven turns. Goal: the session is almost always reachable
for orchestrator mail and questions - availability comes from
releasing the turn slot, not from holding it forever.

Principles:

- **Event-driven turn**: the director wakes only on an event -
  (A) orchestrator mail/directive, (B) background executor
  completion/failure, (C) monitoring checkpoint tick. Every turn
  ends fast: drain -> decide -> end of turn.
- **Turn length cap T** (default 30 min): a turn longer than T is
  an architectural error. Long work = a background executor +
  mandatory checkpoints every T minutes (update task status,
  drain the mailbox, renew the lease via `bridge wait`).
- **One background executor at a time** (MVP): no parallel
  subagent zoo inside the session. Executor contract:
  `{task, context_slice, deadline, report_to}`; per-step timeout
  20-30 min; retry-cap max 3 before escalation.
- **Blood signature**: the director does NOT report on the
  executor's word alone - before reporting/merging it personally
  runs `git status`/`git diff`/a key test and verifies the
  artifact.
- **Kill-switch**: a STOP directive from the orchestrator - the
  director synchronously kills the background executor BEFORE
  replying to the mail.
- **Lease != progress**: `bridge wait` renews the lease only
  while the executor is alive (a progress note with substance,
  not an empty ping).
- **External review only**: the director never spawns a reviewer
  on its own PR - conflict of interest. The reviewer agent is
  invoked independently (separate ASKON-1989 account).
- **Mid-turn inject - empirically closed (2026-11-11, canonical
  devin-session-bridge issue #137)**: `session/prompt` on a busy
  session is rejected outright ("a turn is already running") -
  not queued, not injected. The event-driven model is the only
  viable path; the 'always-on' variant is unreachable.
