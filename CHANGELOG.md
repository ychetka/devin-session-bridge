# Changelog

## [0.2.0] - 2026-10-31

Orchestration protocol release: the bridge becomes a two-way
message bus for orchestrator-managed session fleets, with
zero-touch delivery to locked Desktop sessions, a supervisor
daemon, a Delegation-of-Authority escalation model, and recurring
scheduled dispatch. Closes #14 and #15.

### Message bus (state.db, schema v3)

- `sessions` gains role/mode/status; `worker_heartbeats` with
  generation-counter CAS; orchestrator is a CAS singleton with a
  bounded refusal window and `--force` takeover for known-dead
  holders.
- `questions`: worker->orchestrator channel, `pending > claimed >
  answered`, unique pending per task, 3-question budget, janitor
  releases stale claims (min-one-per-task per pass).
- `notifications` outbox: writers commit `relayed_at NULL`, the
  daemon relays to filesystem mailboxes and marks `relayed_at`;
  `ack_at` watermark survives orchestrator restarts.
- `audit_events`: append-only deliberation + escalation trail
  (verbatim partner outputs, synthesis, directive, doa_trigger).
- `schedules`: recurring task definitions (`every_seconds`,
  `next_run_at`, `catchup`, `enabled`), `tasks.run_after` column.
- `user_version` 1 -> 3, all migrations idempotent under
  `BEGIN IMMEDIATE`.

### Managed Mode delivery (mailbox/)

- `bridge arm`/`disarm`/`disarm --all`: enrolled sessions spawn a
  background mailbox-watcher subagent polling
  `queue/<sid>/msg_*.txt`; deliveries land inside the parent's open
  turn, which re-arms before `end_turn` (`TURN_TTL_CYCLES` bounds
  context growth). Level-triggered, numeric `ls -v` ordering.
- Proven live: zero-touch multi-cycle delivery on a locked
  Desktop session incl. ~27-minute idle persistence.

### bridge-daemon (daemon/)

- `fcntl.flock` singleton; relays unrelayed outbox rows to
  mailboxes embedding `notification_id`/`kind`/`task_id`/
  `question_id` in the payload.
- Staleness sweep: dead worker -> `status=dead` + live tasks
  requeued with stash/reset-base-ref instruction; silent
  orchestrator -> one deduplicated `notify-send`.
- Own heartbeat file + `daemon_decisions.jsonl` for external
  cron/systemd watchdogs; `janitor_questions` runs each pass;
  schedule materialization pass (see below).

### CLI (bridge.py)

- `register <sid> [--role ...] [--force]`, `heartbeat <sid>`,
  `ask`, `ack`, `questions list|claim|answer` (claimant-bound),
  `escalate show` + `escalate reply A|B|defer|abort|override:<text>`
  (defer -> paused, abort -> failed + cleanup note),
  `schedule <sid> --every|--at [--catchup]`, `schedule list|clear`.
- `bridge status`: <25-line fleet view - escalation/orchestrator-dead
  banner with exact reply command, daemon+orchestrator vitals,
  fleet table (sid|repo|task|state|Q n/3|FAIL n/2|hb age), 24h
  digest.

### Docs

- `docs/OPERATOR.md`: 60-second mental model, 4 operator
  playbooks (plug worker / orchestrator start-restart + CAS
  troubleshooting / escalation handling / emergency stop), daemon
  lifecycle, watchdog snippets, status field reference.
- `docs/orchestrator/PLAYBOOK.md`: orchestrator-session contract
  template (DoA matrix, anti-escalation rule, committee protocol,
  escalation report format, mailbox duty, guardrails, bootstrap
  prompt) - copy into target workspaces as
  `.devin/orchestrator/PLAYBOOK.md`.

### Notable fixes

- Numeric mailbox ordering (`ls -v`) - `msg_10` no longer
  overtakes `msg_2`.
- Janitor multi-claim wedge (IntegrityError deadlock) fixed.
- `answer_question` claimant binding (anti-zombie answers).
- Closed-form catchup realign (O(1); a `while` walk would hang a
  `BEGIN IMMEDIATE` lock after long downtime).
- Duty-text `bridge ack --session` corrected.
- `init()` migration race closed (`BEGIN IMMEDIATE`).

## [0.1.3] - 2026-10-30

- `install_hooks.sh` writes the `hooks.v1.json` envelope format the
  client expects (issue #7).

## [0.1.2] - 2026-10-30

- CODEOWNERS + branch-protection plumbing (PR #5).

## [0.1.1] - 2026-10-30

- Hardening follow-ups from v0.1.0 review (issue #3): atomic task
  claims, lease/stale recovery, bounded worker logs, queue depth.

## [0.1.0] - 2026-10-30

- MVP: state.db task queue, ACP push, Stop-hook queue for locked
  sessions, `bridge push/pump/status/retry`, install_hooks.sh.
