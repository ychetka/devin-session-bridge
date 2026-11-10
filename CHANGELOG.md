# Changelog

## [0.9.1] - 2026-11-10

### User-facing

#### fix
- stale halt/resume/disarm deliveries are dropped instead of executed - a control message superseded by a newer one or older than SB_CONTROL_TTL_S (default 6h) no longer self-halts a resumed session `daemon` (issue #119)
- halt/resume/disarm with the daemon stopped relay + prune control mail synchronously at command time - a drain during the outage meets the same mailbox state `cli` (issue #119)

### Codebase

#### daemon
- [fix] control deliveries carry an issued_at epoch header; _control_stale_reason judges supersession (latest control notification id) + TTL at both delivery legs: the relay consumes stale outbox rows via mark_relayed-without-file and a _stale_control_pass janitor unlinks stale control files sitting in mailboxes; every drop journals stale_control_dropped (decisions + decision_events); _msg_headers extracted from _msg_is_tick. Why: incident 2026-11-10: a HALT consumed ~24h late self-halted mahogany-dryosaurus mid-task after the fleet was already resumed - control files had no epoch so an obsolete command executed at drain. (issue #119)
- [fix] janitor resolves genuine relay artifacts through the notification row; unverifiable files are judged by age only (no supersession on a forged basename id) with issued_at clamped to min(header, mtime) so a self-claimed future timestamp cannot extend the TTL (review r1, issue #119)

#### cli
- [fix] _sync_control_delivery runs relay + janitor synchronously from halt/resume/disarm when the daemon is down (daemon_running lockfile check) - the staleness guard no longer depends on the loop being alive; relay failures degrade to a relay_error field instead of aborting the response (review r1, issue #119)

#### state
- [refactor] latest_control_notification_id accepts an optional caller-held connection so the janitor's per-file epoch lookups reuse one handle (review r1, issue #119)

## [0.9.0] - 2026-11-10

### User-facing

#### feat
- dead managed sessions (incl. the orchestrator) now revive themselves when pending mail + session/list presence coincide - no more manual re-register after a tab restart `daemon` (issue #115)

### Codebase

#### daemon
- [feat] daemon resurrect leg: dead+managed sids with pending mail get a batched presence probe; liveness-marked rows CAS dead>active via db.revive_dead_managed (session_killed markers refuse); db.mark_dead makes the dead CAS + task fold + liveness marker one BEGIN IMMEDIATE so a racing kill cannot wedge session_killed between split writes, kill_session pins session_killed even on a zero-effect kill, and the managed orchestrator dead edge gains notify-send parity. Why: the deferred orchestrator_dead auto-recovery gap: a dead-flagged managed session stayed dead until someone manually re-registered - the exact human-in-the-loop the bus exists to remove. Demand-driven probing (mail pending) keeps the probe cheap and operator-kills senior. (issue #115)

## [0.8.3] - 2026-11-10

### User-facing

#### feat
- `bridge kill <task_id>` force-fails one wedged live task (running|dispatch_decided > failed ignoring the lease) and notifies the owning session - no waiting out the lease, no session teardown. `cli` (issue #113, item_35 deferred note)

### Codebase

#### cli,state
- [feat] numeric positional on `bridge kill` routes to db.kill_task: CAS to failed + lease/step clear + dirty-tree note + durable task_killed event in one transaction, NOTICE outbox row to the active owning session; typed TaskKillVerdict (not_found/refused/killed) keeps queued/awaiting_user/terminal rows refused. `state,cli` Why: Deferred item_35 follow-up (the last human-override piece): `bridge finish` is owner-CAS'd so a wedged running task could only die by lease expiry or a full session kill. The operator verdict is senior to the lease and to a waiting_quota park, and the NOTICE keeps the worker from burning a turn on an already-dead task. (issue #113, item_35 deferred note)

## [0.8.2] - 2026-11-10

### User-facing

#### fix
- Rate-limit errors surfacing through wake/prompt transport now park the affected task as waiting_quota instead of burning the retry streak or failing it outright. `daemon,cli` (issue #111, task 230)

### Codebase

#### daemon,cli
- [fix] daemon._wake_pass and cli._acp_failure sniff rpc_error text through parse_quota_signal and CAS the session's live task into waiting_quota (sources wake/acp-<phase>); a marker without a reset parks for the bounded DEFAULT_PARK_SECONDS=900 window, and a quota signal with no live task journals quota_signal_seen. quota.py gains the 'reset in N <unit> (at HH:MM UTC)' combined form where the earlier of the relative window and the absolute clock wins. Why: Live incident 2026-11-09: a fleet-wide free-tier rate limit reached the bridge as 'rpc_error - session/prompt: Reached free model rate limit ... reset in 58 minutes (at 22:01 UTC)' - the marker and reset were in the body but classification stopped at rpc_error, so parked nothing and burned wake retries plus prompt attempts against a dead pool. The transport layer is the earliest surface that sees the provider's text, so it now feeds the same conservative parser the self-report channel uses; the 900s default window for marker-without-reset mirrors free-tier windows and self-heals via the quota pass re-probe. (issue #111, task 230)

## [0.8.1] - 2026-11-10

### User-facing

_No user-visible changes._

### Codebase

#### tooling
- [feat] Living spec pilot: `.changes` entries now carry a required `## Spec impact` section (adds/modifies/removes anchors into docs/specs), docs/specs/ seeds the requirement registry for mailbox/tasks/wake/sessions/quota in WHEN/THEN form, and tools/check_spec_anchors.sh (run by verify_constraints.sh) fails PRs whose anchors do not resolve post-change. Why: Nothing answered 'how does the system behave now' - roadmap tracks work, docs drift; per the approved spec_lifecycle design each PR carries its own spec delta so behavior and documentation cannot diverge. (task 226, issue #107, ADR-0004)

## [0.8.0] - 2026-11-10

### User-facing

#### feat
- The claim-lease TTL is now configurable via `SB_LEASE_TTL_S` and defaults to 1h (was a hardcoded 30m) - long worker turns no longer outlive their lease window as easily, and `bridge` picks the override up at startup. `state` (issue #105, directive 526 (task 213 postmortem))

### Codebase

#### state
- [feat] LEASE_SECONDS resolves once at the state.db layer from `SB_LEASE_TTL_S` (contract default 3600, malformed values degrade to the default, floored at 600s (2x renew margin) so a short override cannot sit inside the renewal window permanently); every lease writer plus the daemon auto-renew ceiling, dispatch renew interval and worker-log retention follow the resolved value. Why: Queue task 213 died on 'lease expired' after ~2.7h of live work when the auto-renew explicit-touch ceiling (6 x 1800s) ran out mid-turn - the lease length itself needed to be an operator tunable with a safer default. (issue #105, directive 526 (task 213 postmortem))

## [0.7.0] - 2026-11-09

### User-facing

#### feat
- New `waiting_quota` step state parks a rate-limited task until its quota reset instead of letting it die on lease expiry; `bridge quota-park`/`quota-resume` control it manually and wake suppression keeps parked sessions asleep until reset. `state/cli/daemon` (issue #103, task 216, ADR-0003)

### Codebase

#### state
- [feat] Adds tasks.step_state + quota columns, quota_park/unpark writers with lease/claim/recovery guards, a quota-signal text parser hooked into `bridge event`, wake suppression until quota_reset_at, a daemon quota pass with a single post-reset probe, and step-state surfacing in inspect/status. Why: Tasks were dying on lease expiry and burning retry budget during model rate limits even though the session had saved its work (precedent: task 212 'lease expired' at HALT); a durable parked step keeps the lease owned by the original claimant and resumes exactly the blocked step. (issue #103, task 216, ADR-0003)

## [0.6.0] - 2026-11-09

### User-facing

#### feat
- New read-only `bridge fleet-metrics [--window 24h|7d]` renders a per-session/repo/orchestrator markdown report from bridge state and repo cycle_logs. `cli` (issue #98, directive 446, ADR-0002)

### Codebase

#### cli
- [feat] Adds commands/metrics.py + a `fleet-metrics` subcommand aggregating tasks/questions/decision_events per session and each repo's .devin/cycle_log.jsonl (paths from session cwd registry) into a markdown report; orchestrator self-metrics and an incident table included. Why: The orchestrator needed a fleet-level fact report for gate-architecture review without sessions ever seeing it; cycle-time and incident data existed but had no single read path. (issue #98, directive 446, ADR-0002)

## [0.5.2] - 2026-11-09

### User-facing

#### fix
- The host-side ingress timer no longer attempts (and fails) to reconcile ns-* forwarders; it owns tcp: rows only, while each project's userns-context loop owns its namespace specs. `ingress` (issue #99, PR #100)

### Codebase

#### ingress
- [fix] Added SESSIONS_BRIDGE_INGRESS_KINDS env filter to `bridge ingress sync` - a pass skips foreign-kind desired specs and leaves their tracked forwarders untouched (unparseable stored specs are treated as foreign); env values are case-normalized and validated against the known spec kinds. Why: The host timer lacks CAP_SYS_ADMIN over a project-owned user namespace, so setns() into ns-* targets always failed; restricting each reconcile context to the kinds it can actually act on removes the noise and prevents a filtered pass from killing foreign forwarders. (issue #99, PR #100)

#### tooling
- [feat] Establishes the .changes/ per-PR entry convention, tools/release_notes.sh aggregation (User-facing + Codebase sections), the PR-template ADR checkbox, docs/adr/ rules and the fleet-adoption spec. Why: Hand-assembled changelogs drifted from what each PR actually shipped; a machine-readable entry per PR makes release notes generated, reviewable and identical across orchestrated repos. (issue #96, directive 444, ADR-0001)

## [0.4.0] - 2026-11-08

Neutral host ingress layer (issue #92, task #209): port forwards
are now materialized from the allocation registry itself.

### ingress/

- `bridge reserve tcp_port <port> --fwd <spec> [--bind <addr>]`
  embeds a machine-readable forward declaration into the row's
  `purpose` (`fwd=`/`bind=` tokens; grammar: `tcp:<host>:<port>`,
  `ns-pid:<pid>:<port>`, `ns-pidfile:<path>:<port>`).
- `bridge ingress sync` reconciles detached
  `session_bridge.ingress.forwarder` processes with the registry:
  reserve -> listener appears, release -> forwarder removed,
  changed spec or pidfile target -> respawn. Reconciler state in
  `<state_dir>/ingress.json`, child logs in
  `logs/ingress-fwd-<port>.log`, flock-serialized.
- `bridge ingress status`: desired-vs-running view.
- `deploy/host-ingress.{service,timer}` + `install_ingress_timer.sh`:
  30s reconcile cadence, Persistent=true.
- docs/ALLOCATIONS.md documents the contract; trade-ai's
  supervisor.sh moves its 5678/8080/8443 chain onto it.

## [0.2.1] - 2026-11-02

Zero-touch delivery completed: the bridge can now wake idle
managed sessions itself, and the daemon's liveness model is
coherent with that wake path. Closes #31 and #33.

### acp-host wake transport (wake/)

- The daemon gains a `_wake_pass` after relay: a managed session
  with a stale heartbeat and relayed-but-unacked mail (or a live
  task) gets a short "drain your mailbox" prompt injected through
  the local detached acp-host - a real turn, so drain-mode
  sessions process queued mail with no human typing. The nudge
  carries no payload; the fs mailbox stays the only data channel.
- Transport: `acp-host-*.lock` discovery ({pid, port, token},
  live-pid preferred, re-read per attempt), NDJSON token hello,
  then plain ACP `initialize > session/list > session/load >
  session/prompt > detach` over `127.0.0.1:<port>`.
- Safety: `cognition.ai/statusEnum` busy check (never prompt a
  mid-turn session -> `wake_skipped_busy`), process-local attach
  mutex, bounded replay drain, 120s pass deadline, per-session
  300s cooldown (60s retry-soon after `sent_unconfirmed` with
  exponential growth, capped), per-host circuit breaker
  (3 failures -> 600s), structured `wake_failed` reasons.
- `bridge wake <sid>` - manual wake through the same path
  (bypasses cooldown, keeps the busy check).
- Security boundary documented: lockfile `0600` + dir `0700` =
  same-UID bearer access.

### Managed-session liveness coherence (item_17)

- The heartbeat sweep no longer marks `mode='managed'` sessions
  dead: between turns they are stale-hb by design (that signal is
  the wake trigger), and the sweep was failing their tasks 120s
  after dispatch while wake prompts were in flight.
- Managed death is ACP-reported instead: 3 consecutive
  `no_session` wake outcomes (absent from the host's
  `session/list`) flip the session to dead and requeue its live
  task through the same path as the unmanaged sweep
  (`worker_dead`, `reason=no_session_streak`). `no_cwd`
  (present in list, unwakeable) counts as seen; all other
  failures are streak-neutral.
- `_wake_candidates` requires `status='active'` and also matches
  sessions with a live task - the wake probe doubles as the
  liveness check when the outbox is empty.
- `bridge arm` clears a leftover `queue_paused` flag in the same
  upsert, so arming restores task claimability.
- `db.ask()` now also writes the active orchestrator a
  `directive` notification ("[question #N pending ...]") in the
  same transaction - worker replies reach the orchestrator
  mailbox without manual `questions` polling.

### Docs

- `docs/OPERATOR.md`: wake transport requirements and failure
  modes, sweep exclusion for managed mode, no_session dead path
  and MTTD note, `bridge wake`, arm-clears-pause.
- `docs/orchestrator/PLAYBOOK.md`: drain + wake close the loop
  for interactive sessions; pending questions arrive as
  `directive` mail (no manual polling needed).

## [0.2.0] - 2026-11-01

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
