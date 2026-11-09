# Operating sessions-bridge

Runbook for the human operator who deploys and supervises a
bridge-managed fleet of Devin sessions. Everything below is verified
against the current CLI; agent-facing protocol detail (the DoA matrix,
committee protocol, mailbox duty) lives in the orchestrator contract
template `docs/orchestrator/PLAYBOOK.md`. To deploy it, copy this file
into your target workspace as `.devin/orchestrator/PLAYBOOK.md` - the
bootstrap prompt in Playbook 2 expects it at that path.

## 60-second mental model

- `state.db` (SQLite, WAL) is the single source of truth: sessions,
  tasks, questions, the notifications outbox, heartbeats and the audit
  log all live there.
- The fs mailbox is the transport: the daemon turns outbox rows into
  `queue/<sid>/msg_<id>.txt` files next to `state.db`.
- `bridge daemon` is the supervisor: it relays the outbox to mailboxes,
  wakes idle managed sessions with a short "drain your mailbox" prompt
  through the local acp-host (no payload - the mailbox stays the data
  channel), and marks dead sessions.
- Managed watchers are the delivery inside an open turn, in one of
  two modes the session picks by context: `resident` (a background
  watcher subagent polls the mailbox, prints and removes pending
  `msg_*` files and exits; its completion lands inside the parent's
  open turn, which processes the message and re-arms a new watcher)
  for autonomous sessions, or `drain` (the session itself lists,
  reads and removes `msg_*` files at turn start and once more before
  the turn ends) for interactive Desktop sessions - a resident
  watcher occupies the turn slot and would block the human's input.
- You are the arbiter: the fleet decides autonomously inside the
  Delegation of Authority (DoA) matrix; only whitelisted triggers
  escalate to you.

```text
operator (you)                  = arbiter - answers escalations only
  |  bridge status / bridge escalate
  v
state.db (SQLite WAL)           = source of truth
  |  notifications outbox (relayed_at NULL)
  v
bridge daemon (flock on         = supervisor + relay, ~5s passes
  daemon.lock)                  |
  |  writes queue/<sid>/msg_<id>.txt
  |  injects a wake nudge via acp-host when the session is idle
  v
managed watcher (resident or    = delivery into the open turn
  drain; resident shown)          |  prints + removes msg files, exits
  v
worker / orchestrator session   = processes, `bridge ack`, re-arms
```

Storage layout (state dir = the directory holding `state.db`, default
`$XDG_DATA_HOME/sessions-bridge` or `~/.local/share/sessions-bridge`):

```text
<sessions-bridge state dir>/
  state.db                              # source of truth
  daemon.lock                           # flock + daemon pid
  daemon_heartbeat.txt                  # unix timestamp, every pass
  daemon_decisions.jsonl                # {ts, event, sid, detail} lines
  logs/daemon.log                       # daemon stderr
  logs/worker_<tid>_<attempt>_<ts>.log  # acp worker stderr (keep 10)
  queue/<sid>.json                      # hook wakeup trigger
  queue/<sid>/msg_<notification_id>.txt # mailbox messages
```

## Playbook 1 - plug in a worker

1. Install the lifecycle hooks into the worker repo (idempotent,
   preserves foreign entries, backs up to `hooks.v1.json.bak`):

   ```bash
   ./install_hooks.sh /path/to/worker-repo
   ```

   This registers `SessionStart`, `SessionEnd`, `UserPromptSubmit` and
   `Stop` hooks in the repo's `.devin/hooks.v1.json` (Stop timeout is
   forced to cover the 150s park cycle, default 600s).

2. Open a Devin session on that repo and note its session id (`<sid>`).

3. Enroll the session in managed mode (safe before or after the
   session exists - `bridge arm` creates the sessions row if absent):

   ```bash
   bridge arm <sid>
   ```

4. The session registers on the bus as a worker (run inside the
   session; `--workspace` defaults to its working directory):

   ```bash
   bridge register <sid>
   ```

5. The mailbox duty reaches the session as `additionalContext` from
   the `SessionStart` hook (on open, resume or turn rotation) or on a
   kick prompt of <=3 chars on `UserPromptSubmit`. From then on the
   watcher loop self-sustains: tasks pushed with `bridge push <sid>`
   arrive as `task_dispatch` mailbox messages with zero touch.

   Managed task lifecycle (item_18): the pump marks the task
   `dispatch_decided` with a lease when it writes the notification,
   so a delivered-but-unclaimed task can never be re-notified every
   pass. The worker drains the mailbox, runs `bridge claim <id>
   --session <sid>` (queued/dispatch_decided > running), executes,
   then `bridge finish <id> --session <sid>` (running > done, or
   `--fail`). The dispatch lease doubles as the claim window: an
   acked-but-never-claimed task folds back to `queued` on lease
   expiry via `recover_stale` and gets exactly one re-dispatch.

   Attribution (item_19): every `lease_owner` names the executing
   session - `bridge-<sid>-<tid>` (managed), `acp-<sid>-<pid>-<tid>`
   (acp-push), `hook-<sid>-<pid>` (Stop-hook worker) - so `inspect`
   and `status` show who runs the task, not which dispatcher pid
   stamped the claim. Mailbox deliveries carry `session_id:` (the
   executing recipient) in the msg header, plus `from:` when the
   source is derivable (`questions.claimed_by` for answers, the
   asking session for worker-ask directives).

   Question lifecycle (item_20): `bridge ask` refuses with
   `no_orchestrator` when no active orchestrator exists - a question
   is never accepted into a void. A pending question the orchestrator
   never claimed within `PENDING_TIMEOUT_SECONDS` (3600s) expires in
   the daemon janitor pass (terminal `expired`); the worker is
   notified through the outbox (`question_answer`, expiry note) and
   its pending slot frees for a re-ask. While a task carries an
   unresolved (pending/claimed) question, `recover_stale` renews its
   lease instead of requeueing - waiting on the orchestrator never
   burns the worker's attempts. The managed duty forbids interactive
   question widgets (`ask_user_question` and any UI dialog) - every
   question goes through `bridge ask`; nobody answers a UI dialog
   and the turn would park forever.

   External waits (item_22): a claim that never renews dies on
   `lease expired` exactly like a dead worker - long PR-review or CI
   waits burned leases silently. `bridge wait <id> --session <sid>
   [--reason "<what>"]` is the claimer's out-of-band wait signal: a
   lease-owner CAS (same guard as claim/finish) extends the lease by
   one `LEASE_SECONDS` per call and lands the reason in the task
   note. It is bounded on purpose - a dead claimer simply stops
   renewing and the task requeues within one lease period. Repeat it
   while the wait lasts; `claim` on an owned running task renews the
   same way without a reason.

   Provider rate limits (task #216): a task that hits an explicit
   model rate limit parks as `waiting_quota` - a durable step state
   stored orthogonally to the lifecycle status, not a failure. The
   park keeps the task's live status and the claimant's lease: no
   attempt is spent, `recover_stale` and the dead-worker fold skip
   the row, and no second claimant can be issued a lease. While the
   reset window is open, daemon wakes and `bridge wake` are
   suppressed (`suppressed: waiting_quota until <reset_at>`); when
   `quota_reset_at` passes, the daemon's quota pass clears the park,
   re-arms the lease and sends one control probe; a probe that cannot
   be delivered (paced, no lockfile, transport error) re-parks for a
   short defer window instead of burning the attempt budget on a
   lapsed lease. Defers are bounded per park episode
   (`quota_defer_count`, reset by every real park) - past the limit
   the task is released back to the ordinary lease lifecycle with a
   durable `quota_probe_gave_up` event. A provider still limited
   re-parks via the self-report channel with the new reset. HALT stays senior - a
   halted session's park is not swept until `bridge resume`. Channels: manual `bridge quota-park <sid>
   --task N --reset-at <iso|HH:MM UTC> --note "<error>" [--model M]
   [--checkpoint "<where>"]`, early release `bridge quota-resume
   <sid> --task N`, and auto-park when a `bridge event`/`bridge
   notice` body carries explicit rate-limit text plus a parseable
   reset time - `rpc_error`, `busy`, `timeout` and `lease expired`
   never park (there is no structured ACP quota error code today,
   so the auto channel is a conservative text parser). `bridge
   inspect` shows `step_state`/`quota_reset_at`/metadata; `bridge
   status` reads the session `quota` in STATE/TSTAT and exposes the
   reset in the JSON fleet rows.

6. Declare the repo's protected paths (e.g. `migrations/`, `auth/`,
   `.github/`) to the orchestrator in the task text or repo rules.
   There is no `bridge register` flag for this in the current CLI -
   the orchestrator applies the DoA protected-path rule against the
   list you declare in prose (see docs/orchestrator/PLAYBOOK.md,
   Guardrails).

Expected healthy output afterwards (the `daemon`/`orchestrator`
lines appear once Playbook 2 and the daemon lifecycle are done - a
worker-only fleet shows `daemon: stopped` and `orchestrator: none`):

```text
$ bridge status
daemon: running (pid 8123, hb 2s)
orchestrator: <orc-sid> active (hb 14s)
SID              REPO             TASK                 TSTAT  STATE       Q FAIL  SEEN WAKE    MAIL
<orc-sid>        ai_controller    -                    -      idle        -    -   14s sent    -
<sid>            worker-repo      #7 fix flaky test    run    running   0/3  0/2   30s -      540s
24h: done 0 | auto-decisions 0 | escalations 0
```

`TSTAT` is the lifecycle of the session's latest task (`queued`,
`disp` = dispatch_decided, `run`, `wait` = awaiting_user, `done`,
`fail`, `paus`, `quota` = parked on a provider rate limit until the
task's `quota_reset_at`); `-` when there is no task. `STATE` is the
session reading: `dead` for a daemon-marked session, `idle` when
nothing is running (a `done` task still reads `idle` - check `TSTAT`
for the task's actual end state), `quota` when the latest task is
`waiting_quota`, otherwise the task status. The two columns
together separate "worker idle after finishing" (`done` + `idle`)
from "task not picked up" (`queued` + `queued`).

`SEEN` is the liveness cell: heartbeat age for unmanaged sessions,
turn-activity recency for managed ones (the freshest of heartbeat,
session/task updates and mailbox acks - a managed session between
drain-mode turns is stale-hb by design, so bare heartbeat age would
lie). `WAKE` shows the last recorded wake outcome (`sent`, `busy`,
`verif`, `unc` = sent_unconfirmed) plus the unconfirmed streak
(`uncx3`) and `!` once the human escalation fired. `MAIL` is the age
of the session's oldest undrained actionable notification (relayed
to the mailbox, still unacked, `kind != heartbeat_tick`); `-` when
the mailbox is clean - the stall gauge the daemon escalates on
(`mail_stall_escalated`, see the daemon pass list). `--json` carries
the full fields (`wake_outcome`, `wake_streak`, `wake_escalated`,
`wake_age_seconds`, `activity_age_seconds`, `mail_age_seconds`).

## Playbook 2 - start / restart the orchestrator

1. Open a new Devin session on the workspace that should own
   orchestration.

2. Deploy the contract template into that workspace (create the
   directory first if needed):

   ```bash
   mkdir -p /path/to/workspace/.devin/orchestrator
   cp docs/orchestrator/PLAYBOOK.md \
      /path/to/workspace/.devin/orchestrator/PLAYBOOK.md
   ```

   The bootstrap prompt below points the session at
   `.devin/orchestrator/PLAYBOOK.md`, so the file must exist at that
   path inside the target workspace.

3. Paste this bootstrap prompt verbatim:

   ```text
   You are the session-orchestrator (Tech Lead) for this workspace.

   1. Read .windsurfrules and .devin/orchestrator/PLAYBOOK.md - your
      operating contract, including the Delegation of Authority matrix.
   2. Run `bridge register <your-session-name> --role orchestrator`.
      If it refuses with
      "heartbeat fresh" and you KNOW the old session is closed, rerun
      with --force.
   3. Re-ground: `bridge status`, .devin/SESSION_STATE.md,
      .devin/roadmap.md. If SESSION_STATE.md does not exist,
      create it - the orchestrator owns this file.
   4. Arm your inbox per the Mailbox duty section of PLAYBOOK.md:
      this session is interactive (the human sits behind it), so
      use drain mode - drain the mailbox through your session's
      turn-start / turn-end mechanism at the start of every turn
      and once more before the turn ends (watcher duty, drain-mode
      self-drain, or harness script - per the session's own
      protocol).
   5. Resume the roadmap. Decide autonomously within the DoA matrix;
      escalate to the human ONLY per the whitelist, in escalation-report
      format.
   ```

4. The session runs `bridge register <its-sid> --role orchestrator`. The
   orchestrator role is a CAS singleton: registration is refused while
   another session holds the role with a fresh heartbeat
   (`HEARTBEAT_STALE_SECONDS = 120`), and also while the holder is a
   managed session still marked active - a DRAIN-mode managed
   orchestrator is stale-hb between turns by design, so heartbeat age
   alone never demotes it (v0.3.2). A holder the supervisor marked
   `dead` blocks nothing regardless of heartbeat age, and a
   dead-flagged session clears its own latch on re-register - or, for
   unmanaged sessions, on its next heartbeat (managed dead verdicts
   come from session/list absence, which a heartbeat cannot refute,
   so a heartbeat never revives a managed row - this also prevents a
   leftover observer-shell heartbeat loop from flapping a dead
   managed session back and forth, v0.3.2).

   Refusal output looks like:

   ```text
   refused: orchestrator <holder-sid> holds a fresh heartbeat (42s ago); use --force to take over
   refused: orchestrator <holder-sid> is a managed-active holder - heartbeat age cannot demote it; use --force to take over
   ```

   Troubleshooting:

   - The old orchestrator session is actually still open: close it, or
     wait until its heartbeat goes stale (~120s after its last
     `bridge register`) and rerun register normally. A managed holder
     never goes stale for CAS purposes - close the session or use
     `--force` there.
   - The old session is known-dead (closed tab, crashed host): rerun
     with `bridge register <its-sid> --role orchestrator --force`. The takeover
     demotes the previous holder to worker.
   - Re-registering the same sid is always accepted and bumps the
     heartbeat generation - that is how a live orchestrator refreshes
     its own heartbeat (see "Session heartbeats" below).

5. Verify: `bridge status` shows `orchestrator: <sid> active (hb Ns)`.

## Playbook 3 - handle an escalation

1. `bridge status` surfaces open escalations in the attention banner
   (up to 5 rows, then a `+N more` fold):

   ```text
   ESCALATED #12: destructive_ops - reply: bridge escalate reply 12 A|B|defer|abort|override:<text>
   ```

2. Inspect the full escalation:

   ```bash
   bridge escalate show            # or: bridge escalate show --json
   ```

   Each entry shows the session and task it came from, `trigger` (the
   DoA rule that fired), `question` (the escalation report body:
   reason, facts + evidence, options A and B, blast radius - as the
   orchestrator wrote it), `synthesis` (its recommendation) and the
   exact reply command. `--json` additionally carries
   `partner_outputs`, `question_id` and `created_at`.

3. Reply with the response grammar:

   ```bash
   bridge escalate reply <id> A                  # pick option A
   bridge escalate reply <id> B                  # pick option B
   bridge escalate reply <id> defer              # decide later
   bridge escalate reply <id> abort              # cancel the task
   bridge escalate reply <id> "override:<text>"  # your own directive
   ```

   Reply semantics:

   - `A` / `B` - the chosen option is recorded as the directive.
   - `override:<text>` - the text after the colon becomes the
     directive verbatim.
   - `defer` - the task is parked as `paused` with the note
     "blocked: deferred by human". Its live-task slot frees, so the
     worker moves on to the next independent task. Resume later with
     `bridge unpark <session>`.
   - `abort` - the task lands on terminal `failed` with the note
     "aborted by human: cleanup branch" (the worker cleans up its
     branch). `bridge retry <session|task_id>` revives it if needed.

   On any reply the bridge CAS-fills the audit row's `directive`
   (a second reply reports "already resolved"), answers the question
   bound to the escalation with the directive text, and sends the
   calling session a `directive` mailbox notification.

## Playbook 4 - emergency stop (Big Red Button)

```bash
bridge disarm --all   # every managed session: mode -> unmanaged + DISARM notice
bridge daemon stop    # SIGTERM the supervisor (5s grace)
```

What this does:

- `bridge disarm --all` flips every `sessions.mode='managed'` row to
  unmanaged and queues each session a `disarm` mailbox message
  ("managed mode disarmed: stop re-arming the mailbox watcher"). A
  session that consumes it stops re-arming its watcher - the delivery
  loop dies with the current watcher.
- `bridge daemon stop` halts the relay and the dead-session sweep.

What it does not do:

- Tasks already delivered into a session keep running in that
  session - the bridge never force-closes a Desktop-open session.
  Tell the session to stop, or let it finish.
- `queued` tasks stay queued in `state.db`; while sessions are
  disarmed, `bridge push` falls back to the unmanaged transports
  (hook queue for a locked session, acp push for an idle one), so hold
  off pushing if you want a real freeze.
- `bridge daemon stop` is idempotent: "daemon not running" is a
  normal (exit 0) answer.

## Fleet halt (bridge halt / bridge resume)

`bridge halt` is the graceful fleet stop (v0.3.11) - the managed
counterpart to the big red button. Where `disarm` is a permanent
exit from managed mode, `halt` is a pause: sessions finish their
current atomic action, save state, stop claiming new tasks and end
their turn, parked until a `resume` delivery.

```bash
bridge halt <sid>               # flag + HALT notification + wake + state report
bridge halt --all               # broadcast to every managed+active session
bridge halt --all --stop-daemon # ... then SIGTERM the supervisor
bridge resume <sid>             # lift: clear the flag + RESUME notification + wake
bridge resume --all             # fleet-wide
```

Design answers (task #180):

- **Semantic, not free text**: `halt`/`resume` are real
  `NotificationKind` values, so the duty routing table, relay, ack
  tracking and stall attribution all apply - a hand-written
  "HALT:" directive rides none of that.
- **Shared priority pipeline, not a second channel**: HALT is the
  first member of the priority-flagged delivery class the deferred
  urgent-interrupt item needs. Preemption lives in the drain
  contract - the duty tells the session to apply `halt`/`disarm`
  deliveries before the numeric file order - not in a parallel
  transport. Mailbox files persist, so HALT survives a daemon
  restart untouched; the wake is best-effort, delivery is the file.
- **`sessions.halted_at` is the durable flag**: `claim_managed`
  refuses new claims while it is set (the enforced half of "do not
  claim new queued tasks" - a claim refusal that says `halted` is
  the contract, not a race). A lease renewal on an already-running
  task still passes: finishing the current atomic action is exactly
  what HALT asks for.
- **Parked mail is not a stall**: the mail-stall pass treats
  halted sessions' undrained mail as parked
  (`mail_stall_deferred_halted`) - stronger evidence than the busy
  grace, so it has no ceiling.
- **Resume is required, and exists**: without RESUME a halted
  session has no way to distinguish "park until woken" from "park
  forever" - `bridge resume` clears the flag and delivers the
  `resume` semantic. `disarm` stays the permanent variant.
- **Honest limits**: halt cannot kill a mid-turn Desktop session -
  it is a drain-on-boundary contract. A busy session sees the HALT
  at its next drain; `--all` reports every target's wake outcome,
  mailbox depth and live task so the operator sees who is mid-turn.

Post-halt assessment of the deferred urgent-interrupt /
human-override items (task #198, v0.3.14):

- **What halt/resume already consumed**: the shared priority-flag
  delivery class (kind-level priority in the drain contract), a
  durable fleet-stop flag with enforced claim refusal, parked-mail
  semantics, and the coarse human override "stop the whole fleet
  now". `bridge escalate reply ... override:<text>` covers the
  DoA-gate override shape.
- **Force-interrupt a busy session is not a bus-layer feature**: a
  wake/drain prompt is ordinary turn input - it queues behind the
  open turn and cannot preempt it (the parked-subagent spike,
  task #184, established this for subagent processes; it applies
  identically to the session's own turn). True interruption is
  host-plane (`devin rm`, Desktop close), outside the bridge's
  reach. Documented out of scope.
- **Priority queue jump is a non-problem**: `ux_tasks_live_session`
  gives every session at most one live task, so a task can never
  queue-jump ahead of another for the same session - "urgent" means
  push to a session whose slot is free, or kill the live task.
- **Remaining real gap - operator force-fail of a running task**:
  `bridge finish` is owner-CAS'd, so a wedged session's running
  task survives until lease recovery (or `lease_auto_renew`'s
  ceiling while it reports busy). The delivered `bridge kill
  <sid>` (v0.5.0) is session-scoped - it detaches the whole
  session and requeues its task for
  re-dispatch. A task scoped `bridge kill <task_id>` - CAS
  `running|dispatch_decided > failed` ignoring lease_owner, with
  the same `requeue_dead_worker` dirty-tree note - would still
  close the "stop THIS task now" piece. Small, deferred as a
  separate item.

## Daemon lifecycle

| Command | Effect |
| --- | --- |
| `bridge daemon start [--interval S]` | spawns the detached `daemon run` supervisor; refuses (exit 4) when `daemon.lock` is held. Interval defaults to `SESSIONS_BRIDGE_DAEMON_INTERVAL` then 5s. |
| `bridge daemon stop` | SIGTERM to the pid in `daemon.lock`; waits up to 5s, then reports a wedge. |
| `bridge daemon status` | running/stopped + pid + heartbeat age (`--json` adds `state_dir`). |
| `bridge daemon once` | one relay+sweep pass in the foreground, then exits - good for debugging. |
| `bridge daemon run [--interval S]` | internal foreground loop; `daemon start` uses it. Do not run by hand except for debugging. |

Daemon files in the state dir:

- `daemon.lock` - flock-held single instance; the file content is the
  daemon pid. Process death releases the lock.
- `daemon_heartbeat.txt` - unix timestamp stamped before every pass,
  so a hung pass ages the stamp (that is the watchdog signal). A
  second line carries observability metrics
  (`mail_undrained_max_age=<s>`) - readers take line 1 only.
- `daemon_decisions.jsonl` - one `{ts, event, sid, detail}` JSON line
  per decision: `daemon_start`, `relayed`, `relay_error`,
  `orchestrator_dead`, `worker_dead`, `pass_error`, `lock_refused`,
  `daemon_stop`, `wake_sent`, `wake_skipped_busy`, `wake_failed`,
  `wake_breaker_open`, `wake_pass_deadline`, `wake_verified`,
  `wake_unconfirmed`, `wake_unconfirmed_escalated`,
  `wake_verify_failed`, `mail_stall_escalated`,
  `mail_stall_deferred`, `mail_stall_deferred_busy`,
  `mail_stall_verify_failed`, `mail_stall_deferred_halted`,
  `session_unregistered`, `lease_auto_renewed`,
  `lease_renew_verify_failed`, `task_recovered`, `pass_timing`.
- `pass_timing` is the per-iteration instrumentation line (v0.3.15):
  one event per loop carrying every pass duration as
  `schedule=Xms relay=Xms pending_scan=Xms wake=Xms
  lease_renew=Xms recovery=Xms sweep=Xms mail_stall=Xms
  question_janitor=Xms`.
  A pass quietly outgrowing the daemon interval shows up here
  before the fleet looks stalled.
- `logs/daemon.log` - the detached daemon's stderr (decision lines are
  echoed there too).

Each pass (~5s, with up to ~1s startup jitter):

1. Relay: every `notifications` row with `relayed_at NULL` becomes a
   `queue/<sid>/msg_<id>.txt` file (atomic write), then `relayed_at`
   is CAS-marked. Delivery is at-least-once; consumers dedup by
   notification id.
2. Wake: managed+armed sessions get a nudge prompt through the
   acp-host wake transport (see below) when they carry actionable
   mail ON DISK - any unconsumed non-tick `msg_*.txt` in the
   session's mailbox dir, whatever heartbeat or notification rows
   say (v0.3.12). The fs mailbox is the authoritative channel: a
   notification row whose file exists is covered by this leg, a row
   whose file was drained but never acked is ack lag, not mail, and
   can no longer wake (the earlier DB-row legs produced a permanent
   ~5-minute wake loop on physically empty mailboxes - the
   mahogany-dryosaurus consumed-but-unacked noise). Foreign writers
   (hand-written directives) qualify too since files are the only
   trigger. `heartbeat_tick` files never wake - a liveness poke is
   not mail. A session mid-turn is never prompted. Dead sessions are
   not candidates. Two task states are excluded from the live-task
   probe (v0.3.3): an `awaiting_user` task is parked on an
   out-of-band answer that itself arrives as actionable mail, and a
   `running` task whose `wait_until` stamp is still fresh declared an
   external wait via `bridge wait` - probing a session that cannot
   act yet is noise; the stamp decays with the lease it mirrors, so
   an abandoned wait becomes probe-worthy on its own.
   A stale-heartbeat session whose ONLY qualification is a live task
   is never prompted (v0.3.7): it gets a batched `session/list`
   presence probe instead, so no_session dead-marking still runs on
   an empty outbox while an empty mailbox can never wake - the
   buttered-tilapia drain-prompt loop incident.
   Use `bridge directive <sid> "<text>"` instead of writing files by
   hand: it creates the notification row so relay, wake and
   stall-escalation all see the mail. `bridge notice <sid> "<text>"`
   is the same registered path for informational reports (worker
   status/task reports) - kind `notice` means "read and
   incorporate", while `directive` means "obey".
3. Lease auto-renew (v0.3.13): `running`/`dispatch_decided` tasks of
   managed+active sessions whose lease falls inside
   `SESSIONS_BRIDGE_LEASE_RENEW_MARGIN` (default 300s) are renewed on
   the worker's behalf when session/list verifies the owning session
   busy - the same `_busy_sessions` batch the stall pass uses. A
   DRAIN-mode session cannot renew inside a long ACP turn, so without
   this pass a >lease-length turn died on schedule and `recover_stale`
   burned an attempt (or failed the task) while the worker was
   verifiably mid-turn doing the work; busy status IS the liveness
   signal the worker cannot send itself. Renew-before-expiry is
   required because `recover_stale` fires lazily inside every bridge
   `_work` pump - a lapsed lease is gone regardless of what the daemon
   does next. Bounded: `auto_renew_lease` never stamps
   `lease_renewed_at`, so it stays the last worker-initiated lease
   touch (claim, mark_running, `bridge wait`, `bridge claim`
   re-claim; `recover_stale`'s question-blocked renewal also stamps
   it, bounded by the question janitor TTLs); once that touch is
   older than
   `SESSIONS_BRIDGE_LEASE_AUTO_RENEW_SECONDS` (default 6x
   `LEASE_SECONDS`, clamped to >= one lease length) the row no
   longer qualifies and expiry recovery takes over - a wedged turn
   that keeps reporting busy cannot hold the task forever. Idle,
   absent, statusless, unmanaged and unverifiable sessions are simply
   not renewed: their leases lapse into `recover_stale` exactly as
   before (fail-closed - unverifiable is not busy). Each renewal logs
   `lease_auto_renewed`.
4. Recovery (v0.3.16): the daemon now owns the lease-expiry fold that
   used to live only in the CLI `_work` pump - `recover_stale` ran
   lazily inside bridge commands, so with zero CLI traffic an expired
   lease could sit unrecovered until somebody typed `bridge status`.
   The `_recovery_pass` runs right after `_lease_renew_pass` so a
   verifiably-busy worker gets its renewal first and only the truly
   expired rows fold: live tasks requeue (`task_recovered` decision,
   terminal `failed` at the attempt budget notifies the operator
   once), stale `dispatch_decided` rows fold back to queued, and the
   renewal is idempotent across daemon/CLI invocations through the
   same CAS + `mark_notified` journal.
5. Sweep: **unmanaged** sessions whose `worker_heartbeats.last_seen_at`
   is older than `HEARTBEAT_STALE_SECONDS` (120s) flip to
   `status='dead'`, and unmanaged sessions with NO heartbeat row at
   all - register-then-vanish ghosts, the impossible-vibraphone class -
   flip after `GHOST_TTL_SECONDS` (86400s) counted from
   `sessions.updated_at` (v0.3.16: previously the sweep required a
   heartbeat row, so ghosts were invisible to it). The same scan runs
   lazily inside `bridge status`, `bridge wait` and the `_work` pump,
   so CLI traffic alone also GCs dead registrations. A ghost that
   still holds a LIVE task is NOT demoted on the ghost leg -
   `sessions.updated_at` does not track task liveness, so a
   silently-working unmanaged worker past GHOST_TTL would die
   mid-task into duplicate execution; lease recovery folds the task
   first and the registration dies next pass. Managed sessions
   are excluded: between turns a managed drain-mode session is
   stale-hb by design, and that signal
   is the wake trigger - treating it as death would fail every
   dispatched task 120s after dispatch. A managed session's real
   death is detected on the wake path instead: 3 consecutive
   `no_session` outcomes (absent from the host's session/list) flip
   it to dead the same way, with the same task requeue (a
   `worker_dead` decision carrying `reason=no_session_streak`). A
   dead worker's live task is requeued (attempt +1, terminal
   `failed` at the `MAX_ATTEMPTS = 2` budget) carrying the note
   "requeued-dead-worker: stash dirty state (git stash -u -m
   dead-worker-backup-<id>) and reset the worktree to the task's base
   ref before starting". A silent orchestrator raises one deduped
   `notify-send -u critical`.
6. Mail stall: any session whose oldest undrained actionable mail is
   older than `SB_STALL_THRESHOLD_S` (default 1800s; the legacy
   `SESSIONS_BRIDGE_MAIL_STALL_SECONDS` alias still works) gets one
   escalation per stall episode - a `notice` notification carrying
   the `[MAIL_STALL]` marker pushed into the active orchestrator's
   mailbox (`mail_stall_escalated` decision), so the orchestrator
   can manual-wake it, forward to `tg_escalate` or escalate to the
   developer. The orchestrator's
   own stall (or the absence of an active holder) falls back to
   `notify-send` - a self-addressed notice would sit undrained
   too. With no delivery target at all the pass logs
   `mail_stall_deferred` and retries next pass without consuming the
   episode. A session verifiably mid-turn (session/list statusEnum
   reports live work) is never escalated at all: it cannot drain
   mid-turn, so its undrained mail is expected ack lag - the pass
   logs `mail_stall_deferred_busy` and consumes no stamp, so the same
   mail escalates once the session goes idle (v0.3.10). The grace is
   bounded by `SESSIONS_BRIDGE_BUSY_GRACE_SECONDS` (default 4x the
   stall threshold): mail older than that on a still-busy session
   escalates anyway - a wedged turn cannot silence the signal.
   Dead, absent, unmanaged, statusless and unverifiable sessions
   (attach failure logs `mail_stall_verify_failed`) get no grace. The
   dedup stamp lives in `wake_state.mail_escalated_at`;
   it is cleared when the mailbox fully drains OR when the oldest
   remaining mail is newer than the stamp (the escalated items were
   drained - what sits there now is a new stall). A stamp older than
   `SB_STALL_REALERT_S` (default 3600s) re-arms itself: a stall that
   never resolves re-alerts on the cadence inside the same episode
   (v0.5.0). Unconsumed
   `msg_*.txt` files count as undrained mail too - their mtime is the
   age evidence for mail the outbox never registered (v0.3.4). The
   pass also writes the maximum undrained age as
   `mail_undrained_max_age=<s>` on the
   second line of `daemon_heartbeat.txt` (line 1 stays the unix
   timestamp - the watchdog contract is unchanged). Since v0.5.0 each
   escalation also appends a durable `decision_events` row
   (`kind=mail_stall`) carrying `reason=mail_stall`, the stalled
   sid, `project` (cwd basename), `unacked`, `oldest_mail_age_s`,
   `pending_questions`, `last_seen_s`, `pending_foreign` and the
   delivery `target` - the restart-proof
   payload the orchestrator's `tg_escalate` forward is sourced from;
   the notification body names the same fields inline (`[MAIL_STALL]
   ... <sid> (<project>)`).

## Wake transport (acp-host)

The mailbox alone cannot reach an idle session: with no turn open,
nobody drains `queue/<sid>/`. The daemon's wake step attaches a
transient client to the detached acp-host that owns every local Devin
session and injects a short "drain your mailbox" prompt - a real
turn, so the session's drain duty runs and the mail is handled. The
nudge carries no payload; the fs mailbox stays the only data
channel, so a lost or duplicated wake is harmless.

Requirements:

- the target session must be open in Devin Desktop / the shared
  `devin acp` process on the same host;
- an `acp-host-*.lock` file in `~/.devin-server/cli/` (JSON
  `{pid, port, token}`); the newest valid lockfile wins and is
  re-read on every attempt;
- the session must not report a non-idle `cognition.ai/statusEnum`
  (`working`, ...): an explicit busy status is skipped with a
  `wake_skipped_busy` decision and retried next pass. A session
  listed with no `statusEnum` at all - a persisted session whose
  Desktop client was closed - counts as idle and gets the nudge:
  the acp-host resumes it on demand and runs the prompt (verified
  live on a days-closed session, v0.3.3). Before the fix such rows
  were misclassified as busy, so a closed-client DRAIN session was
  never woken and its mail sat undrained - the statusless-busy bug
  behind the sleeping-DRAIN incident.

Failure modes (all land in `daemon_decisions.jsonl`, none modify
notification state):

- `wake_failed` with `reason=wake_host_down` - no lockfile, dead pid,
  or connect refused;
- `wake_failed` with `reason=unauthorized` - stale/rotated token;
  a fresh lockfile is picked up on the next pass;
- `wake_failed` with `reason=no_session` - the session is not known
  to the acp-host (tab closed, never opened); 3 consecutive misses
  mark the managed session dead and requeue its live task
  (`worker_dead` decision, `reason=no_session_streak`). The streak is
  held in memory - a daemon restart resets it, so worst-case
  detection time is streak x wake cooldown (~15 min at defaults)
  after the restart;
- `wake_failed` with `reason=no_cwd` - the session IS in the host's
  list but carries no cwd (registered without `--workspace` and the
  host reports none); it counts as alive - fix the registration;
- `wake_failed` with `reason=rpc_error` - a post-list RPC failure
  (e.g. `session/prompt` rejected because the session went busy
  mid-attach); it counts as alive too - only pre-list RPC failures
  surface as `attach_failed` instead, so an `rpc_error` never feeds
  the no_session dead-streak (v0.3.2);
- `wake_breaker_open` - N consecutive host failures suspended all
  wake attempts for the breaker window (default 3 failures, 600s);
- `wake_pass_deadline` - the pass ran out of its 120s budget;
  remaining candidates retry next pass.

Pacing: per-session cooldown 300s between wake attempts, shortened
to 60s after a `sent_unconfirmed` outcome (prompt written but no
turn evidence, doubling 60/120/240 capped at the cooldown on a
streak) so a silently missed nudge self-heals; attaches are
serialized inside each process and the daemon is single-instance
via its flock - a manual `bridge wake` could overlap a daemon
attach, and the acp-host arbitrates concurrent clients (two short
nudges at worst).

Post-inject verification (item_21): a `sent_unconfirmed` wake only
proves the prompt bytes were written, not that a turn opened - a
closed tab or dead agent accepts the prompt into nothing while mail
accumulates invisibly. After the candidate loop the daemon re-checks
every unconfirmed session on one shared `session/list` attach:

- statusEnum busy (`working`, ...) - the turn opened; the wake
  records `verified`, the streak resets (`wake_verified` decision);
- statusEnum still idle or the session absent - the durable
  `wake_state.unconfirmed_streak` grows (`wake_unconfirmed`);
- the verification attach itself fails - `wake_verify_failed`, every
  streak untouched: unverified is never unconfirmed.

The streak lives in the `wake_state` table, so a daemon restart
keeps it - unlike the in-memory pacing streaks. Once the streak
reaches `WAKE_UNCONFIRMED_ESCALATE_STREAK` (default 3) the daemon
fires one `notify-send -u critical` per streak episode
(`wake_unconfirmed_escalated` decision); the `escalated` flag in
`wake_state` suppresses repeats until a live outcome opens a fresh
episode. `bridge status` surfaces the outcome, streak and flag in
the `WAKE` column and `--json` fleet rows.

Security boundary: the lockfile token is a same-uid bearer
credential - Devin writes `acp-host-*.lock` mode 0600 inside the
0700 `~/.devin-server/cli/` directory. Anyone able to read the
lockfile can already act as the user. The idle check is a snapshot
taken just before the prompt; a session that goes busy inside that
window is arbitrated by the acp-host (a rejected prompt surfaces as
`wake_failed`), and a stray nudge is always the fixed drain text,
never payload.

Manual wake:

```bash
bridge wake <session_id>          # attach + idle check + nudge prompt
bridge wake <session_id> --prompt "custom nudge text"
```

### session/resume (protocol >= 2)

The item_139 spike established that an explicit resume step is not
needed at protocol 1: `session/load` + `session/prompt` on a
persisted session whose Desktop client is closed already produces a
real turn (`stopReason: end_turn` verified live on a days-closed
session). Since v0.3.12 the wake transport reads the
`protocolVersion` the host advertises in the `initialize` response:
version >= 2 prefers `session/resume` (same params as load, no
replay drain), and a resume `rpc_error` degrades to `session/load`
on the same channel - the advert is a hint, not a contract. The
flagged risks from the deferred design stay in force:

- **duplicate turns**: a resume racing an in-flight wake prompt (or
  a human reopening the tab) could open a second turn on one
  session - gate resume on the same `statusEnum`-absent-or-idle
  check and let the host arbitrate, never fire resume + prompt as
  two unsequenced calls;
- **parked watchers**: a resumed session may re-attach a resident
  mailbox watcher the user had parked - resume must be treated as a
  liveness event and re-check `queue_paused`/disarm state before
  letting it act;
- **replay effects**: delivery is at-least-once on every channel,
  so a resumed session can re-drain an already-seen mailbox - the
  consumer-side notification-id dedup is the only correct guard,
  not transport-level "send once" assumptions.

`bridge wake` bypasses the cooldown but keeps the busy check - it
refuses (conflict) while the session is mid-turn. Useful to verify
the transport or to nudge a session on demand.

## Code layering notes (v0.5.0)

- `cli/` is a package since v0.5.0 (task #210): `bridge.py` keeps the
  parser, the `_HANDLERS` dispatch table, `main()` and read-compat
  re-exports; `cli/common.py` holds shared helpers (queue/state dirs,
  trigger files, notify-send, tombstone merge, lazy dead-session GC,
  `JsonResponse` emission); `cli/dispatch.py` owns the pump + `_work`
  machinery (semaphore, worker logs, ACP cycle); `cli/status.py` the
  fleet view; `cli/commands/` the `_cmd_*` handlers by domain
  (tasks / sessions / questions / notices / allocations / ops).
  `python -m session_bridge.cli.bridge` is unchanged. Patch seams
  moved with their call sites - tests patch `dispatch._spawn_worker`,
  `common._notify_send`, `ops._sigterm_daemon`,
  `commands.sessions.{wake_session,find_lockfile,verify_sessions}`.

### v0.3.15 notes

- `acp/protocol.py` is the single home of the NDJSON framing,
  JSON-RPC envelope, response classification and `initialize`
  payload shared by both ACP transports (`acp/client.py` stdio,
  `wake/acp_host.py` socket). Protocol fixes now land in one place -
  the two implementations cannot drift on wire semantics, only on
  transport I/O and their own error vocabularies.
- `state/verdict.py` holds the typed verdicts db functions return
  for compound operations (tagged pydantic unions; first consumer
  `unregister_session`). New compound returns belong there, not in
  new ad-hoc `{"status": ...}` dict shapes - the `kind`
  discriminator is the machine-checked contract.
- `bridge unregister` returns a typed verdict internally; the CLI
  JSON surface is unchanged except for the added
  `mailbox_tombstoned` / `mailbox_dir_recreated` fields.

## systemd --user service (auto-start on boot)

On systemd machines the daemon runs best as a user unit (v0.3.12):
`install_daemon_unit.sh` renders `deploy/sessions-bridge.service`
with the resolved `bridge` binary, installs it to
`~/.config/systemd/user/sessions-bridge.service`, and enables +
starts it:

```bash
./install_daemon_unit.sh                    # install + enable + start
./install_daemon_unit.sh --print            # render to stdout only
./install_daemon_unit.sh --db /data/state.db --interval 10
./install_daemon_unit.sh --no-start         # stage without starting
```

The unit runs `bridge daemon run` (the same foreground loop
`daemon start` detaches) with `Restart=on-failure`; `daemon.lock`
inside the state dir still prevents double starts if the unit and a
manual `daemon start` race. Auto-start **on boot** additionally
requires lingering - the installer attempts `loginctl
enable-linger "$USER"` and warns if it needs root; without linger
the unit runs only while a login session is open.

A systemd-managed daemon makes the external watchdog below mostly
redundant - `Restart=on-failure` covers a dead process and the
heartbeat file still marks a hung one (a hung process is not a
failure exit, so keep the watchdog if hang-restart matters). On
non-systemd machines the watchdog stays the only supervision.

## External watchdog (cron / systemd)

The daemon itself is a single point of supervision, so watch it from
outside. `daemon_heartbeat.txt` holds a unix timestamp; staleness is
the failure signal for both a dead and a hung daemon.

Watchdog script `~/bin/sessions-bridge-watchdog.sh`:

```bash
#!/usr/bin/env bash
# Restart the bridge daemon when its heartbeat file goes stale.
# `bridge daemon start` refuses safely if the lock is actually held,
# so a restart attempt against a live daemon is harmless.
set -u
# Note: SESSIONS_BRIDGE_STATE_DIR is this script's own variable, not a
# bridge flag - it must point at the same state dir the daemon uses
# (the directory containing state.db / --db path's parent).
STATE_DIR="${SESSIONS_BRIDGE_STATE_DIR:-$HOME/.local/share/sessions-bridge}"
HB="$STATE_DIR/daemon_heartbeat.txt"
LIMIT=120
BRIDGE="${SESSIONS_BRIDGE_BIN:-bridge}"

age=999999
if [ -f "$HB" ]; then
    ts=$(head -n 1 "$HB" 2>/dev/null | tr -d '[:space:]')
    case "$ts" in
        ''|*[!0-9]*) ts=$(stat -c %Y "$HB" 2>/dev/null || echo 0) ;;
    esac
    age=$(( $(date +%s) - ${ts:-0} ))
fi
if [ "$age" -gt "$LIMIT" ]; then
    "$BRIDGE" daemon start >/dev/null 2>&1 || true
    if command -v notify-send >/dev/null 2>&1; then
        notify-send -u critical "sessions-bridge watchdog" \
            "daemon heartbeat ${age}s old - restart attempted"
    fi
fi
```

Cron (`crontab -e`, every minute; make sure `bridge` is on cron's PATH
or set `SESSIONS_BRIDGE_BIN` to an absolute path):

```cron
* * * * * $HOME/bin/sessions-bridge-watchdog.sh
```

systemd user timer (alternative):

```ini
# ~/.config/systemd/user/sessions-bridge-watchdog.service
[Unit]
Description=sessions-bridge daemon watchdog

[Service]
Type=oneshot
ExecStart=%h/bin/sessions-bridge-watchdog.sh
```

```ini
# ~/.config/systemd/user/sessions-bridge-watchdog.timer
[Unit]
Description=run the sessions-bridge watchdog every minute

[Timer]
OnBootSec=1min
OnUnitActiveSec=1min

[Install]
WantedBy=timers.target
```

```bash
systemctl --user enable --now sessions-bridge-watchdog.timer
```

## `bridge status` field reference

Human output is capped at 24 lines by construction:

```text
ESCALATED #12: destructive_ops - reply: bridge escalate reply 12 A|B|defer|abort|override:<text>
ORCHESTRATOR DEAD: reopen session, bridge register --role orchestrator [--force]
daemon: running (pid 8123, hb 2s)
orchestrator: <orc-sid> active (hb 14s)
SID              REPO             TASK                 TSTAT  STATE       Q FAIL  SEEN WAKE
<orc-sid>        ai_controller    -                    -      idle        -    -   14s -
worker-bee       devin-telegram   #7 fix flaky test    run    running   1/3  0/2   30s -
24h: done 5 | auto-decisions 12 | escalations 1
```

Attention banner (printed only when needed):

- `ESCALATED #<id>: <trigger>` - an `audit_events` escalation row is
  open (directive NULL); the line carries the exact reply command.
  More than 5 collapse into `+N more open escalation(s) - bridge
  escalate show`.
- `ORCHESTRATOR DEAD` - the orchestrator session is marked dead, or
  (unmanaged mode only) its heartbeat is past the 120s stale window;
  a managed orchestrator idle between turns is stale-hb by design and
  does NOT trip this banner. Reopen the session and re-register
  (`--force` when the old holder is known-dead).

Vitals:

- `daemon: running (pid N, hb Ns)` / `daemon: stopped` - flock probe
  plus `daemon_heartbeat.txt` age.
- `orchestrator: <sid> <status> (hb Ns)` / `orchestrator: none` -
  role holder, `active|dead`, heartbeat age.

Fleet table, one row per session:

| Column | Meaning |
| --- | --- |
| `SID` | session id; a leading `*` marks a session that never registered (seen only through a pushed task). |
| `REPO` | basename of the session `cwd`, else its title. |
| `TASK` | `#<id> <label>` of the session's latest task (`-` when none). |
| `TSTAT` | compact lifecycle of that latest task: `queued`, `disp` (= `dispatch_decided`), `run` (= `running`), `wait` (= `awaiting_user`), `done`, `fail` (= `failed`), `paus` (= `paused`); `-` when there is no task. Machine-readable equivalent: `task_status` in `--json` fleet rows. |
| `STATE` | session-level reading: `dead` for a daemon-marked session; `idle` when nothing is running (a `done` task means idle - `TSTAT` carries the task's actual end state); otherwise the latest task's status - `queued`, `dispatch` (= `dispatch_decided`), `running`, `awaiting` (= `awaiting_user`), `paused`, `failed`. |
| `Q` | questions asked on the task, `n/3` (`MAX_QUESTIONS_PER_TASK`). |
| `FAIL` | task attempt counter, `n/2` (`MAX_ATTEMPTS`). |
| `SEEN` | liveness age: heartbeat age for unmanaged sessions, turn-activity recency for managed ones (`-` when none). |
| `WAKE` | last recorded wake outcome plus unconfirmed streak / escalation flag (`-` when never woken). |

Footer digest (rolling 24h): `24h: done X | auto-decisions Y |
escalations Z` - tasks finished, DELIBERATION audit rows written,
ESCALATION audit rows written.

Overflow: `+N more session(s) - see --json`.

`--json` payload (`data` keys, uncapped): `attention[]`,
`vitals.daemon {running, pid, heartbeat_age_seconds, state_dir}`,
`vitals.orchestrator {session_id, status, heartbeat_age_seconds}`,
`fleet[]` (`registered`, `role`, `mode`, `repo`, `session_status`,
`task_id`, `task_label`, `task_status`, `attempt`, `questions`,
`heartbeat_age_seconds`, `state`), `digest_24h {done, auto_decisions,
escalations}`, plus the legacy `sessions[]` per-session views -
including `log_path` and `stderr_tail` for failed tasks, which are
available only in JSON output, not in the human table. Top-level
envelope fields: `queue_depth` (true COUNT, never capped),
`queue_shown`, `queue_truncated`, `in_flight_dispatch`,
`concurrency {active, max}` (acp-push semaphore, max 3),
`saturation_reason` when admission was refused.

Caveat: `bridge status` is not read-only - like every mutating
command it runs a pump pass (lease recovery, then dispatch of queued
tasks) before rendering.

## Command reference

Global flags: `--db PATH` (`SESSIONS_BRIDGE_DB`), `--queue-dir DIR`
(`SESSIONS_BRIDGE_QUEUE_DIR`), `--json` on every command. Exit codes:
0 ok, 2 usage, 3 not found, 4 conflict, 5 internal.

| Command | Effect |
| --- | --- |
| `bridge push <sid> --task "..."` / `--file <path>` | enqueue a task; pump picks the transport: managed session > outbox notification (mailbox) + `dispatch_decided` mark, locked session > hook queue, idle session > acp push worker. Push guard (item_152): a dead sid is **refused** before enqueue (undrainable by every transport); an unmanaged sid still dispatches via its transports but the response carries a warning, the human line is tagged `[UNMANAGED TARGET]`, and the task `note` is flagged so `bridge inspect` surfaces the risk. |
| `bridge claim <task_id> --session SID` | managed worker claims its delivered task: `queued`/`dispatch_decided` > `running` + lease. Idempotent - a repeat claim on the session's own running task renews the lease (long-task heartbeat). Refusals: wrong session, task already claimed/running/done/failed, `queue_paused`. |
| `bridge finish <task_id> --session SID [--fail] [--note T]` | close the managed lifecycle: `running` > `done` (or `failed` with `--fail`), CAS'd on the claiming session's deterministic lease owner. |
| `bridge wait <task_id> --session SID [--reason T]` | external-wait signal (item_22): the claiming session renews its lease by one `LEASE_SECONDS` while blocked out-of-band (CI, reviews, partner consults) and the reason lands in the task note. Same owner CAS as claim/finish; a dead claimer stops renewing and the task requeues within one lease period. |
| `bridge status` | fleet view above. |
| `bridge inspect <sid\|task_id>` | full task detail + next action. |
| `bridge clear <sid>` | drop the session's queued task. |
| `bridge unpark <sid>` | resume a paused/awaiting session (requeues paused tasks, clears the queue pause). |
| `bridge retry <sid\|task_id>` | requeue a failed task; attempt resets. |
| `bridge quota-park <sid> --task N --reset-at <iso\|HH:MM UTC> [--note T] [--model M] [--checkpoint T]` | manually park a live task into `waiting_quota` (v0.7.0, task #216): the step state stores orthogonally to the lifecycle - the task stays live, keeps its claimant lease, spends no attempt, is skipped by `recover_stale`/dead-worker fold and cannot be re-claimed; daemon wakes and `bridge wake` suppress until the reset, then the quota pass clears the park and sends one control probe. A `bridge event`/`bridge notice` body with explicit rate-limit text + a parseable reset auto-parks too (self-report channel); `rpc_error`/`busy`/`timeout`/`lease expired` never park. |
| `bridge quota-resume <sid> --task N` | release a `waiting_quota` park before its reset (operator override; refuses on non-parked tasks and halted sessions - HALT is senior). |
| `bridge arm <sid>` | enroll the session into managed mode; also clears a leftover `queue_paused` flag so tasks stay claimable. |
| `bridge disarm <sid>` / `bridge disarm --all` | leave managed mode + send a `disarm` mailbox message. |
| `bridge halt <sid>` / `bridge halt --all [--stop-daemon]` | graceful fleet stop: sets `halted_at`, queues the `halt` semantic and wakes; `--all` reports each target's wake outcome, mailbox depth and live task; `--stop-daemon` SIGTERMs the supervisor after the broadcast (v0.3.11). |
| `bridge resume <sid>` / `bridge resume --all` | lift a halt: clears `halted_at`, queues the `resume` semantic and wakes the session (v0.3.11). |
| `bridge unregister <sid> [--force]` | remove a dead/ghost registration and all its state (v0.3.14): refuses when a live task exists, when the heartbeat is fresh, or when the acp-host still lists the session (`--force` skips the two liveness legs; the live-task refusal always applies). Deletes sessions/heartbeat/tasks/notifications/questions/schedules/wake_state rows and the mailbox dir - `audit_events` and the decision journal keep the audit trail. Since v0.3.15 the mailbox is tombstone-renamed before the delete commits: a refusal restores it, a success removes the tombstone, and mail landing in a recreated `queue/<sid>` during the delete is post-delete state that survives (`mailbox_dir_recreated`/`mailbox_tombstoned` in the JSON output). |
| `bridge kill <sid>` | force-terminate a session's live bridge state without `unregister`'s liveness refusals or row deletion (v0.5.0): the session flips to `dead`, `running`/`awaiting_user` tasks requeue with leases released (a resurrected session re-dispatches them through the normal pump/claim path - `queued` is the only claimable re-entry state), staged `dispatch_decided` rows fold back to `queued`, and the heartbeat row is removed so metronome writes cannot keep the corpse warm. `queued` tasks stay queued. All effects land in one transaction with a `session_killed` `decision_events` row. Idempotent on an already-dead row (a no-op repeat writes no duplicate event); `not_found` on an unregistered sid. Kill detaches bridge state, it is not a ban - a still-alive session self-revives on its next `register` (unmanaged also on `heartbeat`). |
| `bridge register <sid> [--role worker\|orchestrator] [--workspace DIR] [--force]` | join the bus and stamp the heartbeat; orchestrator is a CAS singleton. |
| `bridge ask --session SID --task ID "text"` | post a worker question; refused when another question is pending (`pending_exists`), the task hit the 3-question budget (`question_limit`), or no active orchestrator exists (`no_orchestrator`). |
| `bridge ack --session SID [--up-to N]` | advance the session's mailbox ack watermark. |
| `bridge directive <sid> "<text>"` | send a registered directive notification: the daemon relays it into the fs mailbox and the wake legs see it. Prefer this over hand-writing `msg_*.txt` files - foreign files get wakes via the file-pending leg but carry no registry evidence (v0.3.4). |
| `bridge notice <sid> "<text>"` | send a registered informational notice - same delivery machinery as directive, but kind `notice` means "read and incorporate" rather than "obey": the path for worker status and task reports (v0.3.9). |
| `bridge fleet-metrics [--window 24h\|7d]` | read-only markdown fleet report (v0.6.0, directive 446): per-session task counters / `plan_delta` / `mail_stall` episodes / questions / median cycle time from the bridge db, per-repo fix-rounds + incidents + `verify_constraints` failures from each session-cwd's `.devin/cycle_log.jsonl`, orchestrator self-metrics (enqueue→done median, hungry-worker snapshot, escalations, mean `ask` answer time) and a flat incident table. The db opens `mode=ro` (skips init); sources and approximations are in ADR-0002. |
| `bridge heartbeat <sid>` | refresh the session's liveness record (bumps the heartbeat generation; no takeover semantics). |
| `bridge event <sid\|-> <kind> "<text>" [--task ID]` | append a row to the durable `decision_events` log (v0.3.16): kinds are lowercase slugs (`contract`, `plan_delta`, `review_verdict`, `milestone`, ... matching `^[a-z][a-z0-9_]{0,39}$`); `-` as the session id records a bus-level decision with no owning session. Survives restarts - contracts and verdicts no longer live in session memory only. |
| `bridge events [--session SID\|-] [--kind K] [--last N]` | read the `decision_events` log newest-first (default 50 rows, `--last` >= 1; `--session -` filters the bus-level rows with no owning session; `--json` for machine use). |
| `bridge reserve <type> <id> --project P [--purpose T] [--ttl S]` | atomically reserve a host resource (v0.3.17): OK = granted or same-owner renew, CONFLICT names the incumbent owner. Deploy pre-flight contract in docs/ALLOCATIONS.md. |
| `bridge release <type> <id> --project P [--force]` | release an allocation (owner-bound; `--force` = orchestrator arbitration only). |
| `bridge allocations [--type T] [--project P]` | list live host-resource allocations (TTL rows swept lazily on every call). |
| `bridge questions list [--session SID] [--task ID]` | list pending worker questions. |
| `bridge questions claim <qid> --claimant SID` | atomically claim a question for answering. |
| `bridge questions answer <qid> --claimant SID "text"` | answer a claimed question and notify the asker. |
| `bridge escalate show` | list open escalations. |
| `bridge escalate reply <id> A\|B\|defer\|abort\|override:<text>` | resolve an escalation (see Playbook 3). |
| `bridge daemon start\|stop\|status\|once\|run` | supervisor lifecycle (see above). |
| `bridge --version` / `bridge version` | true deployed version + source path (item_102a): reads the checkout's pyproject.toml when running from a `src/` tree - editable installs carry stale dist-info and `pip show` lies. `--json` carries it under `data.version`. |

## Session heartbeats

`worker_heartbeats.last_seen_at` is the liveness record the daemon
sweeps and `bridge status` reports. Two commands refresh it:
`bridge heartbeat <sid>` (lightweight, heartbeat only) and
`bridge register` (which also re-stamps + bumps the generation).
Mode matters (item_17): for UNMANAGED sessions a stale heartbeat
(>120s) means `status='dead'` - keep heartbeating on every watcher
cycle to stay alive. For MANAGED sessions the heartbeat is not the
death signal anymore - stale-hb between turns is normal and just
makes the session a wake candidate; a managed session only goes dead
via the no_session streak (or `disarm`). Heartbeats are still useful
for managed sessions though: they drive `bridge status` age columns
and the orchestrator CAS window.

## DoA matrix (summary)

The orchestrator decides autonomously and escalates to you only on a
whitelist: (1) an approved external contract must change or break,
(2) unspecified business logic (money, access, UX) including diffs
that touch the repo's declared protected paths, (3) safety/loop
breakers: >=2 failures of the same validation, >3 questions on one
task, destructive operations, no confident committee synthesis,
quota/turn anomalies, a directive that caused 2 consecutive failures
on a previously-passing baseline, or both quantum partners
unreachable >5min. The full matrix, anti-escalation rule, committee
protocol and escalation-report format are the orchestrator's
operating contract - `docs/orchestrator/PLAYBOOK.md` (copy it into
the target workspace as `.devin/orchestrator/PLAYBOOK.md`).

## Environment variables

| Variable | Default | Used for |
| --- | --- | --- |
| `SESSIONS_BRIDGE_DB` | `$XDG_DATA_HOME/sessions-bridge/state.db` | state.db location (overridden by `--db`). |
| `SESSIONS_BRIDGE_QUEUE_DIR` | `<state.db dir>/queue` | trigger + mailbox root (overridden by `--queue-dir`). |
| `SESSIONS_BRIDGE_DAEMON_INTERVAL` | 5 | seconds between daemon passes. |
| `SESSIONS_BRIDGE_NOTIFY_SEND` | `notify-send` on PATH | notify binary override. |
| `SESSIONS_BRIDGE_WORK_TIMEOUT` | 3600 | acp-push worker end_turn wait. |
| `SESSIONS_BRIDGE_PYTHON` | `.venv/bin/python` then `python3` | interpreter for installed hook commands. |
| `SESSIONS_BRIDGE_STOP_TIMEOUT` | 600 | Stop hook timeout written by install_hooks.sh. |
| `SESSIONS_BRIDGE_CYCLE_SECONDS` | 150 | park cycle length for installed hooks. |
| `SESSIONS_BRIDGE_CYCLES_MAX` | 5 | park budget for installed hooks. |
| `SB_STALL_THRESHOLD_S` | 1800 | undrained-mail age before the daemon escalates to the orchestrator. |
| `SB_STALL_REALERT_S` | 3600 | cadence for re-alerting a still-stalled session inside the same episode. |
| `SB_LEASE_TTL_S` | 3600 | claim-lease TTL applied at every lease write (v0.8.0); the auto-renew ceiling and worker-log retention scale with it. |
| `SESSIONS_BRIDGE_MAIL_STALL_SECONDS` | 1800 | legacy alias of `SB_STALL_THRESHOLD_S`; the canonical name wins when both are set. |
| `SESSIONS_BRIDGE_GHOST_TTL` | 86400 | seconds after `sessions.updated_at` before a no-heartbeat-row unmanaged registration (ghost) is swept dead. |
| `SESSIONS_BRIDGE_BUSY_GRACE_SECONDS` | 7200 (4x stall) | ceiling on mid-turn busy grace - a still-busy session escalates past it; clamped at the stall threshold. |
