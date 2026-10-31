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
- `bridge daemon` is the supervisor: it relays the outbox to mailboxes
  and marks dead sessions. It never injects anything into a session.
- Managed watchers are the delivery inside an open turn: a background
  watcher subagent polls the mailbox, prints and removes pending
  `msg_*` files and exits; its completion lands inside the parent's
  open turn, which processes the message and re-arms a new watcher.
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
  v
managed watcher subagent        = delivery into the open turn
  |  prints + removes msg files, exits
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
SID              REPO             TASK                 STATE       Q FAIL    HB
<orc-sid>        ai_controller    -                    idle        -    -   14s
<sid>            worker-repo      -                    idle        -    -   30s
24h: done 0 | auto-decisions 0 | escalations 0
```

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
      .devin/roadmap.md.
   4. Arm your inbox: spawn a background mailbox watcher subagent per
      PLAYBOOK.md. On its completion: process pending items and re-arm
      before ending your turn.
   5. Resume the roadmap. Decide autonomously within the DoA matrix;
      escalate to the human ONLY per the whitelist, in escalation-report
      format.
   ```

4. The session runs `bridge register <its-sid> --role orchestrator`. The
   orchestrator role is a CAS singleton: registration is refused while
   another session holds the role with a fresh heartbeat
   (`HEARTBEAT_STALE_SECONDS = 120`).

   Refusal output looks like:

   ```text
   refused: orchestrator <holder-sid> is alive (heartbeat 42s ago); use --force to take over
   ```

   Troubleshooting:

   - The old orchestrator session is actually still open: close it, or
     wait until its heartbeat goes stale (~120s after its last
     `bridge register`) and rerun register normally.
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
  so a hung pass ages the stamp (that is the watchdog signal).
- `daemon_decisions.jsonl` - one `{ts, event, sid, detail}` JSON line
  per decision: `daemon_start`, `relayed`, `relay_error`,
  `orchestrator_dead`, `worker_dead`, `pass_error`, `lock_refused`,
  `daemon_stop`.
- `logs/daemon.log` - the detached daemon's stderr (decision lines are
  echoed there too).

Each pass (~5s, with up to ~1s startup jitter):

1. Relay: every `notifications` row with `relayed_at NULL` becomes a
   `queue/<sid>/msg_<id>.txt` file (atomic write), then `relayed_at`
   is CAS-marked. Delivery is at-least-once; consumers dedup by
   notification id.
2. Sweep: sessions whose `worker_heartbeats.last_seen_at` is older
   than `HEARTBEAT_STALE_SECONDS` (120s) flip to `status='dead'`.
   A dead worker's live task is requeued (attempt +1, terminal
   `failed` at the `MAX_ATTEMPTS = 2` budget) carrying the note
   "requeued-dead-worker: stash dirty state (git stash -u -m
   dead-worker-backup-<id>) and reset the worktree to the task's base
   ref before starting". A silent orchestrator raises one deduped
   `notify-send -u critical`.

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
SID              REPO             TASK                 STATE       Q FAIL    HB
<orc-sid>        ai_controller    -                    idle        -    -   14s
worker-bee       devin-telegram   #7 fix flaky test    running   1/3  0/2   30s
24h: done 5 | auto-decisions 12 | escalations 1
```

Attention banner (printed only when needed):

- `ESCALATED #<id>: <trigger>` - an `audit_events` escalation row is
  open (directive NULL); the line carries the exact reply command.
  More than 5 collapse into `+N more open escalation(s) - bridge
  escalate show`.
- `ORCHESTRATOR DEAD` - the orchestrator session is marked dead or
  its heartbeat is past the 120s stale window; reopen the session and
  re-register (`--force` when the old holder is known-dead).

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
| `STATE` | `dead` for a daemon-marked session; `idle` when there is no live task (a `done` task means idle); otherwise the latest task's status - `queued`, `dispatch` (= `dispatch_decided`), `running`, `awaiting` (= `awaiting_user`), `paused`, `failed`. |
| `Q` | questions asked on the task, `n/3` (`MAX_QUESTIONS_PER_TASK`). |
| `FAIL` | task attempt counter, `n/2` (`MAX_ATTEMPTS`). |
| `HB` | seconds since the session's last heartbeat stamp (`-` when none). |

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
| `bridge push <sid> --task "..."` / `--file <path>` | enqueue a task; pump picks the transport: managed session > outbox notification (mailbox), locked session > hook queue, idle session > acp push worker. |
| `bridge status` | fleet view above. |
| `bridge inspect <sid\|task_id>` | full task detail + next action. |
| `bridge clear <sid>` | drop the session's queued task. |
| `bridge unpark <sid>` | resume a paused/awaiting session (requeues paused tasks, clears the queue pause). |
| `bridge retry <sid\|task_id>` | requeue a failed task; attempt resets. |
| `bridge arm <sid>` | enroll the session into managed mode. |
| `bridge disarm <sid>` / `bridge disarm --all` | leave managed mode + send a `disarm` mailbox notice. |
| `bridge register <sid> [--role worker\|orchestrator] [--workspace DIR] [--force]` | join the bus and stamp the heartbeat; orchestrator is a CAS singleton. |
| `bridge ask --session SID --task ID "text"` | post a worker question; refused when another question is pending (`pending_exists`) or the task hit the 3-question budget (`question_limit`). |
| `bridge ack --session SID [--up-to N]` | advance the session's mailbox ack watermark. |
| `bridge heartbeat <sid>` | refresh the session's liveness record (bumps the heartbeat generation; no takeover semantics). |
| `bridge questions list [--session SID] [--task ID]` | list pending worker questions. |
| `bridge questions claim <qid> --claimant SID` | atomically claim a question for answering. |
| `bridge questions answer <qid> --claimant SID "text"` | answer a claimed question and notify the asker. |
| `bridge escalate show` | list open escalations. |
| `bridge escalate reply <id> A\|B\|defer\|abort\|override:<text>` | resolve an escalation (see Playbook 3). |
| `bridge daemon start\|stop\|status\|once\|run` | supervisor lifecycle (see above). |

## Session heartbeats

`worker_heartbeats.last_seen_at` is the liveness record the daemon
sweeps and `bridge status` reports. Two commands refresh it:
`bridge heartbeat <sid>` (lightweight, heartbeat only) and
`bridge register` (which also re-stamps + bumps the generation), so a
session that must stay "alive" for longer than the 120s stale window
should run `bridge heartbeat` on every mailbox-watcher cycle (the
orchestrator does this per its watcher loop; see
docs/orchestrator/PLAYBOOK.md).

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
