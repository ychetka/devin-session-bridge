# Hard-stop for managed sessions: kill/stop semantics gap

Analysis of the 2026-11-13 incident and the design for a real
hard-stop path. Status: implemented - the layered ladder shipped in
v0.15.7/v0.15.8 (issues #164, #167) and the env-marker attribution
of section 6 in v0.15.9 (issue #168).

## 1. Incident

- `bridge kill peat-germanium` ran to completion:
  `status dead->dead, 0 task(s) requeued, 0 staged folded`,
  heartbeat row absent, and task 334 requeued for redispatch.
- The actual Devin Desktop session **kept running for ~20 more
  minutes** - it continued spawning subagents, dialing the phone,
  and writing artifacts.
- The human clicked the X on the "a turn is already running"
  Desktop banner repeatedly; the turn did **not** cancel.
- The dialing only stopped when the orchestrator killed host
  processes by PID; the turn ended when the worker's executor
  finished on its own.

The orchestration layer declared the session dead while the
session was demonstrably alive and acting. That is the gap.

## 2. What `bridge kill` actually does today

`bridge kill <sid>` (`cli/commands/sessions.py:660-701`,
`db.kill_session` in `state/db.py:2457-2590`) is one `BEGIN
IMMEDIATE` transaction on the bus database:

- `running|awaiting_user` tasks -> `queued` (lease released);
- `dispatch_decided` -> `queued`;
- `DELETE FROM worker_heartbeats`;
- `sessions.status -> 'dead'`;
- `session_killed` `decision_events` row (audit).

It sends **no** `session/cancel` and signals **no** host process.
The db.py docstring states this is deliberate: "Kill detaches
bridge state... cutting that off needs the process or its hooks
stopped first, which is the operator's side of the contract." The
contract exists on paper; no bridge verb implements the process
side.

`cancel_turn` (`wake/acp_host.py:633-731`) already implements a
foreign-attach `session/cancel` with honest outcomes
(`idle`/`confirmed`/`requested`/`WakeError`), empirically proven
in issue #137 (~3s turn collapse, `stopReason=cancelled`). It is
wired to **exactly one** command: `bridge withdraw <task_id>
--stop-turn`. `halt`, `disarm`, `unregister`, and `kill` never
call it - so during the incident there was literally no bridge
verb for "stop session X's turn."

## 3. Why a cancel may still not stop a live turn

- **Turn-scoped.** `session/cancel` aborts the session's live
  turn, not its child processes or the next turn
  (`docs/PARENT_CHILD_SESSIONS.md`). Detached exec shells and
  long-lived subagent work can outlive the cancelled turn.
- **Uninterruptible tool calls.** A turn parked inside a
  long-running tool call (a phone-dial MCP call, a wedged
  `terminal/wait_for_exit`) may not honour the cancel until the
  tool returns - upstream ACP issue reports document this wedge
  class (background child holding terminal pipes blocks cancel
  and `session/close` alike).
- **Re-arm loop.** Managed duty requires the session to re-arm a
  mailbox watcher and the SessionStart hook re-injects duty: turn
  N cancelled -> turn N+1 opens immediately. From outside it looks
  like the cancel "did nothing."
- **UI cancel.** The Desktop "a turn is already running" X
  appears to drive the same cancel path; when the turn is wedged
  inside an uninterruptible tool call it has no more effect than
  the wire notification did.

## 4. The host process model (verified live, 2026-11-13)

The decisive fact for hard-stop design:

- One shared backend process `devin acp` (observed PID 1509247,
  child of the `windsurf-cli ... agent acp-host` supervisor in
  `~/.devin-server/cli/acp-host-stable.lock` -> `{pid, port,
  token}`) owns **all** local sessions, including Desktop-open
  ones.
- `~/.local/share/devin/cli/session_locks/<sid>.lock` holds a
  single PID, and every live managed session (`wonderful-aster`,
  `buttered-tilapia`, `shore-felidae`, `stone-ellipse`,
  `peat-germanium`) resolves to **the same backend PID**.
  There is no per-session OS process: a `kill(lockfile-pid)` is a
  **fleet kill**, never a session kill.
- Direct children of the backend PID are a mix of shared
  infrastructure and session-attributable work:
  - **shared:** MCP partner servers (`mcp_partner.py`,
    `hands-agent/mcp_server.py`, `devin-telegram mcp`,
    `mcp-server-mikrotik`) - long-lived, owned by the agent
    config, must never be swept;
  - **attributable:** exec shells (`bash -c ...`) run with
    `cwd` inside the session's `working_directory`; watcher
    loops and drain observers embed `queue/<sid>` /
    `bridge ... --session <sid>` in their cmdline; the
    sessions-bridge hooks `worker.py` child receives the sid
    on stdin only (no cmdline marker).
- `session/list` `_meta` exposes `lockHolderPid` per session
  (contract.py already parses it); it currently resolves to the
  same shared backend PID and is therefore informational, not a
  kill target.

## 5. Design: layered hard-stop

Goal: one operator verb that (a) stops the live turn, (b) tears
down what the turn spawned, (c) flips bus state - in that order,
each layer best-effort with honest outcomes. New command
`bridge stop <sid> [--hard]` (and `bridge kill <sid>` gains an
opt-in `--hard-stop` flag that runs layers 0-2 before the DB
transaction, rather than changing kill's contract silently).

### Layer 0 - turn cancel (existing primitive, new wiring)

Call `cancel_turn(sid)` via the acp-host attach. Proven ~3s on a
live busy session. Outcomes `idle`/`confirmed`/`requested` are
reported verbatim in the command JSON and recorded on the
`session_stopped` audit row.

### Layer 1 - session close, forced (escalation after grace)

If the session still reports busy after a short confirm window
(default ~5s, configurable), send `session/close` with
`_meta.cognition.ai/force=true` - the strongest session-scoped
primitive the ACP host exposes (contract.py:178-192). If it is
rejected or the session persists, report `close_failed` and
continue to layer 2; a wedged turn inside an uninterruptible
tool call may not survive this layer either, which is exactly
why layer 2 exists.

Layer-1 escalation contract (issues #167, #179): the close fires
when layer 0 left the session busy (`cancel_outcome=requested`)
OR when the cancel transport failed (`failed:<reason>`) - a
failed cancel means the session state is UNKNOWN, and a live
session skipped here would respawn fresh children after the
layer-2 sweep (architect review r1 finding 3). Only the observed
goal states `idle`/`confirmed` skip the close, recorded as
`skipped:<outcome>`.

### Layer 2 - attributed orphan sweep (new code)

Enumerate and signal host processes attributable to the session:

- Root of enumeration: the `devin acp` backend PID (from the
  acp-host lockfile's `pid`, validated by cmdline match) - never
  signalled itself.
- A child is session-attributable iff:
  - `readlink(/proc/<pid>/cwd)` is under the session's
    `working_directory` (from `sessions.db` / bridge registry),
    OR
  - its cmdline contains `queue/<sid>`, `--session <sid>`, or
    the session mailbox path.
- Exclusions (never signalled, in blast-radius order): the
  backend and acp-host supervisors themselves; any process whose
  markers, env or cwd name a different registered session;
  shared host-level user daemons (`gpg-agent`, `ssh-agent`,
  `dbus-*`, `dconf-service`, `gnome-keyring-d`, `gvfsd`,
  `dmypy`, `git fsmonitor--daemon`, ...) that inherit whatever
  env the spawning session had and are shared by the whole host
  (review r1 finding 11); then, on the attribution legs, the
  target's own `DEVIN_SESSION_ID` env marker outranks the
  coincidental `mcp` cmdline substring (review r1 finding 9),
  the MCP/tool server whitelist, explicit sid markers, and the
  workspace-cwd fallback - which additionally refuses processes
  holding a controlling TTY (a human's interactive shell sharing
  the workspace, review r1 finding 10) and anything spawned in
  the backend's startup window.
- Signal order: SIGTERM, short grace, SIGKILL to survivors.
- Every swept PID + attribution reason is recorded on the
  `session_stopped` decision event and returned in the JSON.

Layer 2 is deliberately opt-in (`--hard`) for v1: heuristic
attribution can mislabel a shared-workspace process. A
`--dry-run` mode lists would-be victims without signalling.

### Layer 3 - bus teardown (existing)

`db.kill_session` unchanged - the bus row flips dead, tasks
requeue, heartbeat is removed, and the `session_killed` audit
row records that layers 0-2 ran first (new fields:
`cancel_outcome`, `close_outcome`, `swept_pids[]`).

Pre-ladder halt (review r1 finding 4): `bridge kill --hard-stop`
sets `halted_at` BEFORE layer 0 runs, so the multi-second
ladder cannot race a pump/relay wake or a new claim - the flag
closes the window the dead-row teardown would otherwise leave
open, and is harmless once the row lands in `dead`.

## 6. Env-marker attribution (implemented, issue #168)

`DEVIN_SESSION_ID=<sid>` in a process's **exec-time environment**
(the only form `/proc/<pid>/environ` can attribute - an `export`
inside a running shell never shows up there, verified live) is a
STRONG marker equal to the cmdline markers: it sweeps regardless of
process age and is recorded as `env:DEVIN_SESSION_ID`
(`orphan_env:DEVIN_SESSION_ID` in the reparented-orphan pass).
An env naming a DIFFERENT registered session is a foreign exclusion
exactly like a foreign marker hit; a missing/foreign-unregistered
value is dead text and the cwd/marker heuristics decide.

The marker is injected ONLY where the bridge genuinely controls the
process environment at spawn - pretending otherwise would stamp
wrong or dead markers:

- **acp-push workers** (`dispatch._spawn_worker`): every
  `bridge _work` child is born marked; the worker's own `devin acp`
  subprocess and every process that backend spawns for the session
  inherit it - the deepest coverage the bridge owns.
- **Managed-session watcher + drain observer**
  (`mailbox.watcher_command_text`, the duty text): the rendered
  commands carry a `DEVIN_SESSION_ID=<sid>` env-assignment prefix
  (heredoc-wrapped watcher loop, `bash -c`-wrapped observer). This
  is command text the session runs - coverage is best-effort,
  LLM-mediated, not guaranteed.
- **Hook worker** (`hooks/worker.py`): the sid arrives on stdin,
  after exec - so the worker re-execs itself once with the marker
  plus the drained payload relayed via
  `SESSIONS_BRIDGE_HOOK_PAYLOAD` (a parked Stop hook is exactly the
  lingering-orphan class the marker exists for). Children it spawns
  (inotifywait) inherit the `os.environ` write regardless; a failed
  exec degrades to child-only marking.

Surfaces deliberately NOT marked (documented so coverage gaps are
honest, not bugs):

- the shared `devin acp` backend and the acp-host supervisor:
  upstream-owned env, and a per-sid marker on the fleet-wide backend
  would be wrong by definition;
- exec shells the backend spawns for sessions delivered through the
  acp-host (wake/hook paths): their env comes from the backend, not
  from us;
- hook commands installed by `install_hooks.sh`: the command is
  per-repo and knows no sid at install time - the worker's re-exec
  covers the hook process itself instead;
- the bridge daemon, ingress forwarders, notify-send: host
  infrastructure with no session scope.

Blast radius (review 5926327596): the marker propagates by
inheritance through any spawn inside a marked subtree, so a
long-lived infra process launched from a marked shell would carry a
stale sid and become a pass-2 orphan-sweep target. The infra
entrypoints strip it twice: `ops._spawn_daemon` and
`ingress.sync._spawn` drop it from the child's exec-time env, and
`daemon.core.run` / `ingress.forwarder.main` pop the inherited copy
(`env_marker.unmark_env`) so nothing they spawn sees it either.

Marker-less or uncovered processes simply fall back to the existing
cwd/cmdline heuristics - partial coverage is a win, never a
misattribution vector.

## 7. Longer-term hardening (not v1)
- **Per-session cgroup:** the correct isolation model, but
  impossible while all sessions multiplex inside one `devin acp`
  backend - needs upstream host support (separate backend
  process per session or a documented per-session kill verb).
- **`devin stop <sid>` upstream:** worth filing - the CLI has
  `rm` (refused while open) but no turn-stop verb at all.

## 8. Implementation plan

1. `wake/acp_host.py`: add `close_session(sid, force=True)`
   beside `cancel_turn` (same attach plumbing).
2. New `session_hard_stop` module: attribution enumeration +
   term/kill sweep + dry-run.
3. `cli/commands/sessions.py`: `bridge stop <sid> [--hard]
   [--dry-run]`; `bridge kill <sid> --hard-stop` chains
   layers 0-3.
4. `decision_events`: extend `session_killed` /
   `session_stopped` payloads with the per-layer outcomes.
5. Tests: faked `/proc` tree + injected enumerator (existing
   fake-attach harness covers layers 0-1); contract tests for
   attribution predicates.

## 9. Correctness invariants

- Never signal the shared backend, acp-host, or MCP/tool
  servers - a fleet kill dressed as a session kill is worse than
  no kill.
- Every layer reports an honest outcome; "stopped" is only
  claimed when observed (re-list idle / process gone), never
  inferred from a sent request.
- The bus teardown runs last so a failed sweep never leaves the
  registry claiming alive while the session is actually dead.
