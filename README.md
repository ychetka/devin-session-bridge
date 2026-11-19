# sessions-bridge

A CLI and Python library that dispatches tasks to local Devin sessions
without manual copy-paste. Two transports cover both session kinds:

- **ACP push** - for idle sessions: `session/load`, `session/prompt`,
  `session/close` against a headless `devin acp` process.
- **Hook queue** - for sessions open in Devin Desktop: lifecycle hooks
  installed into the worker repo park on the Stop event and poll the
  queue, injecting the next task inside the open session.

`state.db` (SQLite, WAL) is the single source of truth. There is no
daemon: every mutating command, `status`, and each exiting worker runs
a recovery + dispatch pass.

## Requirements

- Linux
- `devin` CLI >= 3000.11.x, authenticated
  (`~/.local/share/devin/credentials.toml` with `windsurf_api_key`)
- Python >= 3.10
- `uv` or `pip`
- Runtime dependency: `pydantic` only

## Install

```bash
pip install -e .
# or
uv pip install -e .
```

Then verify:

```bash
bridge status
```

A source checkout also works without installing: `./bridge.py status`.

## Install hooks into a worker repo

Hook delivery requires the bridge hooks in the target repo's
`.devin/hooks.v1.json`:

```bash
./install_hooks.sh /path/to/repo
```

The script is idempotent: existing non-bridge hook entries are kept, a
pre-existing `hooks.v1.json` is backed up to `hooks.v1.json.bak`, and
the Stop hook timeout is validated to cover the park cycle. Tune via
env: `SESSIONS_BRIDGE_PYTHON`, `SESSIONS_BRIDGE_STOP_TIMEOUT` (600s),
`SESSIONS_BRIDGE_CYCLE_SECONDS` (150s), `SESSIONS_BRIDGE_CYCLES_MAX`
(5).

## Usage

```bash
bridge push <session_id> --task "fix the flaky test"
bridge push <session_id> --file spec.md
bridge status                 # per-session overview
bridge inspect <session|id>   # full task detail + next action
bridge clear  <session>       # drop a queued task
bridge unpark <session>       # resume a paused/awaiting session
bridge retry  <session|id>    # requeue a failed task
```

Every command accepts `--json` and returns a machine-readable envelope
(`status`, `queue_depth`, `events[]`, `agent_instruction`, `data`) for
agent callers. `--db` / `SESSIONS_BRIDGE_DB` and `--queue-dir` /
`SESSIONS_BRIDGE_QUEUE_DIR` override storage locations (default:
`$XDG_DATA_HOME/sessions-bridge/state.db`, triggers in `queue/` next
to it).

## Model

- `state.db` holds sessions and tasks; `queue/<sid>.json` trigger
  files only wake a parked Stop hook and are never a store.
- Per-session states shown by `status`: `RUNNING`, `PARKED`,
  `HEADLESS-IDLE`, `NEEDS-KICK`, `FAILED`.
- `NEEDS-KICK` means the session waits for a user poke: sending a
  short prompt of <= 3 chars (a "." works) unpauses the queue and
  injects the queued task.
- Dispatch picks the transport automatically: a session locked by a
  live devin process gets the hook queue; an idle session gets an ACP
  push worker (at most 3 concurrent).
- Transitions to `NEEDS-KICK` / `FAILED` emit a deduped
  `notify-send -u critical` notification when the binary exists.

## Operating the bridge

Beyond single-session dispatch, the bridge runs a managed fleet:
`bridge arm`/`disarm` enroll sessions into mailbox delivery, the
`bridge daemon` supervisor relays the notifications outbox and marks
dead sessions, `bridge register`/`ask`/`ack`/`escalate` form the
orchestration bus, and `bridge status` renders the fleet view.

Operator runbook (mental model, playbooks, daemon lifecycle,
watchdog): [docs/OPERATOR.md](docs/OPERATOR.md)

Orchestrator session contract template:
[docs/orchestrator/PLAYBOOK.md](docs/orchestrator/PLAYBOOK.md)

## Safety invariants

- Desktop-open sessions are never force-closed; delivery to them goes
  through the hook queue only.
- `~/.local/share/devin/cli/sessions.db` and `session_locks/` are
  read-only for the bridge; only `state.db` is written.

## Testing

```bash
python -m pytest -q
```

`tests/e2e` exercises the full `push > state.db > worker > acp` chain
with a fake `devin` binary; the live-ACP case runs against real
`devin acp` when available and is skipped otherwise.
