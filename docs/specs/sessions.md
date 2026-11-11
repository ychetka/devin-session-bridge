# Sessions - registration, liveness, control

Sessions register in the bus, report liveness via heartbeats, and
answer to fleet-level control: halt, resume, kill, unregister.

### Requirement: managed sessions are orchestrator-owned

A session SHALL carry a role/mode distinguishing managed
(orchestrator-dispatched, lease-aware) from unmanaged registrations;
only managed sessions participate in task claims and auto-renew.

#### Scenario: unmanaged session never claims

- **WHEN** an unmanaged session tries to claim a managed task
- **THEN** the claim path refuses - managed tasks go through dispatch
  or owner-claim only

### Requirement: HALT pauses the fleet semantically

`bridge halt` SHALL stamp `halted_at` on the target session(s) so that
wakes, quota sweeps and escalations stop until `bridge resume` clears
it - HALT is senior to every automatic path. HALT is a fleet-control
flag only: it SHALL NOT be reported as task termination or process
cancellation - an acknowledged halt leaves the task's real lifecycle
status untouched (requested, delivered, acknowledged and terminal
are distinct states).

#### Scenario: halted session keeps its lease and work

- **WHEN** a session is halted mid-task
- **THEN** its task stays parked untouched (no recovery, no requeue)
- **AND** after `bridge resume` the ordinary lifecycle resumes exactly
  where it stopped

#### Scenario: halt never certifies cancellation

- **WHEN** a halt is requested, delivered or acknowledged on a
  session whose worker is mid-turn
- **THEN** no task row flips to a terminal status on that fact
  alone - only `bridge withdraw`/`kill` or the worker's own
  finish/fail write the terminal verdict

### Requirement: kill removes the session, not its work

`bridge kill` SHALL unregister a session and fold its live tasks back
to `queued` for re-claim - work survives the worker.

#### Scenario: killed session's task re-enters the pipeline

- **WHEN** `bridge kill <sid>` lands on a session with a running task
- **THEN** the session row is removed and the task returns to
  `queued`, claimable by the next worker
- **AND** a second kill of the same session is an idempotent no-op

### Requirement: dead managed sessions revive on proven return

A managed session marked dead SHALL self-recover when two signals
coincide: pending mail exists in its mailbox (demand) AND a
session/list presence check reports it back (proof). The revive is
a CAS `dead > active` gated on the latest dead-causing decision
marker being a liveness verdict (`worker_dead`/`orchestrator_dead`)
- an operator `bridge kill <sid>` writes `session_killed` and is
never auto-revived. Unmanaged rows keep the heartbeat revive path
and are never probed; halted rows are never probed either.

#### Scenario: resurrect probe revives on demand

- **WHEN** a dead managed session has undrained `msg_*.txt` and
  session/list reports it present
- **THEN** the daemon flips it back to `active`, journals
  `session_revived`, and the ordinary mail leg wakes it in the
  same pass
- **AND** a sid whose latest marker is `session_killed` stays dead
  and is never probed

### Requirement: stale heartbeats mark sessions dead

A session whose heartbeat falls silent past
`HEARTBEAT_STALE_SECONDS` SHALL be treated as dead for wake and
lease purposes - silence is never evidence of health.

#### Scenario: ghost session cleanup

- **WHEN** an unmanaged registration never produced a heartbeat row
  past `SESSIONS_BRIDGE_GHOST_TTL`
- **THEN** the lazy GC marks it dead instead of letting it pin state
