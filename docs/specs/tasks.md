# Tasks - lifecycle, leases, recovery

Tasks move through a closed status machine and are held by a
lease-owner CAS: exactly one claimant owns a live task at a time.

### Requirement: lifecycle follows the declared transitions

The bridge SHALL move tasks only along `TRANSITIONS`: `queued` >
`dispatch_decided` > `running` > `done`/`failed`, plus `awaiting_user`
and the manual `paused` park. `failed` is terminal - the only exit is
the manual `retry` command. `cancelled` is terminal likewise: it is
reachable from every still-actionable status but only via the manual
`withdraw` edge - no automatic writer may silently cancel a task.

#### Scenario: failed never auto-requeues

- **WHEN** a task reaches `failed` for any reason
- **THEN** no automatic path moves it again
- **AND** only `bridge retry` returns it to `queued` with `attempt=0`

### Requirement: claims are lease-owner CAS writes

Every claim or status transition SHALL be a compare-and-swap on the
task row, guarded by status and lease ownership - two claimants can
never hold the same task.

#### Scenario: second claimant is refused

- **WHEN** a live task already carries `lease_owner=<session>`
- **THEN** a claim by a different owner fails the CAS
- **AND** the task row is unchanged

### Requirement: leases expire on the configured TTL

A claim or lease renewal SHALL stamp `lease_expires_at` with
`now + LEASE_SECONDS` (default 3600s, overridable via `SB_LEASE_TTL_S`
with a 600s floor). An expired lease makes the task visible to
`recover_stale`, which requeues it or fails it at `MAX_ATTEMPTS`.

#### Scenario: stale lease recovery

- **WHEN** `lease_expires_at < now` on a live task
- **THEN** the next recovery pass returns it to `queued` without
  spending the whole attempt budget unless attempts are exhausted
- **AND** a task parked `waiting_quota` is skipped entirely

### Requirement: auto-renew covers busy managed sessions

While a managed+active session is verifiably busy, the daemon SHALL
auto-renew its task leases inside `SESSIONS_BRIDGE_LEASE_RENEW_MARGIN`
(default 300s) before expiry - but never past the explicit-touch
ceiling `SESSIONS_BRIDGE_LEASE_AUTO_RENEW_SECONDS` (default 6x lease).

#### Scenario: wedged turn cannot hold forever

- **WHEN** the last worker-initiated lease touch ages past the ceiling
- **THEN** auto-renew stops and expiry recovery takes over

### Requirement: external waits renew the lease deliberately

`bridge wait` SHALL extend the caller's task lease by one
`LEASE_SECONDS` per call under the same owner CAS as claim/finish -
an out-of-band block (CI, reviews, partner consults) must not read as
abandonment. A `running` task whose `wait_until` stamp is still fresh
SHALL render as a parked/waiting phase in `bridge status` - the
per-session view reads PARKED with `external wait: <reason>` as its
action, the fleet row shows `ewait` in TSTAT and `waiting` in STATE,
and `--json` carries `wait_until`/`wait_reason` - without any
lifecycle change. Un-parking is a mail delivery, not a status flip:
a `bridge notice` or `bridge directive` to the session wakes it, and
the waiter drains and finishes/fails the task itself. No external
poller (GitHub, CI) is part of the design - the resolution signal
comes from the side that knows the condition resolved.

#### Scenario: wait beats expiry

- **WHEN** the lease owner calls `bridge wait <task_id>` before expiry
- **THEN** `lease_expires_at` moves out by one lease length
- **AND** a dead claimer that stops waiting requeues within one period

#### Scenario: fresh wait stamp reads parked, stale decays alone

- **WHEN** a `running` task's `wait_until` is still ahead of now
- **THEN** `bridge status` shows the external-wait phase with its
  declared reason in the human view and `--json`
- **AND** once the stamp ages past now the row reads `running` again
  with no clearing write

### Requirement: status digest reports lease and wait pressure

The `bridge status` 24h digest SHALL count `lease_expired` - tasks
stamped `lease expired` by `requeue_or_fail` inside the window - and
SHALL report `waiting_external`, a current-snapshot gauge of
`running` tasks whose `wait_until` stamp is still fresh. Both fields
SHALL appear in the human footer (`waiting-external` only when >0)
and in the `--json` payload.

#### Scenario: expiry and parked waiter are countable

- **WHEN** a lease fold requeues a task inside 24h and another task
  is wait-parked right now
- **THEN** the digest counts each under `lease_expired` and
  `waiting_external` respectively
- **AND** notes other than `lease expired` and stamps already past
  now are not counted

### Requirement: operator kill force-fails one live task

`bridge kill <task_id>` SHALL CAS `running|dispatch_decided` to
`failed` ignoring `lease_owner` - the operator verdict is senior to
the lease and to a `waiting_quota` park (the step slot clears with
the terminal write). Lease fields clear, `finished_at` stamps, a
dirty-tree note instructs the owning worker to stash and reset, and
a durable `task_killed` decision event journals in the same
transaction. The owning session SHALL receive a NOTICE when it is
registered and active, so its mailbox wakes it to drop the dead
work.

#### Scenario: wedged task dies without waiting out the lease

- **WHEN** a running task is force-failed by `bridge kill <task_id>`
- **THEN** it is `failed` with the lease released and the step slot
  cleared
- **AND** the owning session's outbox gains a NOTICE

#### Scenario: out-of-scope statuses are refused, not touched

- **WHEN** `bridge kill` targets a `queued`, `awaiting_user`, `done`
  or `failed` task
- **THEN** the command refuses with the current status
- **AND** no state or event row is written

### Requirement: orchestrator withdraw marks tasks cancelled

`bridge withdraw <task_id>` SHALL CAS any still-actionable status
(`queued`, `dispatch_decided`, `running`, `awaiting_user`, `paused`)
to terminal `cancelled` - the benign scope-pull `bridge kill` cannot
express: kill is a force-FAIL verdict for wedged work and reaches
only `running|dispatch_decided`. The withdraw SHALL journal a
`task_withdrawn` decision event naming actor and reason (the audit
is authored by the withdrawing side, never impersonating the
worker), clear the lease and step/quota slots, stamp `finished_at`,
and enqueue a NOTICE to the owning session - all inside one
`BEGIN IMMEDIATE`, so the session's live-task index slot frees in
the same write (no ghost slot). Late `finish`/`renew` from a worker
that never saw the notice SHALL be refused by their own CAS
predicates. Terminal rows (`done`, `failed`, `cancelled`) SHALL be
refused, which makes a repeat withdraw idempotent - no duplicate
event or notice. `--stop-turn` is an explicit optional extra that
emits a best-effort `session/cancel` over acp-host and reports the
outcome honestly (`idle`/`confirmed`/`requested`/`failed:<reason>`)
- it never claims the worker process or session died.

#### Scenario: running task is pulled mid-work

- **WHEN** the orchestrator withdraws a `running` task
- **THEN** the row is `cancelled` with lease cleared and
  `finished_at` stamped in the same transaction as the
  `task_withdrawn` event
- **AND** a late `bridge finish` or `bridge wait` by the worker
  fails its CAS and changes nothing

#### Scenario: withdrawal is idempotent

- **WHEN** a task is already `cancelled` (or any terminal status)
- **THEN** a further `bridge withdraw` refuses with the current
  status and writes no second event or notice

#### Scenario: halt plus withdraw is consistent

- **WHEN** a task is withdrawn while its session is halted or its
  turn is busy on the host
- **THEN** the row still reaches `cancelled` - task state never
  depended on turn delivery, and `--stop-turn` merely requests the
  backend interrupt without certifying the process stopped

### Requirement: optional cap on concurrently active sessions

When `SB_MAX_ACTIVE_SESSIONS` is set above zero, a `queued` task
SHALL transition toward `running` only while fewer than that many
distinct sessions hold a live (`running` or `dispatch_decided`)
task - enforced inside the `claim_queued`, `claim_managed` and
`dispatch_managed` CAS predicates so every claim edge (pump, Stop
hook, user kick, managed claim) honors the cap atomically. A
`dispatch_decided` row's self-claim is exempt: its slot was spent
at stage time. A task refused by the cap stays `queued` and is
counted as slot-waiting in `bridge status`, which SHALL report the
active session count, the configured cap, and the backlog. A
default of `0` means unlimited.

The configured value SHALL be durable: the daemon persists its
env-resolved cap to `bridge_settings` at startup, and every claim
CAS, the pump pre-filter, and the status report read that stored
value - a `bridge` subprocess that never inherited the daemon's
environment enforces the same limit, falling back to its own env
read only when no daemon has ever published one. The cap counts
registered sessions holding live tasks only; it declares no bound
on child-agent concurrency inside a session, which the bridge
schema does not track at all.

#### Scenario: queued task waits for a free session slot

- **WHEN** `SB_MAX_ACTIVE_SESSIONS=1`, one session holds a running
  task, and a queued task targets a different session
- **THEN** the pump leaves it `queued` and reports it as
  slot-waiting in `bridge status`
- **AND** once the running task finishes, the next pass dispatches it

#### Scenario: direct claim edges honor the cap

- **WHEN** the cap is held and a session's Stop hook or managed
  claim targets a still-`queued` task directly
- **THEN** the claim CAS refuses and the task stays `queued`
- **AND** a `dispatch_decided` task's own claim is unaffected

#### Scenario: unset cap never blocks

- **WHEN** `SB_MAX_ACTIVE_SESSIONS` is unset or `0`
- **THEN** dispatch admits queued tasks regardless of how many
  sessions are active
