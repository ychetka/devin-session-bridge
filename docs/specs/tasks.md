# Tasks - lifecycle, leases, recovery

Tasks move through a closed status machine and are held by a
lease-owner CAS: exactly one claimant owns a live task at a time.

### Requirement: lifecycle follows the declared transitions

The bridge SHALL move tasks only along `TRANSITIONS`: `queued` >
`dispatch_decided` > `running` > `done`/`failed`, plus `awaiting_user`
and the manual `paused` park. `failed` is terminal - the only exit is
the manual `retry` command.

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
abandonment.

#### Scenario: wait beats expiry

- **WHEN** the lease owner calls `bridge wait <task_id>` before expiry
- **THEN** `lease_expires_at` moves out by one lease length
- **AND** a dead claimer that stops waiting requeues within one period

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
