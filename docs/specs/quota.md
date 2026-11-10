# Quota - waiting_quota step state

Rate-limited work parks at step granularity: the task keeps its
lifecycle status and lease while `step_state=waiting_quota` records
that the current step is blocked on a provider quota reset (v0.7.0,
ADR-0003).

### Requirement: quota park is orthogonal to lifecycle

A task parked on quota SHALL keep its live `status` and its
claimant's lease - `step_state='waiting_quota'` records the blocked
step without spending attempts or freeing the task to other
claimants.

#### Scenario: parked task is invisible to recovery and claims

- **WHEN** a live task carries `step_state='waiting_quota'`
- **THEN** `recover_stale`, dead-worker requeue and lease auto-renew
  skip it
- **AND** claim/dispatch CAS paths refuse to promote it before the
  reset

### Requirement: auto-park needs an explicit limit signal plus reset

A `bridge event`/`bridge notice` body SHALL auto-park the reporting
task only when it carries an explicit rate-limit marker AND a
parseable reset time; generic errors never park. A wake or
dispatch-prompt transport error carrying the same marker parks the
affected live task the same way; on the transport surface a marker
without a reset parks for a bounded default window (900s), which the
quota pass re-probes at its end.

#### Scenario: rpc_error, busy, timeout, lease-expired never park

- **WHEN** a report or transport error carries no quota marker
- **THEN** no park happens
- **AND** a self-report marker without a reset records the signal but
  requires a manual `bridge quota-park` to park

#### Scenario: transport-surfaced rate limit parks the live task

- **WHEN** a wake or prompt `rpc_error` body carries a rate-limit
  marker and the session has a live task
- **THEN** the task parks as `waiting_quota` with the parsed reset -
  or the default window when the marker names none - instead of
  burning the retry streak or finishing the task failed
- **AND** the session drops out of every wake leg until that reset

#### Scenario: quota signal without a live task journals only

- **WHEN** a wake error carries a quota marker but the session has
  no live task
- **THEN** the signal is journaled as `quota_signal_seen` and
  ordinary `rpc_error` handling applies

### Requirement: reset releases the park with one control probe

When `quota_reset_at` passes, the daemon quota pass SHALL clear the
park, re-arm the lease for a full window and send exactly one control
probe to the session.

#### Scenario: undeliverable probe re-parks, bounded

- **WHEN** the released task's probe cannot be delivered (no lockfile,
  pacing, transport error)
- **THEN** the task re-parks for a short defer window
- **AND** after the per-episode defer limit the park releases to the
  ordinary lease lifecycle with a durable `quota_probe_gave_up` event

### Requirement: HALT is senior to a quota reset

A halted session's expired quota park SHALL stay parked - the quota
pass sweeps it only after `bridge resume` lifts the halt.

#### Scenario: halted session past reset

- **WHEN** `quota_reset_at` passed but the session is halted
- **THEN** the park remains and no probe fires until resume
