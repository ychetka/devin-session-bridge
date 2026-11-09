# Wake - daemon nudge path

The daemon keeps managed sessions progressing by nudging them on two
legs - pending mailbox files and liveness - under a wake policy that
bounds cadence and failure loops.

### Requirement: mailbox files wake the session

A session with undrained `msg_*.txt` files SHALL be a wake candidate:
the daemon nudges it so the drain protocol runs at the next turn
boundary.

#### Scenario: file present, session idle

- **WHEN** a `msg_*.txt` exists in the session's queue directory and
  the session is not already mid-turn
- **THEN** the daemon delivers one wake within the policy cadence

### Requirement: wake policy bounds retries

Wake attempts SHALL honor `WakePolicy` cooldown between nudges and a
breaker that stops waking a session whose nudges keep failing.

#### Scenario: failing nudges trip the breaker

- **WHEN** a session's wake deliveries fail repeatedly
- **THEN** the policy stops issuing new nudges for that session until
  the breaker resets

### Requirement: quota-parked tasks suppress wakes

A session whose live task is `waiting_quota` SHALL NOT be woken
while `quota_reset_at` is in the future - the reset window, not the
nudge cadence, decides the next step.

#### Scenario: manual wake is also suppressed

- **WHEN** `bridge wake` targets a session with a future
  `quota_reset_at`
- **THEN** the command returns `suppressed` with the reset time
  instead of nudging

### Requirement: HALT suppresses every wake leg

A halted session SHALL receive no wakes at all - file-pending and
liveness legs both respect the fleet pause until `bridge resume`.

#### Scenario: halted session with pending mail

- **WHEN** a halted session has undrained mail
- **THEN** the daemon sends no nudge until the halt lifts
