# Mailbox

The message bus delivers notifications to sessions through an
outbox-plus-files scheme: a durable row in `notifications`, a
`msg_<nid>.txt` file in `queue/<session_id>/`, and an ack protocol
closing the loop.

### Requirement: notification delivery lands a queue file

The bridge SHALL deliver each notification as both a durable
`notifications` row and a `msg_<notification_id>.txt` file in the
target session's queue directory.

#### Scenario: push writes row and file

- **WHEN** a notice, directive, task dispatch or question is sent to a
  registered session
- **THEN** a `notifications` row exists with the session id and kind
- **AND** a `msg_<nid>.txt` file exists in `queue/<session_id>/`

### Requirement: drain protocol is file-driven

The receiving session SHALL drain its mailbox by reading `msg_*.txt`
files in `ls -v` order, deleting each after handling, and finally
running `bridge ack --session <sid>` to close the rows.

#### Scenario: consumed but unacked is not a stall

- **WHEN** a session deleted a `msg_*.txt` file mid-drain but has not
  acked its row yet
- **THEN** the notification counts as consumed-in-flight, not
  undrained
- **AND** the daemon does not escalate the session for it

### Requirement: undrained mail escalates once per episode

Undrained actionable mail older than `SB_STALL_THRESHOLD_S`
(default 1800s) SHALL trigger exactly one escalation to the
orchestrator per stall episode - a `[MAIL_STALL]` notice plus a
durable `mail_stall` decision event - and SHALL re-alert every
`SB_STALL_REALERT_S` (default 3600s) while the episode stays open.

#### Scenario: file age decides, not the row

- **WHEN** a `msg_*.txt` file has sat on disk past the threshold while
  its row is unacked
- **THEN** the daemon emits the stall escalation once
- **AND** a later pass inside the same episode is silent until the
  re-alert cadence lapses

### Requirement: stale control deliveries are dropped, not executed

Control-class deliveries (`halt`, `resume`, `disarm`) SHALL carry
their epoch (`issued_at` header) and be dropped at delivery when
stale - superseded by a newer control notification for the same
session, or older than `SB_CONTROL_TTL_S` (default 21600s). The drop
applies at both relay (stale outbox row is marked relayed without a
file) and at a janitor pass that unlinks stale control files already
sitting in mailboxes; every drop is journaled as
`stale_control_dropped`. When the daemon is not running, `bridge
halt`/`resume`/`disarm` run the relay and janitor synchronously so a
drain during the outage meets the same mailbox state. The durable
flag the command accompanied is unaffected - `halted_at` enforcement
lives in the database, the file is only a courtesy signal.

#### Scenario: superseded halt never reaches the drain

- **WHEN** a `halt` then a `resume` notification exist for one session
- **THEN** the `halt` delivery is dropped with a
  `stale_control_dropped` journal entry naming the superseding id
- **AND** only the `resume` file reaches the mailbox
