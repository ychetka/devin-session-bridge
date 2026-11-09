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
