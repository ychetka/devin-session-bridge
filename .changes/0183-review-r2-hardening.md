bump: minor
user_visible: true
issue: 183

Hard-stop and lifecycle hardening from the external architect's
round-2 audit: `bridge stop` now holds `halted_at` for the ladder's
duration (released after; parking stays `halt`'s contract), `kill
--hard-stop` stamps a durable `session_kill_started` marker the
daemon's new kill-teardown pass uses to finalize a CLI that died
mid-ladder, the interactive-TTY sweep exclusion narrows to direct
backend children (deeper pty tool calls stay sweepable), the
bridge daemon is protected from the sweep, pass-2 orphans with
cwd-only attribution are reported as `weak_suspects`,
`waiting_quota` parks release the session slot, `bridge wait` is
cumulatively bounded per claim (`SB_WAIT_MAX_S`, default 24h), and
`question_answer`/`task_dispatch` deliveries get stale guards at
relay and the janitor leg so a re-register never drains dead
prompts or answered-gone questions. `bridge finish` and `bridge
wait` gain `--note-file`/`--reason-file` payload options for notes
too large for argv (E2BIG).

## Spec impact
- modifies: docs/specs/sessions.md#layered-stop-reports-observed-outcomes-only
- modifies: docs/specs/tasks.md#external-waits-renew-the-lease-deliberately
- modifies: docs/specs/tasks.md#optional-cap-on-concurrently-active-sessions
- modifies: docs/specs/mailbox.md#stale-control-deliveries-are-dropped-not-executed
