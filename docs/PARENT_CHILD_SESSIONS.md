# Parent-child session linkage (design note - not implemented)

**Status: design item - not implemented.** Nothing in this document
exists in code; it records the agreed shape so a future task can
implement it without re-deriving the semantics. Written for bridge
task 282 / issue #149.

## Motivation: the 2026-11-11 misroute incident

A spawned child task kept running under HALT and outside the
session-slot cap because the transport records no link between a
session and the work it spawns. Today the schema "tracks no
parent/child task linkage" (docs/OPERATOR.md,
`SB_MAX_ACTIVE_SESSIONS`): `bridge halt` parks the fleet's claim
and wake surfaces but cannot show which live rows belong to one
logical job, and `bridge status` renders a flat fleet table -
the parent->child tree has to be reconstructed by hand from task
notes.

## Proposal

### (a) Linkage recording

When session A pushes a task that session B ends up claiming, or
otherwise spawns session B, the child task row records:

- `parent_task_id` - the task the parent was running when it
  spawned the child (NULL for root tasks);
- `parent_session_id` - the session that authored the spawn.

Both fields are observational metadata stamped at dispatch/claim
time; they carry no lifecycle semantics of their own. The parent's
identity arrives via the push path (the pushing session's own sid +
its live task) or an explicit `--parent-task` flag on
`bridge push` for out-of-band spawns.

### (b) Status tree annotation

`bridge status` keeps the fleet table shape and adds a
parent->child annotation: a child row indents under its parent
(`task N - child of task M / sid X`), and rows whose parent is
dead/cancelled carry an `orphaned` marker so work that outlived
its scope is visible instead of silently surviving.

### (c) Propagation is explicit POLICY, never automatic

A parent finishing, failing, or being withdrawn never implicitly
touches children - every row keeps its own lifecycle and the
linkage exists for visibility and operator/orchestrator policy
decisions only.

| Action | Effect on children |
| --- | --- |
| `bridge halt` (sid or `--all`) | per-session claim/wake flag, NOT global state - `halt <sid>` targets exactly one session, `--all` reaches `managed AND active` sessions only; children are never halted implicitly (the 2026-11-11 incident). Propagation to children stays an explicit operator choice, e.g. a future `halt --tree` |
| `bridge withdraw` on a parent task | does NOT cascade - children withdraw individually by task id |
| `bridge kill <sid>` (session kill) | children unaffected - they keep their own rows and slots |
| `session/cancel` | per-turn only - aborts the target session's live turn, never a child's |
| slot-cap counting | once linkage exists a child occupies its own slot like any session - the cap still counts distinct sessions, linkage only makes the tree visible |

## Open questions for implementation

- Whether `bridge withdraw` grows an opt-in `--cascade` flag
  (default stays no).
- How deep `bridge status` renders the tree (one level vs
  recursive).
- Whether `orphaned` children of cancelled parents get a janitor
  sweep or remain operator-visible only.
