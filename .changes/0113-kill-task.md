---
type: feat
scope: cli,state
user_visible: true
breaking: false
user_summary: "`bridge kill <task_id>` force-fails one wedged live task (running|dispatch_decided > failed ignoring the lease) and notifies the owning session - no waiting out the lease, no session teardown. `cli` (issue #113)"
dev_summary: "numeric positional on `bridge kill` routes to db.kill_task: CAS to failed + lease/step clear + dirty-tree note + durable task_killed event in one transaction, NOTICE outbox row to the active owning session; typed TaskKillVerdict (not_found/refused/killed) keeps queued/awaiting_user/terminal rows refused. `state,cli`"
why: "Deferred item_35 follow-up (the last human-override piece): `bridge finish` is owner-CAS'd so a wedged running task could only die by lease expiry or a full session kill. The operator verdict is senior to the lease and to a waiting_quota park, and the NOTICE keeps the worker from burning a turn on an already-dead task."
refs: issue #113, item_35 deferred note
---

## Spec impact

- adds: docs/specs/tasks.md#operator-kill-force-fails-one-live-task
