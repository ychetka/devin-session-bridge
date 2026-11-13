# Changelog

## [0.15.7] - 2026-11-13

### User-facing

#### feat
- `bridge stop <sid> [--hard] [--dry-run]` is the real session stop: `session/cancel`, forced `session/close`, and an opt-in attributed process sweep - with per-layer observed outcomes instead of claimed kills; `bridge kill <sid> --hard-stop` runs the same ladder before the bus teardown. `cli` (issue #164, task 339)

### Codebase

#### cli
- [feat] New `wake.acp_host.close_session` (forced `session/close` with `cognition.ai/force` meta, honest idle/confirmed/requested outcomes), a new `stop` package implementing the attributed /proc orphan sweep (backend-tree descendants attributed by workspace cwd or sid markers; backend, acp-host supervisor, MCP servers and foreign sessions excluded; SIGTERM -> grace -> SIGKILL), a `bridge stop` CLI command, `kill --hard-stop`/`--dry-run` flags, `db.kill_session(stop_report=...)` folding layer outcomes into `session_killed`, and a `session_stopped` decision event. Why: The 2026-11-12 incident showed `bridge kill <sid>` only tore down bridge state while the Devin Desktop session kept running - spawning subagents, dialing tools, writing artifacts. The layered design (docs/SESSION_HARD_STOP.md) stops the session honestly: cancel first, forced close second, a conservative attributed sweep only under --hard, and the bus teardown last for kill --hard-stop - every layer reports observed state, never a claim inferred from delivery. (issue #164, task 339, docs/SESSION_HARD_STOP.md)

## [0.15.6] - 2026-11-13

### Codebase

#### docs
- [docs] Added `docs/SESSION_HARD_STOP.md`: root-cause analysis of the managed-session stop incident (kill is bus-state only, cancel wired only to `withdraw --stop-turn`, single shared `devin acp` backend owns all sessions) and the layered hard-stop design with implementation plan. (issue #164)

## [0.15.5] - 2026-11-12

### Codebase

#### tooling
- [fix] repo_merge.sh now blocks merges that touch shipping paths (src/, pyproject.toml, uv.lock) without bumping [project].version - pre-merge `version_not_bumped` failure per the developer ruling of 2026-11-12 (issue #161). Exemptions: non-shipping-only diffs and PRs labeled `no-release`; post-merge release flow unchanged. (issue #161)

## [0.15.2] - 2026-11-12

### User-facing

_No user-visible changes._

### Codebase

#### tooling
- [chore] Cleaned the .devin/.gitignore inventory drift (PR #156 residual): the blanket '.devin/' and '.windsurfrules' entries were replaced with precise local-only patterns (roadmap.md, tasks/, cycle_log.jsonl*, harness_log.jsonl, SESSION_STATE.md, hooks.v1.json, archive/, migration_ok stamp), and the remaining canon files that the blanket rule had been silently hiding are now tracked - .devin/agents/{git,hygiene}.md, .devin/config.json (generic permissions list, no machine paths), and 11 scripts (check_setup, cycle_log, session_audit, verify_constraints, ship_push, repo_issue_create, repo_issue_close, repo_pr_open, repo_status_check, repo_read, repo_branch_delete). Why: every .windsurfrules reference now resolves to a tracked file and every repository-agent contract verb maps to a tracked repo_*.sh wrapper, so the agent contract is no longer fail-closed on a fresh clone; local-only session state stays ignored by exact path. (issue #157)

## [0.15.1] - 2026-11-12

### User-facing

_No user-visible changes._

### Codebase

#### tooling
- [feat] Ported the devin-telegram version-tag release automation (PR #183) - repo_release.sh now derives the tag from [project].version on the base head and fails on missing/unparseable pyproject, version-behind-latest-tag, or tag-exists-on-different-sha; repo_merge.sh gained a post-merge release block (pre-merge sha capture, structured skip reasons, release result folded into the merged JSON); new tools/check_version_tag.sh + version-tag.yml CI assert every merged version lands a remote tag. Why: The 2026-11-12 devin-telegram version-drift incident showed latest_tag+1 arithmetic and a release step decoupled from merge can land untagged versions - the same gap left sessions-bridge v0.14.0 untagged overnight. Canon adaptations: REMOTE detection (canon remote is 'github', not 'origin') in the new merge block, and the idempotent 'gh release view' early-exit kept from the pre-port repo_release. (issue #155)

## [0.15.0] - 2026-11-12

### User-facing

#### feat
- `bridge retry` now trips a thrash breaker after `SB_THRASH_LIMIT` (default 3) consecutive task failures - it refuses with an open human escalation until the escalation is answered or `--force` is passed. `state` (issue #153, ADR-0005)

### Codebase

#### state
- [feat] New `tasks.fail_streak` column counts consecutive failure outcomes (finish --fail and terminal wedge folds; done resets, plain retry preserves); the retry CAS guards on it, and a trip writes a thrash_breaker decision event plus a deduplicated ESCALATION audit row plus an orchestrator notice - all atomically. Why: A fix-fail-fix loop was invisible to the bus because retry resets attempt, so repeated gate/test failures cycled forever; fail_streak counts outcomes across retries so the breaker can park a thrashing task on a human escalation. (issue #153, ADR-0005)

## [0.14.0] - 2026-11-12

### User-facing

#### feat
- `bridge status` now surfaces `bridge wait` external waits - the parked phase reads `ewait`/`waiting` with the declared reason, the 24h digest counts lease expiries, and a `waiting-external` gauge tracks parked claimers. `cli` (issue #151, task 293)

### Codebase

#### cli
- [feat] status.py renders fresh wait_until stamps as a distinct parked phase (session view PARKED + 'external wait: <reason>', fleet waiting/ewait cells, wait_until/wait_reason in --json) and the digest gains lease_expired + waiting_external; OPERATOR.md documents the notice/directive un-park flow. Why: A wait-stamped running task was indistinguishable from real work in `bridge status` (running + '-'), so parked claimers were invisible and the un-park convention was undocumented; this is observability only - no lifecycle change and no external polling. (issue #151, task 293)
- [fix] Dedicated wait-observability columns on tasks: `wait_reason` carries the declared `bridge wait` reason (written unconditionally, so a reason-less wait clears it and a stale lifecycle note can never render as the cause) and `lease_expired_at` stamps every requeue_or_fail fold, making the digest's lease_expired count exact instead of a 'lease expired' note proxy. Why: review of the observability MVP found the note proxy both overcounted (sticky note + updated_at bumps) and undercounted (note overwritten), and a reason-less wait after a fold rendered 'external wait: lease expired'. (issue #151)

## [0.13.3] - 2026-11-11

### Codebase

#### tooling
- [docs] Lifecycle reconciliation - docs/OPERATOR.md marks `bridge kill <task_id>` the destructive last-resort against withdraw's benign scope-pull, and docs/PARENT_CHILD_SESSIONS.md lands the parent-child linkage design note (linkage recording, status tree annotation, explicit propagation policy). Why: Task 282 closes the residual kill-vs-withdraw ambiguity after v0.13.0 - withdraw covers every benign pull path and kill keeps only the wedged-live-work niche; the parent-child spec is written down as a design item following the 2026-11-11 misroute incident, and the stale SOAK-3 soak note is closed as covered by continuous fleet operation. (issue #149, task 282)

## [0.13.2] - 2026-11-11

### Codebase

#### tooling
- [chore] Canon adoption: 12 files copied byte-for-byte from ai_controller canonical sources and md5-verified - agent profiles (code_writer, judge, reviewer), gate scripts (gate_tests, hygiene_check, migration_gate, ship_commit, plus new migration_pattern.sh), hazard docs (CODING_HAZARDS, REVIEW_HAZARDS), and director-model docs (harness.md, plus new controller-manager/development-cycle.md). Why: local copies had drifted from the canonical templates; byte-for-byte adoption restores a single point of truth for fleet tooling. (issue #146)

## [0.13.1] - 2026-11-11

### Codebase

#### tooling
- [fix] Post-incident audit fixes for the fleet quality gates rollout: hygiene_check anchors the `sk-` secret pattern (word boundary + min length, no more `task-`/`risk-` false positives), splits commit text on the last `@@SUBJECTS_END@@` sentinel so injected fake-diff tails cannot smuggle exempt-path cyrillic, reads project.json inside the python sandbox (fail-closed on malformed JSON) and skips lockfile added-lines in both diff modes; migration_gate covers staged deletions only via a roll-forward note naming each path, tightens the rollback regex to `down` inside `--` comments, reads `dry_run_cmd` from project.json instead of an env var, and binds deletions into the stamp hash which ship_commit now makes single-use; reviewer profile gains shared-checkout worktree isolation, an untrusted-PR-content rule, head-sha-bound verdict dismissal and PATH-first gh-askon resolution. Why: the issue #144 audit found fail-open paths convention-level rules could not close. (issue #144)

## [0.13.0] - 2026-11-11

### User-facing

#### feat
- New `bridge withdraw` command marks tasks `cancelled` with an audited actor/reason event - the orchestrator's benign scope-pull that `bridge kill` (a failure verdict) could not express. `cli,state,daemon,wake` (issue #142, task 267, issue #137)

### Codebase

#### cli,state,daemon,wake
- [feat] Audited orchestrator withdraw (`cancelled` status + `bridge withdraw` with optional `--stop-turn` session/cancel) and durable `bridge_settings` persistence of the session-slot cap. Why: Task 264 stayed `running` after a HALT/withdrawal acknowledgment because nothing in the model could terminally close a scope-pull: HALT parks claims without touching the task row, and `kill` is a force-fail verdict for wedged work only. `withdraw` writes the audited terminal cancellation, frees the live-task index slot atomically and refuses late worker finish/renew via their own CAS. `--stop-turn` uses the empirically proven session/cancel (issue #137) and reports idle/confirmed/requested honestly - never a process-death claim. The cap is now daemon-published so worker CLIs, which never inherit the daemon env, enforce the same limit. (issue #142, task 267, issue #137)

## [0.12.1] - 2026-11-11

### Codebase

#### tooling
- [chore] Canonical fleet quality gates rollout - hazards docs, writer/reviewer/judge profiles, and commit-wrapper gates with negative tests. Why: Shared language-neutral reliability rules and mechanical enforcement (precommit re-grounding, whitelist scope, migration gate) replace drifting per-repo prompt lists; canonical source lives in ai_controller templates. (issue #140)

## [0.12.0] - 2026-11-11

### User-facing

#### feat
- Worker sessions now follow the director session model: short event-driven turns, one background executor, mandatory checkpoints. `docs` (issue #137, task 262)

### Codebase

#### docs
- [feat] Adopted canonical director session model section into controller-manager/harness.md; mid-turn inject proven impossible (issue #137). Why: Canonical rollout of the director session model (task 262). A live ACP probe proved session/prompt is rejected mid-turn ("a turn is already running"), so the event-driven model is the only viable variant. (issue #137, task 262)

## [0.11.0] - 2026-11-11

### User-facing

#### feat
- Every PR that changes files must now carry a release-notes record: a new `.changes` fragment, a folded `CHANGELOG.md` change, or the `no-changes-entry` opt-out marker - enforced by CI and by the merge gate. `tooling` (issue #135, task 261)

### Codebase

#### tooling
- [feat] New `tools/check_changes_entry.sh` runs in the lint workflow on pull_request (diff vs base ref, PR metadata via env) and inside `.devin/scripts/repo_merge.sh` as a pre-merge guard over the gh API file list; merges without a record are blocked with failure_class `missing_changes_entry`. `CHANGELOG.md` changes satisfy the gate so release-prep folds still merge, and `no-changes-entry` as a whole-line declaration in title/body or an exact label is the documented docs/ops opt-out. Why: PRs #128 and #132 merged with no `.changes` fragment and no version bump, each blocking `repo_release.sh` on version_drift until a retroactive fragment+bump PR fixed the record. Approved design gates the fragment itself - not the version bump, which stays a release-step concern so parallel PRs cannot collide on the version number. (issue #135, task 261)

## [0.10.3] - 2026-11-10

### User-facing

_No user-visible changes._

### Codebase

#### tooling
- [chore] Updates the canonical `code_writer` agent profile to v2 with evidence-based gates (PR #132) and bumps the package to 0.10.3 so the merged head can tag. Why: PR #132 merged on version 0.10.2 which repo_release.sh correctly rejects as version_drift; the v2 profile tightens the writer-agent contract to evidence-based gates, so the release carries it as a chore entry. (issue #133, PR #132)

## [0.10.2] - 2026-11-10

### User-facing

_No user-visible changes._

### Codebase

#### tooling
- [chore] Adds the canonical `code_writer` agent profile under `.devin/agents/` (PR #128) and bumps the package to 0.10.2 so the merged head can tag. Why: PR #128 merged on version 0.10.1 which repo_release.sh correctly rejects as version_drift; the profile documents the repo's writer-agent contract, so the release carries it as a chore entry. (issue #129, PR #128)

## [0.10.1] - 2026-11-10

### User-facing

#### fix
- Quota probes can no longer be starved by mail-wake cooldowns, a parked task whose owner is still working is held+escalated instead of released to lease expiry, and every real park is now durable-journaled with its original marker. `daemon` (issue #123)

### Codebase

#### daemon
- [fix] db.quota_park commits a durable `quota_park` decision_events row (reset, source, marker note) atomically with the CAS for real parks; the daemon quota pass paces control probes on a dedicated WakePolicy instance; give-up past the defer limit on a verifiably-busy owner re-parks a bounded hold (`SESSIONS_BRIDGE_QUOTA_BUSY_HOLD_SECONDS`, default 3600s, source `busy-hold`) and escalates once per episode to the orchestrator as a `[QUOTA_PROBE]` notice (notify-send fallback), bounded at `_QUOTA_BUSY_HOLD_LIMIT` (3) holds so a wedged turn cannot park forever; defer re-parks preserve the original `quota_note`; `quota_signal_seen` also lands in decision_events. Why: Task 249 postmortem of the task 224 misclassification: the park journal lived only in the daemon jsonl log so decision_events could never explain the episode; the shared 300s wake cooldown starved all 10 control probes (`paced` without a single attempt); and gave_up folded a probe-delivery problem into a worker failure on a verifiably mid-turn session (lease expiry -> attempts exhausted -> failed). (issue #123)

## [0.10.0] - 2026-11-10

### User-facing

#### feat
- optional SB_MAX_ACTIVE_SESSIONS caps concurrently active sessions; over-cap tasks queue with a slot-wait reason in bridge status `cli` (issue #121)

### Codebase

#### cli
- [feat] db.active_session_ids counts distinct sids holding running|dispatch_decided tasks; SB_MAX_ACTIVE_SESSIONS (default 0 = unlimited, negatives clamp) is enforced inside the claim_queued / claim_managed / dispatch_managed CAS predicates via _SLOT_SQL - a queued claim needs a free slot while a staged dispatch_decided self-claim passes the EXISTS leg - so every claim edge (pump, Stop hook, user kick, managed claim) honors the cap atomically; the pump additionally pre-filters for slot_wait observability surfaced via bridge status session_slots + slot_wait_reason. Why: fleet-wide quota preservation - the operator needs a coarse lever bounding how many sessions burn in parallel; CAS-enforcement rather than a pump snapshot because direct claim edges would bypass a dispatch-only gate (review r1) (issue #121)

## [0.9.1] - 2026-11-10

### User-facing

#### fix
- stale halt/resume/disarm deliveries are dropped instead of executed - a control message superseded by a newer one or older than SB_CONTROL_TTL_S (default 6h) no longer self-halts a resumed session `daemon` (issue #119)
- halt/resume/disarm with the daemon stopped relay + prune control mail synchronously at command time - a drain during the outage meets the same mailbox state `cli` (issue #119)

### Codebase

#### daemon
- [fix] control deliveries carry an issued_at epoch header; _control_stale_reason judges supersession (latest control notification id) + TTL at both delivery legs: the relay consumes stale outbox rows via mark_relayed-without-file and a _stale_control_pass janitor unlinks stale control files sitting in mailboxes; every drop journals stale_control_dropped (decisions + decision_events); _msg_headers extracted from _msg_is_tick. Why: incident 2026-11-10: a HALT consumed ~24h late self-halted mahogany-dryosaurus mid-task after the fleet was already resumed - control files had no epoch so an obsolete command executed at drain. (issue #119)
- [fix] janitor resolves genuine relay artifacts through the notification row; unverifiable files are judged by age only (no supersession on a forged basename id) with issued_at clamped to min(header, mtime) so a self-claimed future timestamp cannot extend the TTL (review r1, issue #119)

#### cli
- [fix] _sync_control_delivery runs relay + janitor synchronously from halt/resume/disarm when the daemon is down (daemon_running lockfile check) - the staleness guard no longer depends on the loop being alive; relay failures degrade to a relay_error field instead of aborting the response (review r1, issue #119)

#### state
- [refactor] latest_control_notification_id accepts an optional caller-held connection so the janitor's per-file epoch lookups reuse one handle (review r1, issue #119)

## [0.9.0] - 2026-11-10

### User-facing

#### feat
- dead managed sessions (incl. the orchestrator) now revive themselves when pending mail + session/list presence coincide - no more manual re-register after a tab restart `daemon` (issue #115)

### Codebase

#### daemon
- [feat] daemon resurrect leg: dead+managed sids with pending mail get a batched presence probe; liveness-marked rows CAS dead>active via db.revive_dead_managed (session_killed markers refuse); db.mark_dead makes the dead CAS + task fold + liveness marker one BEGIN IMMEDIATE so a racing kill cannot wedge session_killed between split writes, kill_session pins session_killed even on a zero-effect kill, and the managed orchestrator dead edge gains notify-send parity. Why: the deferred orchestrator_dead auto-recovery gap: a dead-flagged managed session stayed dead until someone manually re-registered - the exact human-in-the-loop the bus exists to remove. Demand-driven probing (mail pending) keeps the probe cheap and operator-kills senior. (issue #115)

## [0.8.3] - 2026-11-10

### User-facing

#### feat
- `bridge kill <task_id>` force-fails one wedged live task (running|dispatch_decided > failed ignoring the lease) and notifies the owning session - no waiting out the lease, no session teardown. `cli` (issue #113, item_35 deferred note)

### Codebase

#### cli,state
- [feat] numeric positional on `bridge kill` routes to db.kill_task: CAS to failed + lease/step clear + dirty-tree note + durable task_killed event in one transaction, NOTICE outbox row to the active owning session; typed TaskKillVerdict (not_found/refused/killed) keeps queued/awaiting_user/terminal rows refused. `state,cli` Why: Deferred item_35 follow-up (the last human-override piece): `bridge finish` is owner-CAS'd so a wedged running task could only die by lease expiry or a full session kill. The operator verdict is senior to the lease and to a waiting_quota park, and the NOTICE keeps the worker from burning a turn on an already-dead task. (issue #113, item_35 deferred note)

## [0.8.2] - 2026-11-10

### User-facing

#### fix
- Rate-limit errors surfacing through wake/prompt transport now park the affected task as waiting_quota instead of burning the retry streak or failing it outright. `daemon,cli` (issue #111, task 230)

### Codebase

#### daemon,cli
- [fix] daemon._wake_pass and cli._acp_failure sniff rpc_error text through parse_quota_signal and CAS the session's live task into waiting_quota (sources wake/acp-<phase>); a marker without a reset parks for the bounded DEFAULT_PARK_SECONDS=900 window, and a quota signal with no live task journals quota_signal_seen. quota.py gains the 'reset in N <unit> (at HH:MM UTC)' combined form where the earlier of the relative window and the absolute clock wins. Why: Live incident 2026-11-09: a fleet-wide free-tier rate limit reached the bridge as 'rpc_error - session/prompt: Reached free model rate limit ... reset in 58 minutes (at 22:01 UTC)' - the marker and reset were in the body but classification stopped at rpc_error, so parked nothing and burned wake retries plus prompt attempts against a dead pool. The transport layer is the earliest surface that sees the provider's text, so it now feeds the same conservative parser the self-report channel uses; the 900s default window for marker-without-reset mirrors free-tier windows and self-heals via the quota pass re-probe. (issue #111, task 230)

## [0.8.1] - 2026-11-10

### User-facing

_No user-visible changes._

### Codebase

#### tooling
- [feat] Living spec pilot: `.changes` entries now carry a required `## Spec impact` section (adds/modifies/removes anchors into docs/specs), docs/specs/ seeds the requirement registry for mailbox/tasks/wake/sessions/quota in WHEN/THEN form, and tools/check_spec_anchors.sh (run by verify_constraints.sh) fails PRs whose anchors do not resolve post-change. Why: Nothing answered 'how does the system behave now' - roadmap tracks work, docs drift; per the approved spec_lifecycle design each PR carries its own spec delta so behavior and documentation cannot diverge. (task 226, issue #107, ADR-0004)

## [0.8.0] - 2026-11-10

### User-facing

#### feat
- The claim-lease TTL is now configurable via `SB_LEASE_TTL_S` and defaults to 1h (was a hardcoded 30m) - long worker turns no longer outlive their lease window as easily, and `bridge` picks the override up at startup. `state` (issue #105, directive 526 (task 213 postmortem))

### Codebase

#### state
- [feat] LEASE_SECONDS resolves once at the state.db layer from `SB_LEASE_TTL_S` (contract default 3600, malformed values degrade to the default, floored at 600s (2x renew margin) so a short override cannot sit inside the renewal window permanently); every lease writer plus the daemon auto-renew ceiling, dispatch renew interval and worker-log retention follow the resolved value. Why: Queue task 213 died on 'lease expired' after ~2.7h of live work when the auto-renew explicit-touch ceiling (6 x 1800s) ran out mid-turn - the lease length itself needed to be an operator tunable with a safer default. (issue #105, directive 526 (task 213 postmortem))

## [0.7.0] - 2026-11-09

### User-facing

#### feat
- New `waiting_quota` step state parks a rate-limited task until its quota reset instead of letting it die on lease expiry; `bridge quota-park`/`quota-resume` control it manually and wake suppression keeps parked sessions asleep until reset. `state/cli/daemon` (issue #103, task 216, ADR-0003)

### Codebase

#### state
- [feat] Adds tasks.step_state + quota columns, quota_park/unpark writers with lease/claim/recovery guards, a quota-signal text parser hooked into `bridge event`, wake suppression until quota_reset_at, a daemon quota pass with a single post-reset probe, and step-state surfacing in inspect/status. Why: Tasks were dying on lease expiry and burning retry budget during model rate limits even though the session had saved its work (precedent: task 212 'lease expired' at HALT); a durable parked step keeps the lease owned by the original claimant and resumes exactly the blocked step. (issue #103, task 216, ADR-0003)

## [0.6.0] - 2026-11-09

### User-facing

#### feat
- New read-only `bridge fleet-metrics [--window 24h|7d]` renders a per-session/repo/orchestrator markdown report from bridge state and repo cycle_logs. `cli` (issue #98, directive 446, ADR-0002)

### Codebase

#### cli
- [feat] Adds commands/metrics.py + a `fleet-metrics` subcommand aggregating tasks/questions/decision_events per session and each repo's .devin/cycle_log.jsonl (paths from session cwd registry) into a markdown report; orchestrator self-metrics and an incident table included. Why: The orchestrator needed a fleet-level fact report for gate-architecture review without sessions ever seeing it; cycle-time and incident data existed but had no single read path. (issue #98, directive 446, ADR-0002)

## [0.5.2] - 2026-11-09

### User-facing

#### fix
- The host-side ingress timer no longer attempts (and fails) to reconcile ns-* forwarders; it owns tcp: rows only, while each project's userns-context loop owns its namespace specs. `ingress` (issue #99, PR #100)

### Codebase

#### ingress
- [fix] Added SESSIONS_BRIDGE_INGRESS_KINDS env filter to `bridge ingress sync` - a pass skips foreign-kind desired specs and leaves their tracked forwarders untouched (unparseable stored specs are treated as foreign); env values are case-normalized and validated against the known spec kinds. Why: The host timer lacks CAP_SYS_ADMIN over a project-owned user namespace, so setns() into ns-* targets always failed; restricting each reconcile context to the kinds it can actually act on removes the noise and prevents a filtered pass from killing foreign forwarders. (issue #99, PR #100)

#### tooling
- [feat] Establishes the .changes/ per-PR entry convention, tools/release_notes.sh aggregation (User-facing + Codebase sections), the PR-template ADR checkbox, docs/adr/ rules and the fleet-adoption spec. Why: Hand-assembled changelogs drifted from what each PR actually shipped; a machine-readable entry per PR makes release notes generated, reviewable and identical across orchestrated repos. (issue #96, directive 444, ADR-0001)

## [0.4.0] - 2026-11-08

Neutral host ingress layer (issue #92, task #209): port forwards
are now materialized from the allocation registry itself.

### ingress/

- `bridge reserve tcp_port <port> --fwd <spec> [--bind <addr>]`
  embeds a machine-readable forward declaration into the row's
  `purpose` (`fwd=`/`bind=` tokens; grammar: `tcp:<host>:<port>`,
  `ns-pid:<pid>:<port>`, `ns-pidfile:<path>:<port>`).
- `bridge ingress sync` reconciles detached
  `session_bridge.ingress.forwarder` processes with the registry:
  reserve -> listener appears, release -> forwarder removed,
  changed spec or pidfile target -> respawn. Reconciler state in
  `<state_dir>/ingress.json`, child logs in
  `logs/ingress-fwd-<port>.log`, flock-serialized.
- `bridge ingress status`: desired-vs-running view.
- `deploy/host-ingress.{service,timer}` + `install_ingress_timer.sh`:
  30s reconcile cadence, Persistent=true.
- docs/ALLOCATIONS.md documents the contract; trade-ai's
  supervisor.sh moves its 5678/8080/8443 chain onto it.

## [0.2.1] - 2026-11-02

Zero-touch delivery completed: the bridge can now wake idle
managed sessions itself, and the daemon's liveness model is
coherent with that wake path. Closes #31 and #33.

### acp-host wake transport (wake/)

- The daemon gains a `_wake_pass` after relay: a managed session
  with a stale heartbeat and relayed-but-unacked mail (or a live
  task) gets a short "drain your mailbox" prompt injected through
  the local detached acp-host - a real turn, so drain-mode
  sessions process queued mail with no human typing. The nudge
  carries no payload; the fs mailbox stays the only data channel.
- Transport: `acp-host-*.lock` discovery ({pid, port, token},
  live-pid preferred, re-read per attempt), NDJSON token hello,
  then plain ACP `initialize > session/list > session/load >
  session/prompt > detach` over `127.0.0.1:<port>`.
- Safety: `cognition.ai/statusEnum` busy check (never prompt a
  mid-turn session -> `wake_skipped_busy`), process-local attach
  mutex, bounded replay drain, 120s pass deadline, per-session
  300s cooldown (60s retry-soon after `sent_unconfirmed` with
  exponential growth, capped), per-host circuit breaker
  (3 failures -> 600s), structured `wake_failed` reasons.
- `bridge wake <sid>` - manual wake through the same path
  (bypasses cooldown, keeps the busy check).
- Security boundary documented: lockfile `0600` + dir `0700` =
  same-UID bearer access.

### Managed-session liveness coherence (item_17)

- The heartbeat sweep no longer marks `mode='managed'` sessions
  dead: between turns they are stale-hb by design (that signal is
  the wake trigger), and the sweep was failing their tasks 120s
  after dispatch while wake prompts were in flight.
- Managed death is ACP-reported instead: 3 consecutive
  `no_session` wake outcomes (absent from the host's
  `session/list`) flip the session to dead and requeue its live
  task through the same path as the unmanaged sweep
  (`worker_dead`, `reason=no_session_streak`). `no_cwd`
  (present in list, unwakeable) counts as seen; all other
  failures are streak-neutral.
- `_wake_candidates` requires `status='active'` and also matches
  sessions with a live task - the wake probe doubles as the
  liveness check when the outbox is empty.
- `bridge arm` clears a leftover `queue_paused` flag in the same
  upsert, so arming restores task claimability.
- `db.ask()` now also writes the active orchestrator a
  `directive` notification ("[question #N pending ...]") in the
  same transaction - worker replies reach the orchestrator
  mailbox without manual `questions` polling.

### Docs

- `docs/OPERATOR.md`: wake transport requirements and failure
  modes, sweep exclusion for managed mode, no_session dead path
  and MTTD note, `bridge wake`, arm-clears-pause.
- `docs/orchestrator/PLAYBOOK.md`: drain + wake close the loop
  for interactive sessions; pending questions arrive as
  `directive` mail (no manual polling needed).

## [0.2.0] - 2026-11-01

Orchestration protocol release: the bridge becomes a two-way
message bus for orchestrator-managed session fleets, with
zero-touch delivery to locked Desktop sessions, a supervisor
daemon, a Delegation-of-Authority escalation model, and recurring
scheduled dispatch. Closes #14 and #15.

### Message bus (state.db, schema v3)

- `sessions` gains role/mode/status; `worker_heartbeats` with
  generation-counter CAS; orchestrator is a CAS singleton with a
  bounded refusal window and `--force` takeover for known-dead
  holders.
- `questions`: worker->orchestrator channel, `pending > claimed >
  answered`, unique pending per task, 3-question budget, janitor
  releases stale claims (min-one-per-task per pass).
- `notifications` outbox: writers commit `relayed_at NULL`, the
  daemon relays to filesystem mailboxes and marks `relayed_at`;
  `ack_at` watermark survives orchestrator restarts.
- `audit_events`: append-only deliberation + escalation trail
  (verbatim partner outputs, synthesis, directive, doa_trigger).
- `schedules`: recurring task definitions (`every_seconds`,
  `next_run_at`, `catchup`, `enabled`), `tasks.run_after` column.
- `user_version` 1 -> 3, all migrations idempotent under
  `BEGIN IMMEDIATE`.

### Managed Mode delivery (mailbox/)

- `bridge arm`/`disarm`/`disarm --all`: enrolled sessions spawn a
  background mailbox-watcher subagent polling
  `queue/<sid>/msg_*.txt`; deliveries land inside the parent's open
  turn, which re-arms before `end_turn` (`TURN_TTL_CYCLES` bounds
  context growth). Level-triggered, numeric `ls -v` ordering.
- Proven live: zero-touch multi-cycle delivery on a locked
  Desktop session incl. ~27-minute idle persistence.

### bridge-daemon (daemon/)

- `fcntl.flock` singleton; relays unrelayed outbox rows to
  mailboxes embedding `notification_id`/`kind`/`task_id`/
  `question_id` in the payload.
- Staleness sweep: dead worker -> `status=dead` + live tasks
  requeued with stash/reset-base-ref instruction; silent
  orchestrator -> one deduplicated `notify-send`.
- Own heartbeat file + `daemon_decisions.jsonl` for external
  cron/systemd watchdogs; `janitor_questions` runs each pass;
  schedule materialization pass (see below).

### CLI (bridge.py)

- `register <sid> [--role ...] [--force]`, `heartbeat <sid>`,
  `ask`, `ack`, `questions list|claim|answer` (claimant-bound),
  `escalate show` + `escalate reply A|B|defer|abort|override:<text>`
  (defer -> paused, abort -> failed + cleanup note),
  `schedule <sid> --every|--at [--catchup]`, `schedule list|clear`.
- `bridge status`: <25-line fleet view - escalation/orchestrator-dead
  banner with exact reply command, daemon+orchestrator vitals,
  fleet table (sid|repo|task|state|Q n/3|FAIL n/2|hb age), 24h
  digest.

### Docs

- `docs/OPERATOR.md`: 60-second mental model, 4 operator
  playbooks (plug worker / orchestrator start-restart + CAS
  troubleshooting / escalation handling / emergency stop), daemon
  lifecycle, watchdog snippets, status field reference.
- `docs/orchestrator/PLAYBOOK.md`: orchestrator-session contract
  template (DoA matrix, anti-escalation rule, committee protocol,
  escalation report format, mailbox duty, guardrails, bootstrap
  prompt) - copy into target workspaces as
  `.devin/orchestrator/PLAYBOOK.md`.

### Notable fixes

- Numeric mailbox ordering (`ls -v`) - `msg_10` no longer
  overtakes `msg_2`.
- Janitor multi-claim wedge (IntegrityError deadlock) fixed.
- `answer_question` claimant binding (anti-zombie answers).
- Closed-form catchup realign (O(1); a `while` walk would hang a
  `BEGIN IMMEDIATE` lock after long downtime).
- Duty-text `bridge ack --session` corrected.
- `init()` migration race closed (`BEGIN IMMEDIATE`).

## [0.1.3] - 2026-10-30

- `install_hooks.sh` writes the `hooks.v1.json` envelope format the
  client expects (issue #7).

## [0.1.2] - 2026-10-30

- CODEOWNERS + branch-protection plumbing (PR #5).

## [0.1.1] - 2026-10-30

- Hardening follow-ups from v0.1.0 review (issue #3): atomic task
  claims, lease/stale recovery, bounded worker logs, queue depth.

## [0.1.0] - 2026-10-30

- MVP: state.db task queue, ACP push, Stop-hook queue for locked
  sessions, `bridge push/pump/status/retry`, install_hooks.sh.
