## Task 345 / issues #167-#170: hard-stop follow-ups

- issue #167 (bug, in progress): layer-1 forced session/close runs
  unconditionally - observed cancel_outcome=idle +
  close_outcome=failed:rpc_error on an idle session. Layer 1 must run
  ONLY when the session still reports busy after layer-0 grace
  (cancel_outcome=requested); idle/confirmed skip with an honest
  outcome. Being fixed in branch issue_167_v0.15.8.
- issue #168 (enhancement, backlog): DEVIN_SESSION_ID env marker
  propagated to spawned processes - strong sweep attribution
  replacing cwd/marker heuristics (design doc section 6).
- issue #169 (upstream, backlog): 'devin stop' feature request
  against the Devin CLI - no turn-stop verb exists.
- issue #170 (investigation, doc-only, backlog): stale
  "1 subagent running" badge in Devin Desktop while ACP reports
  idle - repro + evidence for upstream.
