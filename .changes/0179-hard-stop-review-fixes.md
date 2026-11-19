---
type: fix
scope: cli
user_visible: true
breaking: false
user_summary: "hard-stop: env marker outranks 'mcp' substring, shared user daemons and interactive shells excluded, close escalates on failed cancel, kill --hard-stop pre-halts the session"
dev_summary: classify ordering fix (env > MCP token), _SHARED_DAEMON exclusion, tty_nr exclusion on the cwd leg, layer-1 close on failed:*, set_session_halted before the kill ladder
why: external architect audit (REVIEW_ROUND1) found boundary defects at the sweep's exclusion order, the layer-1 gate and the teardown race window.
refs: issue #179
---

## Spec impact
- modifies: docs/specs/sessions.md#layered-stop-reports-observed-outcomes-only
