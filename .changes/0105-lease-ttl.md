---
type: feat
scope: state
user_visible: true
breaking: false
user_summary: "The claim-lease TTL is now configurable via `SB_LEASE_TTL_S` and defaults to 1h (was a hardcoded 30m) - long worker turns no longer outlive their lease window as easily, and `bridge` picks the override up at startup."
dev_summary: "LEASE_SECONDS resolves once at the state.db layer from `SB_LEASE_TTL_S` (contract default 3600, malformed values degrade to the default, floored at 300s so a short override cannot sit inside the renewal window permanently); every lease writer plus the daemon auto-renew ceiling, dispatch renew interval and worker-log retention follow the resolved value."
why: "Queue task 213 died on 'lease expired' after ~2.7h of live work when the auto-renew explicit-touch ceiling (6 x 1800s) ran out mid-turn - the lease length itself needed to be an operator tunable with a safer default."
refs: issue #105, directive 526 (task 213 postmortem)
---
