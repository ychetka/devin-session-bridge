---
type: feat
scope: state
user_visible: true
breaking: false
user_summary: "New `waiting_quota` step state parks a rate-limited task until its quota reset instead of letting it die on lease expiry; `bridge quota-park`/`quota-resume` control it manually and wake suppression keeps parked sessions asleep until reset. `state/cli/daemon` (issue #103, task 216)"
dev_summary: "Adds tasks.step_state + quota columns, quota_park/unpark writers with lease/claim/recovery guards, a quota-signal text parser hooked into `bridge event`, wake suppression until quota_reset_at, a daemon quota pass with a single post-reset probe, and step-state surfacing in inspect/status."
why: "Tasks were dying on lease expiry and burning retry budget during model rate limits even though the session had saved its work (precedent: task 212 'lease expired' at HALT); a durable parked step keeps the lease owned by the original claimant and resumes exactly the blocked step."
refs: issue #103, task 216, ADR-0003
---
