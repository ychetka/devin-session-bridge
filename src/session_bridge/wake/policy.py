"""Wake pacing policy (item_16).

In-memory per-daemon state guarding the wake pass:

- per-session cooldown - one wake per session per window, however
  often new mail relays in;
- per-host circuit breaker - N consecutive failures against the same
  acp-host lockfile suspend every wake until the window passes, so a
  dead host does not spam an attach attempt every poll tick.

Nothing is persisted: a daemon restart resets the clocks, which only
means a wake may fire once sooner than configured - the mailbox is
authoritative, so a duplicate nudge is an accepted cost, never a bug.
"""

import time

__all__ = ["WakePolicy"]


class WakePolicy:
    """Pacing gates for ``_wake_pass``; cheap and process-local."""

    def __init__(
        self,
        *,
        cooldown_seconds: float = 300.0,
        breaker_threshold: int = 3,
        breaker_cooldown_seconds: float = 600.0,
        clock=time.time,
    ) -> None:
        self._cooldown = cooldown_seconds
        self._breaker_threshold = breaker_threshold
        self._breaker_cooldown = breaker_cooldown_seconds
        self._clock = clock
        self._last_attempt: dict[str, float] = {}
        self._consecutive_failures: dict[str, int] = {}
        self._suspended_until: dict[str, float] = {}

    def check(self, session_id: str, host_key: str) -> str | None:
        """None when a wake may proceed, else a skip reason string."""
        now = self._clock()
        suspended = self._suspended_until.get(host_key)
        if suspended is not None and now < suspended:
            return "breaker_open"
        last = self._last_attempt.get(session_id)
        if last is not None and now - last < self._cooldown:
            return "cooldown"
        return None

    def record_attempt(self, session_id: str) -> None:
        """Stamp the attempt time - cooldown runs from the attach try,
        not the outcome, so failures cannot hot-loop a session."""
        self._last_attempt[session_id] = self._clock()

    def record_result(self, host_key: str, ok: bool) -> bool:
        """Track per-host failures; True when the breaker just opened."""
        if ok:
            self._consecutive_failures[host_key] = 0
            return False
        count = self._consecutive_failures.get(host_key, 0) + 1
        self._consecutive_failures[host_key] = count
        if count >= self._breaker_threshold:
            self._suspended_until[host_key] = self._clock() + self._breaker_cooldown
            self._consecutive_failures[host_key] = 0
            return True
        return False
