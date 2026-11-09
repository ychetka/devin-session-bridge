"""Quota-signal parsing for the waiting_quota park path (task #216).

The ACP/transport surface exposes no structured rate-limit error code
today - what reaches the bridge is free text ("rate limit ... reset
at 12:51 UTC"), so the auto channel is a conservative text parser:
an explicit quota marker AND a parseable reset time must both be
present for a self-report to park the task. ``rpc_error``, ``busy``,
``timeout`` and ``lease expired`` carry no quota marker and can never
park. A marker without a reset time returns ``reset_at=None`` so the
caller can journal "signal seen, no window" and leave parking to the
manual ``bridge quota-park --reset-at`` path.
"""

import re
import time
from datetime import datetime, timedelta, timezone

# Explicit provider-limit vocabulary only - generic failure words
# (error, timeout, busy, retry) must never trigger a park.
_QUOTA_MARKER = re.compile(
    r"rate.?limit|quota|resource.?exhausted|\b429\b"
    r"|too many requests|insufficient_(?:quota|credits)",
    re.IGNORECASE,
)

# Reset-time phrasings seen across providers/subagent errors:
#   "reset at 12:51 UTC" / "resets at 14:00 UTC"
#   "reset in 18 minutes" / "retry after 3600s" / "try again in 2 hours"
#   ISO stamps: "reset at 2026-11-09T12:51:00Z"
_RESET_AT_CLOCK = re.compile(r"resets?\s+at\s+(\d{1,2}):(\d{2})\s*(?:utc|z)\b", re.IGNORECASE)
_RESET_IN = re.compile(
    r"(?:reset(?:s)?\s+in|retry\s+after|try\s+again\s+in|available\s+in)"
    r"\s+(\d+)\s*(seconds?|secs?|s|minutes?|mins?|m|hours?|hrs?|h)\b",
    re.IGNORECASE,
)
_RESET_AT_ISO = re.compile(
    r"resets?\s+at\s+(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2})?"
    r"(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)",
    re.IGNORECASE,
)

_UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600}

#: ceiling for relative reset windows ("retry after N hours"): worker
#: text is untrusted input, so an absurd duration degrades to
#: "signal without a window" instead of overflowing the epoch bind.
_MAX_RELATIVE_SECONDS = 30 * 86400


def _clock_reset(hh: int, mm: int, now: float) -> float | None:
    """Epoch of the next ``HH:MM`` UTC wall-clock point after ``now``,
    or None when the captured time is not a real clock time."""
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        return None
    base = datetime.now(timezone.utc)
    today = base.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if today.timestamp() <= now:
        today += timedelta(days=1)
    return today.timestamp()


def parse_reset_at(text: str, now: float | None = None) -> int | None:
    """Extract a quota-reset epoch from free text, or None."""
    if now is None:
        now = time.time()
    m = _RESET_AT_ISO.search(text)
    if m:
        try:
            dt = datetime.fromisoformat(m.group(1).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except ValueError:
            pass
    m = _RESET_AT_CLOCK.search(text)
    if m:
        reset = _clock_reset(int(m.group(1)), int(m.group(2)), now)
        if reset is not None:
            return int(reset)
    m = _RESET_IN.search(text)
    if m:
        seconds = int(m.group(1)) * _UNIT_SECONDS[m.group(2)[0].lower()]
        if seconds <= _MAX_RELATIVE_SECONDS:
            return int(now + seconds)
    return None


def is_quota_signal(text: str | None) -> bool:
    """True when the text explicitly names a provider rate limit."""
    return bool(text) and bool(_QUOTA_MARKER.search(text))


def parse_quota_signal(text: str | None, now: float | None = None) -> dict | None:
    """Classify a self-reported error string.

    Returns ``{"reset_at": <epoch|None>}`` when the text carries an
    explicit quota marker, ``None`` otherwise (non-quota failures like
    rpc_error/busy/timeout/lease-expired return None and never park).
    ``reset_at`` may be None inside a hit - the signal was real but
    the window is unknown; parking then needs the manual path.
    """
    if not is_quota_signal(text):
        return None
    return {"reset_at": parse_reset_at(text, now)}
