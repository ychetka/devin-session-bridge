"""Quota-signal parser tests (task #216).

The auto-park channel is deliberately conservative: only an explicit
provider-limit marker classifies, and only a parseable reset time
completes a park - generic transport failures must never park.
"""

import time

from session_bridge.quota import (
    is_quota_signal,
    parse_quota_signal,
    parse_reset_at,
)


def test_quota_markers_classify():
    """Explicit rate-limit vocabulary is a quota signal."""
    for text in (
        "model rate limit reached, reset at 12:51 UTC",
        "API returned 429 Too Many Requests",
        "quota exceeded for this billing period",
        "RESOURCE_EXHAUSTED: insufficient_quota",
        "rate-limit on subagent pool, retry after 300s",
    ):
        assert is_quota_signal(text), text
        assert parse_quota_signal(text) is not None


def test_generic_failures_never_park():
    """GWT-216-neg: rpc_error/busy/timeout/dead/lease-expired carry no
    quota marker and must not park (task #216 spec)."""
    for text in (
        "rpc_error: host closed the channel",
        "session is busy mid-turn",
        "timeout waiting for prompt ack",
        "worker marked dead",
        "lease expired - task requeued",
        "error: connection refused",
        "no response from acp-host",
        None,
        "",
    ):
        assert parse_quota_signal(text) is None, text


def test_reset_at_clock_utc():
    """'reset at HH:MM UTC' yields the next such UTC wall-clock point."""
    now = time.time()
    reset = parse_reset_at("rate limit reached, reset at 23:59 UTC", now)
    assert reset is not None and reset > now
    # an already-past clock time rolls to tomorrow
    reset = parse_reset_at("quota resets at 00:01 UTC", now)
    assert reset is not None and reset > now


def test_reset_relative_units():
    """'reset in N', 'retry after N' and 'try again in N' parse by unit."""
    now = int(time.time())
    assert parse_reset_at("retry after 3600s", now) == now + 3600
    assert parse_reset_at("rate limit resets in 18 minutes", now) == now + 1080
    assert parse_reset_at("try again in 2 hours", now) == now + 7200


def test_reset_at_iso():
    """Full ISO stamps parse with and without timezone suffixes."""
    now = time.time()
    reset = parse_reset_at("limit resets at 2999-01-01T00:00:00Z", now)
    assert reset is not None and reset > now
    reset = parse_reset_at("reset at 2999-01-01 00:00:00", now)
    assert reset is not None and reset > now


def test_marker_without_reset_is_signal_not_park():
    """A quota marker with no parseable window returns reset_at=None -
    the signal is real but manual quota-park owns the window."""
    sig = parse_quota_signal("rate limit exceeded")
    assert sig is not None and sig["reset_at"] is None


def test_junk_and_ambiguous_text():
    """No marker or no window, no park."""
    assert parse_quota_signal("random prose") is None
    assert parse_reset_at("rate limit someday", 1000) is None
