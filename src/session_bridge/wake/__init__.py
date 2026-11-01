"""acp-host wake transport (item_16).

Re-exports the public surface so callers can
``from session_bridge.wake import wake_session, WakePolicy``.
"""

from session_bridge.wake.acp_host import (
    ATTACH_LOCK,
    DEFAULT_AGENT_ID,
    DEFAULT_LOCK_DIR,
    WakeError,
    find_lockfile,
    nudge_text,
    pid_alive,
    wake_session,
)
from session_bridge.wake.policy import WakePolicy

__all__ = [
    "ATTACH_LOCK",
    "DEFAULT_AGENT_ID",
    "DEFAULT_LOCK_DIR",
    "WakeError",
    "WakePolicy",
    "find_lockfile",
    "nudge_text",
    "pid_alive",
    "wake_session",
]
