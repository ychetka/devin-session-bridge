"""Hard-stop support (issue #164).

Re-exports the attributed-sweep surface so callers can
``from session_bridge.stop import hard_stop`` or address the module
directly.
"""

from session_bridge.stop import hard_stop

__all__ = ["hard_stop"]
