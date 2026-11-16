"""``DEVIN_SESSION_ID`` environment marker (issue #168).

The layer-2 hard-stop sweep (``session_bridge.stop.hard_stop``)
attributes processes to a session from ``/proc/<pid>/environ``,
``cwd`` and cmdline markers. ``/proc`` environ only reflects the
exec-time environment, so the marker can only be injected where the
bridge controls a process's environment AT SPAWN:

- ``cli.dispatch._spawn_worker`` sets it on every acp-push
  ``bridge _work`` child; the worker's own ``devin acp`` subprocess
  and every process the session spawns through that connection
  inherit it;
- ``mailbox.core`` renders it as an env prefix on the watcher and
  drain-observer commands the managed duty hands to the session;
- ``hooks.worker`` re-execs itself once with it after the payload
  reveals the sid, so the worker's own environ carries it too
  (children inherit the ``os.environ`` write regardless).

Surfaces the bridge does NOT control stay unmarked by design: the
shared ``devin acp`` backend and Desktop session processes are
upstream-owned (and a per-sid marker on the fleet-wide backend would
be wrong anyway); hook commands written by ``install_hooks.sh`` are
per-repo and know no sid at install time; the daemon, ingress
forwarders and notify-send are host infrastructure with no session
scope. Marker-less processes fall back to the cwd/marker heuristics -
partial coverage degrades gracefully, it does not mis-attribute.
"""

import os

__all__ = ["SESSION_ID_ENV", "marked_env"]

#: the env var read by the sweep from ``/proc/<pid>/environ``.
SESSION_ID_ENV = "DEVIN_SESSION_ID"


def marked_env(session_id: str, base=None) -> dict:
    """``base`` (default ``os.environ``) plus ``DEVIN_SESSION_ID``.

    The single env-building seam for spawn sites - a falsy
    ``session_id`` leaves the environment untouched so shared-code
    paths without a session context never stamp a wrong marker.
    """
    env = dict(os.environ if base is None else base)
    if session_id:
        env[SESSION_ID_ENV] = session_id
    return env
