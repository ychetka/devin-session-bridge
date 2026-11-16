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
per-repo and know no sid at install time. Marker-less processes fall
back to the cwd/marker heuristics - partial coverage degrades
gracefully, it does not mis-attribute.

Blast radius (issue #168 review): the marker propagates by
inheritance through every spawn inside a marked subtree, so a
LONG-LIVED shared-infra process launched from a marked shell (e.g.
``bridge daemon start`` run inside a marked orchestrator shell) would
carry the caller's sid and become a pass-2 orphan-sweep kill
candidate. Infra entrypoints therefore strip it twice over: the
spawn seams (``cli.commands.ops._spawn_daemon``,
``ingress.sync._spawn``) drop it from the exec-time env - the only
place ``/proc/<pid>/environ`` reflects - and the in-process entries
(``daemon.core.run``, ``ingress.forwarder.main``) call
``unmark_env`` so their children never see it either.
"""

import os

__all__ = ["SESSION_ID_ENV", "marked_env", "unmark_env"]

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


def unmark_env(env=None) -> bool:
    """Drop ``DEVIN_SESSION_ID`` from ``env`` (default ``os.environ``).

    Long-lived shared-infrastructure entrypoints call this so a
    marker the caller's shell happened to carry never propagates into
    infrastructure children - see the module docstring's blast-radius
    note (issue #168). Note an in-process pop cannot rewrite this
    process's OWN ``/proc/<pid>/environ`` (exec-time snapshot); the
    spawn seams strip the var from the exec env for that. Returns
    ``True`` when a marker was actually removed.
    """
    target = os.environ if env is None else env
    return target.pop(SESSION_ID_ENV, None) is not None
