"""e2e conftest: orphan devin-session sweep (issue #9).

The per-test ``scratch_sessions`` fixture deletes every registered
scratch session via ``devin rm --force`` in teardown, but a session
can outlive it: a crash mid-test, an aborted run, or a future live
test that forgets to register. This module-scoped autouse fixture
runs once after the whole e2e module and removes any leftover devin
session whose working_directory sits under the pytest scratch root -
the only place this suite is allowed to spawn real sessions.

Safety contract:
- matching is scoped to the pytest basetemp family: default runs
  sweep under ``<tmp>/pytest-of-<user>/`` (covers orphans of earlier
  crashed runs of the same user); a custom ``--basetemp`` sweeps only
  that exact directory - nothing outside pytest tmp dirs is ever a
  candidate;
- sessions still locked by a live devin process (Desktop-held) are
  skipped via ``db.is_locked``;
- ``devin rm`` runs from HOME when the scratch dir is already gone;
- a missing/unreadable sessions.db yields no orphans, no error.

Orphans that never materialized a turn are invisible here by design:
``session/new`` does not persist into sessions.db until the first
turn, and such sessions die with their acp host - the sweep only
needs to catch sessions that DID materialize.

Hygiene rule (mirrored in .windsurfrules): a live e2e test may only
create devin sessions inside a scratch repo named ``scratch-repo``
under the pytest tmp dir, and must register it with
``scratch_sessions`` immediately after ``new_session`` returns.
"""

import os
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from session_bridge.state import db as bridge_db

#: scratch repos are always created at ``tmp_path / "scratch-repo"``
_SCRATCH_BASENAME = "scratch-repo"


def _live_devin_available() -> bool:
    """True when a real devin binary + credentials could have spawned
    sessions this run (mirrors test_e2e._live_acp_skip_reason)."""
    return shutil.which("devin") is not None and os.path.isfile(
        os.path.expanduser("~/.local/share/devin/credentials.toml")
    )


def _sweep_root(tmp_path_factory) -> Path:
    """Directory the sweep is confined to.

    Default pytest basetemp (``<tmp>/pytest-of-<user>/pytest-<N>``)
    sweeps under the ``pytest-of-<user>`` parent so orphans of
    earlier crashed runs are caught too; any custom ``--basetemp``
    confines the sweep to that directory alone.
    """
    basetemp = Path(tmp_path_factory.getbasetemp())
    if basetemp.name.startswith("pytest-"):
        return basetemp.parent
    return basetemp


def _is_e2e_scratch(cwd: str | None, root: Path) -> bool:
    """True when ``cwd`` is a scratch repo under the sweep root."""
    if not cwd:
        return False
    p = Path(cwd)
    if p.name != _SCRATCH_BASENAME:
        return False
    try:
        p.relative_to(root)
    except ValueError:
        return False
    return True


def _scratch_orphans(root: Path):
    """(session_id, working_directory) rows from devin's own
    sessions.db whose cwd is an e2e scratch repo under ``root``."""
    sessions_db = Path(bridge_db.DEVIN_SESSIONS_DB)
    if not sessions_db.is_file():
        return []

    def op():
        con = sqlite3.connect(f"file:{sessions_db}?mode=ro", uri=True)
        try:
            return con.execute("SELECT id, working_directory FROM sessions").fetchall()
        finally:
            con.close()

    try:
        rows = bridge_db._retry(op, tries=3, base=0.1)
    except sqlite3.Error:
        return []
    return [(sid, cwd) for sid, cwd in rows if _is_e2e_scratch(cwd, root)]


@pytest.fixture(scope="module", autouse=True)
def orphan_devin_session_sweep(tmp_path_factory):
    """Guard sweep: after the e2e module, delete any leftover devin
    session whose cwd is a pytest scratch repo (issue #9)."""
    yield
    if not _live_devin_available():
        return
    root = _sweep_root(tmp_path_factory)
    for sid, cwd in _scratch_orphans(root):
        locked, _pid = bridge_db.is_locked(sid)
        if locked:
            # Desktop-held session - a human may be in it; never force
            # delete a live session from under someone's hands
            continue
        try:
            res = subprocess.run(
                ["devin", "rm", sid, "--force"],
                # the scratch dir may already be deleted - run from
                # HOME, only pass cwd when it still exists
                cwd=cwd if os.path.isdir(cwd) else os.path.expanduser("~"),
                capture_output=True,
                text=True,
                timeout=60,
            )
        except Exception as e:
            print(
                f"sessions-bridge e2e: orphan sweep devin rm {sid} raised {e!r}",
                file=sys.stderr,
            )
            continue
        if res.returncode != 0:
            print(
                f"sessions-bridge e2e: orphan sweep devin rm {sid} exited"
                f" {res.returncode}: {res.stderr.strip()}",
                file=sys.stderr,
            )
        else:
            print(
                f"sessions-bridge e2e: orphan sweep removed {sid}",
                file=sys.stderr,
            )
