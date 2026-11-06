"""e2e conftest: orphan devin-session sweep (issue #9).

The per-test ``scratch_sessions`` fixture deletes every registered
scratch session via ``devin rm --force`` in teardown, but a session
can outlive it: a crash between ``client.new_session`` and the
fixture append, a run aborted mid-test, or a future live test that
forgets to register. This module-scoped autouse fixture runs once
after the whole e2e module and removes any devin session whose
working_directory is a pytest scratch repo - the only places this
suite is allowed to spawn real sessions.

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

#: scratch repos are always created at ``tmp_path / "scratch-repo"``;
#: a ``pytest-*`` path component (pytest-of-<user>, pytest-<n>) guards
#: the sweep from ever touching a real session that happens to share
#: the basename - a bare ``pytest`` component does not count
_SCRATCH_BASENAME = "scratch-repo"


def _live_devin_available() -> bool:
    """True when a real devin binary + credentials could have spawned
    sessions this run (mirrors test_e2e._live_acp_skip_reason)."""
    return shutil.which("devin") is not None and os.path.isfile(
        os.path.expanduser("~/.local/share/devin/credentials.toml")
    )


def _is_e2e_scratch(cwd: str | None) -> bool:
    """True when ``cwd`` is a pytest scratch repo this suite may have
    spawned a session in."""
    if not cwd:
        return False
    p = Path(cwd)
    return p.name == _SCRATCH_BASENAME and any(part.startswith("pytest-") for part in p.parts)


def _scratch_orphans():
    """(session_id, working_directory) rows from devin's own
    sessions.db whose cwd is an e2e scratch repo."""
    sessions_db = Path(bridge_db.DEVIN_SESSIONS_DB)
    if not sessions_db.is_file():
        return []
    try:
        con = sqlite3.connect(f"file:{sessions_db}?mode=ro", uri=True)
        try:
            rows = con.execute("SELECT id, working_directory FROM sessions").fetchall()
        finally:
            con.close()
    except sqlite3.Error:
        return []
    return [(sid, cwd) for sid, cwd in rows if _is_e2e_scratch(cwd)]


@pytest.fixture(scope="module", autouse=True)
def orphan_devin_session_sweep():
    """Guard sweep: after the e2e module, delete any leftover devin
    session whose cwd is a pytest scratch repo (issue #9)."""
    yield
    if not _live_devin_available():
        return
    for sid, cwd in _scratch_orphans():
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
