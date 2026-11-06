"""Guard-sweep logic tests (issue #9, headless).

The sweep's subprocess calls to ``devin rm`` need the live binary and
are exercised by GWT-1's real teardown path instead; what this file
pins is the matching logic - which sessions count as e2e orphans -
and the sessions.db scan.
"""

import sqlite3

from e2e.conftest import _is_e2e_scratch, _scratch_orphans
from session_bridge.state import db as bridge_db


def test_item09_gwt1_scratch_path_matching():
    """[ITEM-09.GWT-1] only pytest tmp dirs named scratch-repo match;
    a real session sharing the basename is safe."""
    assert _is_e2e_scratch("/tmp/pytest-of-u/pytest-1/test_x0/scratch-repo")
    assert _is_e2e_scratch("/tmp/pytest-42/test_item08/scratch-repo")
    # same basename outside a pytest tmp dir -> not ours
    assert not _is_e2e_scratch("/home/user/work/scratch-repo")
    # pytest tmp dir but a different basename -> not ours
    assert not _is_e2e_scratch("/tmp/pytest-1/test_x0/other-repo")
    assert not _is_e2e_scratch("")
    assert not _is_e2e_scratch(None)


def test_item09_gwt2_orphan_scan_reads_only_scratch_rows(tmp_path, monkeypatch):
    """[ITEM-09.GWT-2] the sessions.db scan returns exactly the rows
    whose cwd is an e2e scratch repo."""
    sessions_db = tmp_path / "sessions.db"
    con = sqlite3.connect(sessions_db)
    try:
        con.execute(
            "CREATE TABLE sessions (id TEXT PRIMARY KEY, working_directory TEXT, title TEXT)"
        )
        con.execute(
            "INSERT INTO sessions VALUES ('orphan-1', ?, NULL)",
            ("/tmp/pytest-of-u/pytest-7/test_live0/scratch-repo",),
        )
        con.execute(
            "INSERT INTO sessions VALUES ('real-1', ?, NULL)",
            ("/home/user/projects/real-repo",),
        )
        con.execute(
            "INSERT INTO sessions VALUES ('near-miss', ?, NULL)",
            ("/home/user/pytest/scratch-repo",),
        )
        con.commit()
    finally:
        con.close()
    monkeypatch.setattr(bridge_db, "DEVIN_SESSIONS_DB", str(sessions_db))

    rows = _scratch_orphans()
    assert rows == [("orphan-1", "/tmp/pytest-of-u/pytest-7/test_live0/scratch-repo")]


def test_item09_gwt3_missing_sessions_db_returns_empty(tmp_path, monkeypatch):
    """[ITEM-09.GWT-3] no devin sessions.db at all -> empty, no error."""
    monkeypatch.setattr(bridge_db, "DEVIN_SESSIONS_DB", str(tmp_path / "absent.db"))
    assert _scratch_orphans() == []
