"""Guard-sweep logic tests (issue #9, headless).

The sweep's subprocess calls to ``devin rm`` need the live binary and
are exercised by GWT-1's real teardown path instead; what this file
pins is the matching logic - which sessions count as e2e orphans -
and the sessions.db scan.
"""

import sqlite3

from e2e.conftest import (
    _is_e2e_scratch,
    _scratch_orphans,
    _sweep_root,
)
from session_bridge.state import db as bridge_db


def test_item09_gwt1_scratch_path_matching(tmp_path):
    """[ITEM-09.GWT-1] only scratch-repo dirs under the sweep root
    match; a real session sharing the basename is safe."""
    root = tmp_path / "pytest-of-u"
    assert _is_e2e_scratch(str(root / "pytest-1" / "test_x0" / "scratch-repo"), root)
    assert _is_e2e_scratch(str(root / "pytest-6" / "old_run" / "scratch-repo"), root)
    # same basename outside the sweep root -> not ours
    assert not _is_e2e_scratch("/home/user/work/scratch-repo", root)
    assert not _is_e2e_scratch("/home/user/pytest-plugins/scratch-repo", root)
    # under the root but a different basename -> not ours
    assert not _is_e2e_scratch(str(root / "pytest-1" / "test_x0" / "other-repo"), root)
    assert not _is_e2e_scratch("", root)
    assert not _is_e2e_scratch(None, root)


def test_item09_gwt2_orphan_scan_reads_only_scratch_rows(tmp_path, monkeypatch):
    """[ITEM-09.GWT-2] the sessions.db scan returns exactly the rows
    whose cwd is an e2e scratch repo under the sweep root."""
    root = tmp_path / "pytest-of-u"
    scratch = root / "pytest-7" / "test_live0" / "scratch-repo"
    sessions_db = tmp_path / "sessions.db"
    con = sqlite3.connect(sessions_db)
    try:
        con.execute(
            "CREATE TABLE sessions (id TEXT PRIMARY KEY, working_directory TEXT, title TEXT)"
        )
        con.execute(
            "INSERT INTO sessions VALUES ('orphan-1', ?, NULL)",
            (str(scratch),),
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

    rows = _scratch_orphans(root)
    assert rows == [("orphan-1", str(scratch))]


def test_item09_gwt3_missing_sessions_db_returns_empty(tmp_path, monkeypatch):
    """[ITEM-09.GWT-3] no devin sessions.db at all -> empty, no error."""
    monkeypatch.setattr(bridge_db, "DEVIN_SESSIONS_DB", str(tmp_path / "absent.db"))
    assert _scratch_orphans(tmp_path) == []


def test_item09_gwt4_sweep_root_scopes(tmp_path):
    """[ITEM-09.GWT-4] default basetemp sweeps under pytest-of-<user>;
    a custom basetemp confines the sweep to itself."""
    basetemp = tmp_path / "pytest-of-u" / "pytest-9"
    basetemp.mkdir(parents=True)

    class _FakeFactory:
        def __init__(self, bt):
            self._bt = bt

        def getbasetemp(self):
            return self._bt

    assert _sweep_root(_FakeFactory(basetemp)) == tmp_path / "pytest-of-u"
    custom = tmp_path / "custom-bt"
    custom.mkdir()
    assert _sweep_root(_FakeFactory(custom)) == custom
    # a leaf named pytest-* NOT under pytest-of-* stays confined
    sneaky = tmp_path / "pytest-mine"
    sneaky.mkdir()
    assert _sweep_root(_FakeFactory(sneaky)) == sneaky
