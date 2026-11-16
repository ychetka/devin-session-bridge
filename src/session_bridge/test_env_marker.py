"""Tests for the DEVIN_SESSION_ID env-marker helpers (issue #168)."""

import os

from session_bridge.env_marker import SESSION_ID_ENV, marked_env, unmark_env


def test_issue168_marked_env_stamps_sid(monkeypatch):
    """[ISSUE-168.GWT-9] marked_env copies the ambient env plus the
    marker; a falsy sid leaves the environment untouched so shared
    code paths never stamp a wrong marker."""
    monkeypatch.setenv("KEEP_ME", "1")
    monkeypatch.delenv(SESSION_ID_ENV, raising=False)

    env = marked_env("s1")
    assert env[SESSION_ID_ENV] == "s1"
    assert env["KEEP_ME"] == "1"
    assert SESSION_ID_ENV not in os.environ  # base env not mutated

    assert SESSION_ID_ENV not in marked_env("")
    assert SESSION_ID_ENV not in marked_env(None)


def test_issue168_unmark_env_strips_and_reports(monkeypatch):
    """[ISSUE-168.GWT-10] unmark_env removes an inherited marker from
    os.environ (the infra-entrypoint blast-radius guard) and reports
    whether anything was actually removed."""
    monkeypatch.setenv(SESSION_ID_ENV, "s1")
    assert unmark_env() is True
    assert SESSION_ID_ENV not in os.environ
    assert unmark_env() is False

    # explicit dict form for spawn-seam use
    env = {SESSION_ID_ENV: "s1", "OTHER": "x"}
    assert unmark_env(env) is True
    assert env == {"OTHER": "x"}
