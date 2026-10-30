"""Tests for the hook worker and install_hooks.sh (item_06).

The worker is fed through stdin/stdout capture (``monkeypatch`` on
``sys.stdin``, ``capsys`` for stdout) against a real state.db in a
tmp_path - the db is the contract boundary and stays unmocked.
"""

import io
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from session_bridge.hooks import worker
from session_bridge.state import db
from session_bridge.state.contract import Transport

REPO_ROOT = Path(__file__).resolve().parents[3]
INSTALL_SH = REPO_ROOT / "install_hooks.sh"

SID = "s1"


@pytest.fixture()
def env(tmp_path):
    """Isolated state.db + trigger dir; returns worker argv + paths."""
    db_path = str(tmp_path / "state.db")
    db.init(db_path)
    qdir = tmp_path / "queue"
    qdir.mkdir()
    return {
        "db": db_path,
        "qdir": qdir,
        "argv": [
            "--db",
            db_path,
            "--queue-dir",
            str(qdir),
            "--hook-timeout",
            "60",
            "--cycle-seconds",
            "1",
            "--cycles-max",
            "2",
        ],
    }


def _run(env, payload, monkeypatch, capsys, argv=None):
    """Invoke worker.main with ``payload`` on stdin; (rc, stdout_json)."""
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(payload)))
    rc = worker.main(argv if argv is not None else env["argv"])
    out = capsys.readouterr().out.strip()
    return rc, (json.loads(out) if out else None)


def _stop(sid=SID, active=False):
    return {
        "hook_event_name": "Stop",
        "session_id": sid,
        "stop_hook_active": active,
        "last_assistant_message": "turn done",
    }


def _task(env, sid=SID, text="do the thing", **kw):
    tid = db.enqueue(sid, text, path=env["db"], **kw)
    assert tid is not None
    return tid


def _get_task(env, tid):
    return db.get_task(tid, path=env["db"])


# --- GWT-1: Stop + empty queue -> park / allow per config ---------------------


def test_item06_gwt1_stop_empty_queue_parks(env, monkeypatch, capsys):
    rc, out = _run(env, _stop(), monkeypatch, capsys)
    assert rc == 0
    assert out["decision"] == "block"
    assert out["reason"].startswith("[PARKED]")
    # one park cycle was consumed from the persisted budget
    row = db.get_session(SID, path=env["db"])
    assert row is not None
    assert row["park_cycles_left"] == 1  # cycles_max=2 minus one cycle


def test_item06_gwt1_park_budget_spent_allows_stop(env, monkeypatch, capsys):
    db.upsert_session(SID, park_cycles_left=0, path=env["db"])
    rc, out = _run(env, _stop(active=True), monkeypatch, capsys)
    assert rc == 0
    assert out is None  # cycles exhausted -> allow the stop


def test_item06_gwt1_invalid_park_config_allows(env, monkeypatch, capsys):
    # cycle_seconds > hook_timeout violates the ParkConfig precondition:
    # parking is disabled and the stop is allowed (fail-safe)
    argv = [
        "--db",
        env["db"],
        "--queue-dir",
        str(env["qdir"]),
        "--hook-timeout",
        "60",
        "--cycle-seconds",
        "120",
        "--cycles-max",
        "2",
    ]
    rc, out = _run(env, _stop(), monkeypatch, capsys, argv=argv)
    assert rc == 0
    assert out is None


# --- GWT-2: Stop + queued task -> block deliver + running lease ---------------


def test_item06_gwt2_stop_delivers_queued_task(env, monkeypatch, capsys):
    tid = _task(env)
    rc, out = _run(env, _stop(), monkeypatch, capsys)
    assert rc == 0
    assert out == {
        "decision": "block",
        "reason": "[PARKED>TASK] do the thing",
    }
    row = _get_task(env, tid)
    assert row["status"] == "running"
    assert row["transport"] == Transport.HOOK_QUEUE.value
    assert row["lease_owner"].startswith("hook-")
    assert row["lease_expires_at"] > int(time.time())
    # a delivered task refreshes the park budget
    assert db.get_session(SID, path=env["db"])["park_cycles_left"] == 2


def test_item06_gwt2_stop_delivers_task_file(env, monkeypatch, capsys):
    tid = db.enqueue(SID, task_file="/specs/plan.md", path=env["db"])
    assert tid is not None
    rc, out = _run(env, _stop(), monkeypatch, capsys)
    assert rc == 0
    assert out["reason"] == "[PARKED>TASK] execute spec file /specs/plan.md"
    assert _get_task(env, tid)["status"] == "running"


# --- GWT-3: smart-kick on UserPromptSubmit ------------------------------------


def test_item06_gwt3_short_kick_injects_task(env, monkeypatch, capsys):
    tid = _task(env)
    payload = {
        "hook_event_name": "UserPromptSubmit",
        "session_id": SID,
        "prompt": ".",
    }
    rc, out = _run(env, payload, monkeypatch, capsys)
    assert rc == 0
    inner = out["hookSpecificOutput"]
    assert inner["hookEventName"] == "UserPromptSubmit"
    assert "do the thing" in inner["additionalContext"]
    assert _get_task(env, tid)["status"] == "running"


def test_item06_gwt3_empty_prompt_is_a_kick(env, monkeypatch, capsys):
    _task(env)
    payload = {
        "hook_event_name": "UserPromptSubmit",
        "session_id": SID,
        "prompt": "   ",
    }
    rc, out = _run(env, payload, monkeypatch, capsys)
    assert rc == 0
    assert "additionalContext" in out["hookSpecificOutput"]


def test_item06_gwt3_long_prompt_pauses_queue(env, monkeypatch, capsys):
    tid = _task(env)
    payload = {
        "hook_event_name": "UserPromptSubmit",
        "session_id": SID,
        "prompt": "please refactor the parser module",
    }
    rc, out = _run(env, payload, monkeypatch, capsys)
    assert rc == 0
    assert out is None  # no injection on a real prompt
    assert db.get_session(SID, path=env["db"])["queue_paused"] == 1
    assert _get_task(env, tid)["status"] == "queued"


def test_item06_gwt3_kick_resumes_paused_queue(env, monkeypatch, capsys):
    tid = _task(env)
    db.session_pause(SID, True, path=env["db"])
    payload = {
        "hook_event_name": "UserPromptSubmit",
        "session_id": SID,
        "prompt": "..",
    }
    rc, out = _run(env, payload, monkeypatch, capsys)
    assert rc == 0
    # the kick clears the pause first, so the queued task is claimable
    # again inside the same call and gets injected
    assert db.get_session(SID, path=env["db"])["queue_paused"] == 0
    assert "do the thing" in out["hookSpecificOutput"]["additionalContext"]
    assert _get_task(env, tid)["status"] == "running"


# --- GWT-4: exhaustion/pause -> awaiting_user + deduped notify ----------------


def test_item06_gwt4_cycles_exhausted_flags_awaiting_user(env, monkeypatch, capsys):
    tid = _task(env)
    db.upsert_session(SID, park_cycles_left=0, path=env["db"])
    rc, out = _run(env, _stop(active=True), monkeypatch, capsys)
    assert rc == 0
    assert out is None  # allow the stop
    row = _get_task(env, tid)
    assert row["status"] == "awaiting_user"
    assert row["last_notified_at"] is not None
    assert row["lease_owner"] is None


def test_item06_gwt4_notify_flag_is_deduplicated(env, monkeypatch, capsys):
    # two flagged stops inside the dedup window: mark_notified loses its
    # CAS the second time and last_notified_at must not move
    tid = _task(env)
    db.session_pause(SID, True, path=env["db"])
    _run(env, _stop(), monkeypatch, capsys)
    first = _get_task(env, tid)["last_notified_at"]
    assert first is not None
    time.sleep(1.1)
    _run(env, _stop(), monkeypatch, capsys)
    assert _get_task(env, tid)["last_notified_at"] == first


def test_item06_gwt4_paused_queue_flags_notify_no_block(env, monkeypatch, capsys):
    tid = _task(env)
    db.session_pause(SID, True, path=env["db"])
    rc, out = _run(env, _stop(), monkeypatch, capsys)
    assert rc == 0
    assert out is None
    row = _get_task(env, tid)
    # a paused queued task cannot be claimed, so it keeps its status
    # but is flagged for the notifier
    assert row["status"] == "queued"
    assert row["last_notified_at"] is not None


def test_item06_gwt4_hook_owned_running_parks_for_user(env, monkeypatch, capsys):
    # a hook-delivered task still running when Stop fires again means
    # the agent finished the injected turn -> awaiting_user + notify
    tid = _task(env)
    assert db.claim_queued(tid, "hook-999", Transport.HOOK_QUEUE, path=env["db"])
    assert db.mark_running(tid, "hook-999", path=env["db"])
    rc, out = _run(env, _stop(active=True), monkeypatch, capsys)
    assert rc == 0
    assert out is None
    row = _get_task(env, tid)
    assert row["status"] == "awaiting_user"
    assert row["lease_owner"] is None
    assert row["last_notified_at"] is not None


# --- GWT-5: SessionStart / SessionEnd ----------------------------------------


def test_item06_gwt5_session_start_records_session(env, monkeypatch, capsys):
    payload = {
        "hook_event_name": "SessionStart",
        "session_id": SID,
        "source": "startup",
        "cwd": "/repo/x",
    }
    rc, out = _run(env, payload, monkeypatch, capsys)
    assert rc == 0
    assert out is None
    row = db.get_session(SID, path=env["db"])
    assert row is not None
    assert row["cwd"] == "/repo/x"
    assert row["park_cycles_left"] == 2  # fresh budget


def test_item06_gwt5_session_start_resume_recovers_stale(env, monkeypatch, capsys):
    tid = _task(env)
    assert db.claim_queued(tid, "w-old", Transport.ACP_PUSH, path=env["db"])
    assert db.mark_running(tid, "w-old", path=env["db"])
    con = db.connect(env["db"])
    try:
        con.execute(
            "UPDATE tasks SET lease_expires_at=? WHERE id=?",
            (int(time.time()) - 1, tid),
        )
    finally:
        con.close()
    payload = {
        "hook_event_name": "SessionStart",
        "session_id": SID,
        "source": "resume",
    }
    rc, out = _run(env, payload, monkeypatch, capsys)
    assert rc == 0
    assert out is None
    row = _get_task(env, tid)
    assert row["status"] == "queued"  # stale lease recovered
    assert row["attempt"] == 1


def test_item06_gwt5_session_end_fails_queued_orphans(env, monkeypatch, capsys):
    tid = _task(env)
    payload = {
        "hook_event_name": "SessionEnd",
        "session_id": SID,
        "reason": "clear",
    }
    rc, out = _run(env, payload, monkeypatch, capsys)
    assert rc == 0
    assert out is None
    row = _get_task(env, tid)
    assert row["status"] == "failed"
    assert row["note"] == "session ended"
    assert "clear" in db.get_session(SID, path=env["db"])["note"]


def test_item06_gwt5_session_end_parks_running_orphan(env, monkeypatch, capsys):
    tid = _task(env)
    assert db.claim_queued(tid, "w-acp", Transport.ACP_PUSH, path=env["db"])
    assert db.mark_running(tid, "w-acp", path=env["db"])
    payload = {
        "hook_event_name": "SessionEnd",
        "session_id": SID,
        "reason": "logout",
    }
    rc, out = _run(env, payload, monkeypatch, capsys)
    assert rc == 0
    assert out is None
    row = _get_task(env, tid)
    assert row["status"] == "awaiting_user"
    assert row["note"] == "session ended"
    assert row["lease_owner"] is None


def test_item06_session_end_race_keeps_running_lease(env, monkeypatch, capsys):
    # regression: the task was dispatch_decided when read, but the real
    # owner marked it running before our mark_running CAS arrived -
    # the release_lease fallback must not strip the running task's
    # lease (a running task without a lease is unrecoverable)
    tid = _task(env)
    owner = "acp-7-1"
    assert db.claim_queued(tid, owner, Transport.ACP_PUSH, path=env["db"])

    real_mark_running = db.mark_running

    def owner_won_the_race(task_id, o, *, path=None):
        # the owner's transition lands first; our CAS then loses
        real_mark_running(task_id, o, path=path)
        return False

    monkeypatch.setattr(db, "mark_running", owner_won_the_race)
    payload = {
        "hook_event_name": "SessionEnd",
        "session_id": SID,
        "reason": "logout",
    }
    rc, out = _run(env, payload, monkeypatch, capsys)
    assert rc == 0
    assert out is None
    row = _get_task(env, tid)
    assert row["status"] == "running"
    assert row["lease_owner"] == owner
    assert row["lease_expires_at"] is not None


# --- queue trigger inside the park cycle --------------------------------------


def test_item06_trigger_arriving_mid_park_delivers(env, monkeypatch, capsys):
    trigger = Path(env["qdir"]) / f"{SID}.json"

    def enqueue_later():
        time.sleep(0.3)
        _task(env)
        trigger.write_text(json.dumps({"session_id": SID}))

    threading.Thread(target=enqueue_later, daemon=True).start()
    argv = list(env["argv"])
    argv[argv.index("--cycle-seconds") + 1] = "5"
    rc, out = _run(env, _stop(), monkeypatch, capsys, argv=argv)
    assert rc == 0
    assert out["reason"] == "[PARKED>TASK] do the thing"


def test_item06_invalid_trigger_is_consumed_and_parked(env, monkeypatch, capsys):
    (Path(env["qdir"]) / f"{SID}.json").write_text("{not json")
    rc, out = _run(env, _stop(), monkeypatch, capsys)
    assert rc == 0
    assert out["decision"] == "block"  # spurious trigger -> park cycle
    assert not (Path(env["qdir"]) / f"{SID}.json").exists()


# --- fail-safe ----------------------------------------------------------------


def test_item06_db_error_never_blocks(env, monkeypatch, capsys):
    Path(env["db"]).write_bytes(b"not a sqlite database")
    rc, out = _run(env, _stop(), monkeypatch, capsys)
    assert rc == 0
    assert out is None


def test_item06_garbage_payload_never_blocks(env, monkeypatch, capsys):
    monkeypatch.setattr(sys, "stdin", io.StringIO("not json{"))
    rc = worker.main(env["argv"])
    assert rc == 0
    assert capsys.readouterr().out.strip() == ""


# --- GWT-6: install_hooks.sh --------------------------------------------------


def _install(repo: Path):
    return subprocess.run(
        ["bash", str(INSTALL_SH), str(repo)],
        capture_output=True,
        text=True,
        timeout=30,
    )


def _hooks(repo: Path):
    return json.loads((repo / ".devin" / "hooks.v1.json").read_text())


def test_item06_gwt6_install_on_repo_without_devin(tmp_path):
    repo = tmp_path / "target"
    repo.mkdir()
    res = _install(repo)
    assert res.returncode == 0, res.stderr
    data = _hooks(repo)
    for event in (
        "SessionStart",
        "SessionEnd",
        "UserPromptSubmit",
        "Stop",
    ):
        assert event in data
    stop_cmds = [
        h["command"]
        for e in data["Stop"]
        for h in e["hooks"]
        if "worker.py" in h.get("command", "")
    ]
    assert len(stop_cmds) == 1
    # the command embeds the park budget and names worker.py absolutely
    assert str(REPO_ROOT / "src/session_bridge/hooks/worker.py") in stop_cmds[0]
    assert "--cycle-seconds" in stop_cmds[0]
    # installed timeout covers a park cycle
    ours = next(h for e in data["Stop"] for h in e["hooks"] if "worker.py" in h.get("command", ""))
    assert ours["timeout"] >= 150


def test_item06_gwt6_install_is_idempotent_and_preserves(tmp_path):
    repo = tmp_path / "target"
    (repo / ".devin").mkdir(parents=True)
    foreign = {
        "Stop": [
            {
                "matcher": "",
                "hooks": [
                    {
                        "type": "command",
                        "command": "other-tool hook",
                        "timeout": 15,
                    }
                ],
            }
        ]
    }
    (repo / ".devin" / "hooks.v1.json").write_text(json.dumps(foreign))
    assert _install(repo).returncode == 0
    assert _install(repo).returncode == 0  # second run stays clean
    data = _hooks(repo)
    stop = data["Stop"]
    foreign_left = [h for e in stop for h in e["hooks"] if h.get("command") == "other-tool hook"]
    ours = [h for e in stop for h in e["hooks"] if "worker.py" in h.get("command", "")]
    assert len(foreign_left) == 1  # foreign hook not clobbered
    assert len(ours) == 1  # no duplicate entries after re-install
    assert (repo / ".devin" / "hooks.v1.json.bak").exists()


def test_item06_gwt6_install_on_missing_repo_fails(tmp_path):
    res = _install(tmp_path / "nonexistent")
    assert res.returncode != 0


def test_item06_gwt6_install_fails_without_pydantic(tmp_path):
    # regression: an interpreter missing the runtime deps must abort
    # the install, not leave a hook that dies on import at every event
    repo = tmp_path / "target"
    repo.mkdir()
    fake = tmp_path / "python-no-deps"
    fake.write_text("#!/bin/sh\nexit 1\n")
    fake.chmod(0o755)
    res = subprocess.run(
        ["bash", str(INSTALL_SH), str(repo)],
        capture_output=True,
        text=True,
        timeout=30,
        env={**os.environ, "SESSIONS_BRIDGE_PYTHON": str(fake)},
    )
    assert res.returncode != 0
    assert "pydantic" in res.stderr
    assert not (repo / ".devin" / "hooks.v1.json").exists()


def test_item06_gwt6_install_fails_without_session_bridge(tmp_path):
    # a python that has pydantic but cannot import the package must
    # abort too - the PYTHONPATH wiring is part of the check
    repo = tmp_path / "target"
    repo.mkdir()
    fake = tmp_path / "python-no-bridge"
    fake.write_text('#!/bin/sh\nif [ "$2" = "import pydantic" ]; then\n    exit 0\nfi\nexit 1\n')
    fake.chmod(0o755)
    res = subprocess.run(
        ["bash", str(INSTALL_SH), str(repo)],
        capture_output=True,
        text=True,
        timeout=30,
        env={**os.environ, "SESSIONS_BRIDGE_PYTHON": str(fake)},
    )
    assert res.returncode != 0
    assert "session_bridge" in res.stderr
    assert not (repo / ".devin" / "hooks.v1.json").exists()


# --- item_11: hooks.v1.json top-level format (issue #7) ------------------------
#
# The hooks object is the entire file: top-level keys are event names,
# no "hooks" wrapper key. Files written by the pre-fix installer
# ({"hooks": {...}}) are migrated in place - foreign entries move to
# the top level along with ours.


EVENT_NAMES = (
    "SessionStart",
    "SessionEnd",
    "UserPromptSubmit",
    "Stop",
)

LEGACY_OUR_HOOK = {
    "type": "command",
    "command": "PYTHONPATH=/old /usr/bin/python3 "
    "/old/src/session_bridge/hooks/worker.py "
    "--hook-timeout 60 --cycle-seconds 150 --cycles-max 5",
    "timeout": 60,
}


def _our_hooks(data, event):
    return [
        h
        for e in data.get(event, [])
        for h in e.get("hooks", [])
        if isinstance(h.get("command"), str) and "session_bridge/hooks/worker.py" in h["command"]
    ]


def test_item11_gwt1_fresh_install_uses_top_level_event_keys(tmp_path):
    repo = tmp_path / "target"
    repo.mkdir()
    res = _install(repo)
    assert res.returncode == 0, res.stderr
    data = _hooks(repo)
    assert "hooks" not in data  # no wrapper key in v1 format
    for event in EVENT_NAMES:
        assert isinstance(data.get(event), list), event
        assert len(_our_hooks(data, event)) == 1


def test_item11_gwt2_legacy_wrapper_migrates_to_top_level(tmp_path):
    repo = tmp_path / "target"
    (repo / ".devin").mkdir(parents=True)
    legacy = {
        "hooks": {
            "Stop": [
                {"matcher": "", "hooks": [dict(LEGACY_OUR_HOOK)]},
                {
                    "matcher": "",
                    "hooks": [
                        {
                            "type": "command",
                            "command": "other-tool stop",
                            "timeout": 15,
                        }
                    ],
                },
            ],
            "SessionStart": [{"matcher": "", "hooks": [dict(LEGACY_OUR_HOOK)]}],
            "PreToolUse": [
                {
                    "matcher": "Bash",
                    "hooks": [{"type": "command", "command": "other-tool pre"}],
                }
            ],
        }
    }
    (repo / ".devin" / "hooks.v1.json").write_text(json.dumps(legacy))
    res = _install(repo)
    assert res.returncode == 0, res.stderr
    data = _hooks(repo)
    assert "hooks" not in data  # wrapper migrated away
    # our legacy entries moved to the top level and were refreshed
    # in place - exactly one copy per event, new command + timeout
    for event in EVENT_NAMES:
        ours = _our_hooks(data, event)
        assert len(ours) == 1, event
        assert "--hook-timeout 600" in ours[0]["command"]
    assert _our_hooks(data, "Stop")[0]["timeout"] == 600
    # foreign entries under the wrapper were migrated, not dropped
    migrated_foreign = [
        h["command"]
        for e in data["Stop"]
        for h in e["hooks"]
        if h.get("command") == "other-tool stop"
    ]
    assert migrated_foreign == ["other-tool stop"]
    assert data["PreToolUse"][0]["hooks"][0]["command"] == "other-tool pre"
    assert (repo / ".devin" / "hooks.v1.json.bak").exists()
    # the backup keeps the pre-migration bytes for recovery
    bak = json.loads((repo / ".devin" / "hooks.v1.json.bak").read_text())
    assert "hooks" in bak


def test_item11_gwt3_foreign_top_level_entries_not_clobbered(tmp_path):
    repo = tmp_path / "target"
    (repo / ".devin").mkdir(parents=True)
    foreign = {
        "Stop": [
            {
                "matcher": "",
                "hooks": [
                    {
                        "type": "command",
                        "command": "other-tool stop",
                        "timeout": 15,
                    }
                ],
            }
        ],
        "PreToolUse": [
            {
                "matcher": "Bash",
                "hooks": [{"type": "command", "command": "other-tool pre"}],
            }
        ],
    }
    (repo / ".devin" / "hooks.v1.json").write_text(json.dumps(foreign))
    res = _install(repo)
    assert res.returncode == 0, res.stderr
    data = _hooks(repo)
    assert "hooks" not in data
    # foreign entries survive the merge untouched
    assert data["PreToolUse"] == foreign["PreToolUse"]
    foreign_stop = [
        h for e in data["Stop"] for h in e["hooks"] if h.get("command") == "other-tool stop"
    ]
    assert len(foreign_stop) == 1
    assert foreign_stop[0]["timeout"] == 15
    # our events were merged alongside them
    for event in EVENT_NAMES:
        assert len(_our_hooks(data, event)) == 1


def test_item11_non_dict_hooks_wrapper_kept_under_legacy_key(tmp_path):
    # a stray non-dict "hooks" value cannot be hook data - it is moved
    # aside under "_legacy_hooks" instead of being dropped, and the
    # rest of the file still gets a clean top-level install
    repo = tmp_path / "target"
    (repo / ".devin").mkdir(parents=True)
    (repo / ".devin" / "hooks.v1.json").write_text(json.dumps({"hooks": "not-a-hook-map"}))
    res = _install(repo)
    assert res.returncode == 0, res.stderr
    data = _hooks(repo)
    assert "hooks" not in data
    assert data["_legacy_hooks"] == "not-a-hook-map"
    for event in EVENT_NAMES:
        assert len(_our_hooks(data, event)) == 1


def test_item11_mixed_legacy_and_top_level_dedupes_our_hook(tmp_path):
    # our hook present both under the legacy wrapper and at the top
    # level: migration concatenates the lists, the merge must collapse
    # the copies to exactly one and leave no empty entry husk behind
    repo = tmp_path / "target"
    (repo / ".devin").mkdir(parents=True)
    mixed = {
        "Stop": [
            {"matcher": "", "hooks": [dict(LEGACY_OUR_HOOK)]},
            {
                "matcher": "",
                "hooks": [{"type": "command", "command": "other-tool stop"}],
            },
        ],
        "hooks": {
            "Stop": [{"matcher": "", "hooks": [dict(LEGACY_OUR_HOOK)]}],
        },
    }
    (repo / ".devin" / "hooks.v1.json").write_text(json.dumps(mixed))
    res = _install(repo)
    assert res.returncode == 0, res.stderr
    data = _hooks(repo)
    assert "hooks" not in data
    ours = _our_hooks(data, "Stop")
    assert len(ours) == 1
    assert ours[0]["timeout"] == 600  # refreshed, not a stale copy
    # the deduped duplicate left no {"hooks": []} husk behind
    assert all(e.get("hooks") for e in data["Stop"])
    foreign = [h for e in data["Stop"] for h in e["hooks"] if h.get("command") == "other-tool stop"]
    assert len(foreign) == 1


def test_item11_foreign_hook_with_non_string_command_ignored(tmp_path):
    # regression: a foreign hook entry whose "command" is not a string
    # (null, int) must not crash the marker check - it is simply not
    # ours and passes through untouched
    repo = tmp_path / "target"
    (repo / ".devin").mkdir(parents=True)
    foreign = {
        "Stop": [
            {
                "matcher": "",
                "hooks": [
                    {"type": "command", "command": 123},
                    {"type": "command", "command": None},
                ],
            }
        ]
    }
    (repo / ".devin" / "hooks.v1.json").write_text(json.dumps(foreign))
    res = _install(repo)
    assert res.returncode == 0, res.stderr
    data = _hooks(repo)
    non_str = [
        h["command"]
        for e in data["Stop"]
        for h in e["hooks"]
        if not isinstance(h.get("command"), str)
    ]
    assert non_str == [123, None]  # preserved verbatim
    for event in EVENT_NAMES:
        assert len(_our_hooks(data, event)) == 1
