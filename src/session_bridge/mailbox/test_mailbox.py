"""Tests for the fs mailbox + managed-mode watcher duty (item_13c).

The mailbox is plain files under ``<queue root>/<sid>/``; tests use a
tmp queue dir and exercise the real write/next/consume path plus the
rendered WATCHER_LOOP bash (run for real when bash is available).
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

from session_bridge.mailbox import (
    POLL_SECONDS,
    WATCHER_LOOP,
    consume,
    mailbox_dir,
    mailbox_duty_text,
    next_message,
    queue_root,
    watcher_loop_text,
    write_message,
)
from session_bridge.state.contract import TURN_TTL_CYCLES, WATCHER_TTL_SECONDS

SID = "s1"


@pytest.fixture()
def qdir(tmp_path):
    return str(tmp_path / "queue")


def _msg_files(mdir):
    return [n for n in os.listdir(mdir) if n.startswith("msg_")]


# --- mailbox_dir / queue_root ------------------------------------------------


def test_item13c_mailbox_dir_under_queue_root(qdir):
    assert mailbox_dir(SID, queue_dir=qdir) == os.path.join(qdir, SID)


def test_item13c_queue_root_precedence(tmp_path, monkeypatch):
    # explicit queue_dir wins over the env override and the db-derived path
    monkeypatch.setenv("SESSIONS_BRIDGE_QUEUE_DIR", "/env/queue")
    assert queue_root(queue_dir="/explicit") == "/explicit"
    assert queue_root() == "/env/queue"
    monkeypatch.delenv("SESSIONS_BRIDGE_QUEUE_DIR")
    db_path = str(tmp_path / "state.db")
    assert queue_root(db_path) == os.path.join(tmp_path, "queue")


def test_item13c_session_id_sanitized_into_dir(qdir):
    # slashes/.. cannot escape the queue root
    mdir = mailbox_dir("../evil/sid", queue_dir=qdir)
    assert mdir.startswith(qdir + os.sep)
    assert ".." not in Path(mdir).relative_to(qdir).parts


# --- GWT-1: write_message -> next_message -> consume -------------------------


def test_item13c_gwt1_write_next_consume_cycle(qdir):
    path = write_message(SID, 7, "task payload", queue_dir=qdir)
    assert path == os.path.join(qdir, SID, "msg_7.txt")
    hit = next_message(SID, queue_dir=qdir)
    assert hit is not None
    rpath, text = hit
    assert rpath == path
    assert text == "task payload"
    # level-triggered: the message stays until consumed
    assert next_message(SID, queue_dir=qdir) == hit
    assert consume(rpath) is True
    assert next_message(SID, queue_dir=qdir) is None
    # consume is idempotent
    assert consume(rpath) is False


def test_item13c_next_message_empty_and_missing_dir(qdir):
    assert next_message("ghost", queue_dir=qdir) is None
    mdir = mailbox_dir(SID, queue_dir=qdir)
    os.makedirs(mdir)
    assert next_message(SID, queue_dir=qdir) is None


def test_item13c_head_of_queue_by_notification_id(qdir):
    for nid in (3, 1, 2):
        write_message(SID, nid, f"payload {nid}", queue_dir=qdir)
    seen = []
    while True:
        hit = next_message(SID, queue_dir=qdir)
        if hit is None:
            break
        path, text = hit
        seen.append(text)
        assert consume(path)
    # head-of-queue order is the notification id, not the write order
    assert seen == ["payload 1", "payload 2", "payload 3"]


def test_item13c_id_order_is_numeric_not_lexical(qdir):
    # msg_2 must beat msg_10 at the head of the queue
    write_message(SID, 10, "ten", queue_dir=qdir)
    write_message(SID, 2, "two", queue_dir=qdir)
    hit = next_message(SID, queue_dir=qdir)
    assert hit[1] == "two"


# --- write atomicity ---------------------------------------------------------


def test_item13c_write_is_atomic_no_tmp_left(qdir):
    write_message(SID, 1, "x", queue_dir=qdir)
    mdir = mailbox_dir(SID, queue_dir=qdir)
    # only the final file exists; the .msg-* tmp was renamed away
    assert sorted(os.listdir(mdir)) == ["msg_1.txt"]


def test_item13c_stray_tmp_files_are_not_delivered(qdir):
    mdir = mailbox_dir(SID, queue_dir=qdir)
    os.makedirs(mdir)
    # a leftover tmp file from a crashed writer must be invisible
    Path(mdir, ".msg-stuck").write_text("partial")
    Path(mdir, "other.txt").write_text("not a message")
    assert next_message(SID, queue_dir=qdir) is None


def test_item13c_rewrite_same_id_is_idempotent(qdir):
    # the relay is at-least-once: rewriting msg_<id> replaces cleanly
    write_message(SID, 5, "first", queue_dir=qdir)
    write_message(SID, 5, "second", queue_dir=qdir)
    mdir = mailbox_dir(SID, queue_dir=qdir)
    assert _msg_files(mdir) == ["msg_5.txt"]
    assert next_message(SID, queue_dir=qdir)[1] == "second"


# --- WATCHER_LOOP template ---------------------------------------------------


def test_item13c_watcher_loop_template_renders(qdir):
    # the raw template carries the substitution placeholders
    for token in ("{mailbox_dir}", "{poll_seconds}", "{ttl_seconds}"):
        assert token in WATCHER_LOOP
    loop = watcher_loop_text("/mb/x", ttl_seconds=30, poll_seconds=2)
    assert 'mailbox_dir="/mb/x"' in loop
    assert "msg_*.txt" in loop
    assert "sleep 2" in loop
    assert "SECONDS + 30" in loop
    # level-triggered head-of-line consume: ls | head -1; cat; rm
    assert "head -n 1" in loop
    assert "cat " in loop and "rm -f " in loop
    assert "WATCHER_TTL_EXPIRED" in loop


def test_item13c_watcher_loop_defaults_from_contract(qdir):
    loop = watcher_loop_text("/mb/x")
    assert f"sleep {POLL_SECONDS}" in loop
    assert f"SECONDS + {WATCHER_TTL_SECONDS}" in loop


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash required")
def test_item13c_gwt1_watcher_loop_delivers_and_exits(qdir):
    """Run the rendered loop for real: message printed, consumed, exit."""
    write_message(SID, 1, "hello managed", queue_dir=qdir)
    loop = watcher_loop_text(mailbox_dir(SID, queue_dir=qdir), ttl_seconds=15, poll_seconds=1)
    res = subprocess.run(
        ["bash", "-c", loop], capture_output=True, text=True, timeout=20, check=False
    )
    assert res.returncode == 0
    assert "=== mailbox message msg_1.txt ===" in res.stdout
    assert "hello managed" in res.stdout
    # the delivery was consumed level-triggered (cat + rm)
    assert next_message(SID, queue_dir=qdir) is None


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash required")
def test_item13c_watcher_loop_ttl_expiry_marks_rotation(qdir):
    loop = watcher_loop_text(mailbox_dir(SID, queue_dir=qdir), ttl_seconds=1, poll_seconds=1)
    res = subprocess.run(
        ["bash", "-c", loop], capture_output=True, text=True, timeout=15, check=False
    )
    assert res.returncode == 0
    assert "WATCHER_TTL_EXPIRED" in res.stdout


# --- mailbox_duty_text ---------------------------------------------------------


def test_item13c_gwt2_duty_text_required_instructions(qdir):
    text = mailbox_duty_text(SID, queue_dir=qdir)
    # watcher spawn as a background subagent, mailbox dir substituted
    assert "background" in text
    assert mailbox_dir(SID, queue_dir=qdir) in text
    assert watcher_loop_text(mailbox_dir(SID, queue_dir=qdir)) in text
    # process deliveries by notification kind
    for kind in (
        "task_dispatch",
        "question_answer",
        "directive",
        "heartbeat_tick",
        "disarm",
    ):
        assert kind in text
    # re-arm before end_turn + TURN_TTL rotation instruction
    assert "re-arm" in text
    assert "before your turn ends" in text
    assert str(TURN_TTL_CYCLES) in text
    assert "SessionStart" in text
    # ack + disarm exit
    assert f"bridge ack {SID}" in text
    assert "disarm" in text


def test_item13c_duty_text_carries_session_id(qdir):
    text = mailbox_duty_text("s-xyz", queue_dir=qdir)
    assert "s-xyz" in text
