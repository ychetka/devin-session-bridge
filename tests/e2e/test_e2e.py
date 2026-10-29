"""End-to-end smoke tests for sessions-bridge (item_08).

Unlike the unit suites, these tests drive the real entry points as
subprocesses (``python -m session_bridge.cli.bridge``, the installed
hook command from ``install_hooks.sh``) against a real state.db in a
unique tmp dir per run (``SESSIONS_BRIDGE_DB`` +
``SESSIONS_BRIDGE_QUEUE_DIR``).

Headless coverage runs against an injected fake environment:

- a fake ``devin`` binary on PATH that wraps tests/fake_acp.py and
  logs every invocation;
- a redirected HOME carrying a fake credentials.toml, a fake devin
  ``cli/sessions.db`` and ``cli/session_locks/`` (all devin paths are
  resolved via ``os.path.expanduser`` at import time inside the
  subprocess, so HOME redirection is sufficient);
- a fake lock file + a live process whose /proc cmdline contains
  "devin" simulates a Desktop-held session.

The full bridge > state.db > _work > acp chain then runs with no
live Devin and no Desktop session.

GWT-1 exercises the real ``devin acp`` and is skipped when the binary
or credentials.toml is absent - detected from the environment, never
a hard failure. Every scratch session the tests create in real devin
is deleted in teardown via ``devin rm <session_id> --force`` (not
merely closed), and scratch repos/queue/tmp dirs are removed - the
finalizers run even when a test fails.
"""

import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest

from session_bridge.acp.client import (
    DEFAULT_CREDENTIALS_PATH,
    AcpClient,
)
from session_bridge.hooks.contract import PARKED_TASK_PREFIX
from session_bridge.state import db
from session_bridge.state.contract import (
    MAX_ATTEMPTS,
    TaskStatus,
    Transport,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = REPO_ROOT / "src"
FAKE_ACP = REPO_ROOT / "tests" / "fake_acp.py"
INSTALL_HOOKS = REPO_ROOT / "install_hooks.sh"

SID = "e2e-s1"


# --- helpers -----------------------------------------------------------------


def _wait_for_status(db_path, tid, statuses, timeout):
    """Poll the task row until it reaches one of ``statuses``."""
    deadline = time.monotonic() + timeout
    row = db.get_task(tid, path=db_path)
    while (
        row is not None
        and row["status"] not in statuses
        and time.monotonic() < deadline
    ):
        time.sleep(0.4)
        row = db.get_task(tid, path=db_path)
    return row


def _set_task_fields(db_path, tid, **fields):
    """Direct UPDATE for test fixture state (lease expiry, attempt)."""
    sets = ", ".join(f"{k}=?" for k in fields)
    con = db.connect(db_path)
    try:
        con.execute(
            f"UPDATE tasks SET {sets} WHERE id=?",
            (*fields.values(), tid),
        )
    finally:
        con.close()


def _ok_scenario(sid, reply="e2e reply body", prompt_sleep=0):
    """fake_acp scenario: load ok, prompt streams one chunk then
    end_turn, close ok."""
    prompt_actions = []
    if prompt_sleep:
        prompt_actions.append({"sleep": prompt_sleep})
    prompt_actions += [
        {
            "send": {
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": sid,
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": reply},
                    },
                },
            }
        },
        {
            "send": {
                "jsonrpc": "2.0",
                "id": "$id",
                "result": {
                    "stopReason": "end_turn",
                    "usage": {
                        "totalTokens": 3,
                        "inputTokens": 2,
                        "outputTokens": 1,
                    },
                },
            }
        },
    ]
    return {
        "rules": [
            {
                "method": "session/load",
                "actions": [
                    {
                        "send": {
                            "jsonrpc": "2.0",
                            "id": "$id",
                            "result": {},
                        }
                    }
                ],
            },
            {"method": "session/prompt", "actions": prompt_actions},
            {
                "method": "session/close",
                "actions": [
                    {
                        "send": {
                            "jsonrpc": "2.0",
                            "id": "$id",
                            "result": {},
                        }
                    }
                ],
            },
        ]
    }


def _stop_payload(sid, active=False):
    return {
        "hook_event_name": "Stop",
        "session_id": sid,
        "stop_hook_active": active,
        "last_assistant_message": "turn done",
    }


# --- fixtures -----------------------------------------------------------------


@pytest.fixture()
def scratch_repo(tmp_path):
    """A scratch repo dir (git-initialised); removed in teardown."""
    repo = tmp_path / "scratch-repo"
    repo.mkdir()
    subprocess.run(
        ["git", "init", "-q", str(repo)],
        capture_output=True,
        timeout=15,
        check=False,
    )
    yield repo
    shutil.rmtree(repo, ignore_errors=True)


@pytest.fixture()
def scratch_sessions():
    """Tracks live devin scratch sessions for teardown.

    Tests append ``(session_id, cwd)`` pairs right after creation;
    every entry is deleted via ``devin rm <sid> --force`` in teardown
    even when the test fails - closing alone does not remove a session.
    """
    sessions: list[tuple[str, str]] = []
    yield sessions
    for sid, cwd in sessions:
        try:
            res = subprocess.run(
                ["devin", "rm", sid, "--force"],
                cwd=cwd,
                capture_output=True,
                text=True,
                timeout=60,
            )
        except Exception as e:
            print(
                f"sessions-bridge e2e: devin rm {sid} raised {e!r}",
                file=sys.stderr,
            )
            continue
        if res.returncode != 0:
            print(
                f"sessions-bridge e2e: devin rm {sid} exited"
                f" {res.returncode}: {res.stderr.strip()}",
                file=sys.stderr,
            )


# --- headless world -----------------------------------------------------------


class _E2EWorld:
    """Headless bridge world: real subprocesses over a fake devin."""

    def __init__(self, tmp_path: Path):
        self.tmp = tmp_path
        self.db = str(tmp_path / "state.db")
        db.init(self.db)
        self.qdir = tmp_path / "queue"
        self.home = tmp_path / "home"
        devin_dir = self.home / ".local" / "share" / "devin"
        cli_dir = devin_dir / "cli"
        self.locks_dir = cli_dir / "session_locks"
        self.locks_dir.mkdir(parents=True)
        (devin_dir / "credentials.toml").write_text(
            'windsurf_api_key = "e2e-fake-key"\n'
        )
        # devin's own sessions.db, faked under the redirected HOME;
        # sessions_db_lookup only needs id + working_directory
        self.devin_sessions_db = cli_dir / "sessions.db"
        con = sqlite3.connect(self.devin_sessions_db)
        try:
            con.execute(
                "CREATE TABLE sessions (id TEXT PRIMARY KEY,"
                " working_directory TEXT, title TEXT)"
            )
            con.commit()
        finally:
            con.close()
        self.devin_log = tmp_path / "devin-invocations.log"
        self._scenario_n = 0
        self.procs: list[subprocess.Popen] = []
        self.bin = tmp_path / "bin"
        self.bin.mkdir()
        shim = self.bin / "devin"
        shim.write_text(
            "#!/usr/bin/env bash\n"
            f'echo "devin $*" >> "{self.devin_log}"\n'
            'if [ "$1" = "acp" ]; then\n'
            f'  exec "{sys.executable}" "{FAKE_ACP}"'
            ' "${FAKE_ACP_SCENARIO:?missing scenario}"\n'
            "fi\n"
            'echo "unexpected devin invocation: $*" >&2\n'
            "exit 42\n"
        )
        shim.chmod(0o755)

    def env(self, scenario=None) -> dict:
        env = dict(os.environ)
        env["HOME"] = str(self.home)
        env["PATH"] = f"{self.bin}{os.pathsep}{env['PATH']}"
        prev = env.get("PYTHONPATH")
        env["PYTHONPATH"] = (
            f"{SRC_DIR}{os.pathsep}{prev}" if prev else str(SRC_DIR)
        )
        env["SESSIONS_BRIDGE_DB"] = self.db
        env["SESSIONS_BRIDGE_QUEUE_DIR"] = str(self.qdir)
        env["SESSIONS_BRIDGE_WORK_TIMEOUT"] = "120"
        if scenario is not None:
            self._scenario_n += 1
            path = self.tmp / f"scenario_{self._scenario_n}.json"
            path.write_text(json.dumps(scenario))
            env["FAKE_ACP_SCENARIO"] = str(path)
        return env

    def bridge(
        self, *argv, scenario=None, timeout=30
    ) -> subprocess.CompletedProcess:
        return subprocess.run(
            [
                sys.executable,
                "-m",
                "session_bridge.cli.bridge",
                "--db",
                self.db,
                "--queue-dir",
                str(self.qdir),
                *argv,
            ],
            env=self.env(scenario),
            capture_output=True,
            text=True,
            timeout=timeout,
        )

    def hook(
        self, payload: dict, timeout=30
    ) -> subprocess.CompletedProcess:
        return subprocess.run(
            [
                sys.executable,
                "-m",
                "session_bridge.hooks.worker",
                "--db",
                self.db,
                "--queue-dir",
                str(self.qdir),
                "--hook-timeout",
                "60",
                "--cycle-seconds",
                "1",
                "--cycles-max",
                "2",
            ],
            env=self.env(),
            input=json.dumps(payload),
            capture_output=True,
            text=True,
            timeout=timeout,
        )

    def register_session(self, sid: str, cwd: str) -> None:
        """Make ``sid`` known to both devin's sessions.db (cwd for the
        acp load) and the bridge sessions table."""
        con = sqlite3.connect(self.devin_sessions_db)
        try:
            con.execute(
                "INSERT OR REPLACE INTO sessions"
                " (id, working_directory) VALUES (?, ?)",
                (sid, cwd),
            )
            con.commit()
        finally:
            con.close()
        db.upsert_session(sid, cwd=cwd, path=self.db)

    def lock_session(self, sid: str) -> None:
        """Simulate a Desktop-held session: a lock file naming a live
        PID whose /proc cmdline contains "devin"."""
        proc = subprocess.Popen(
            ["bash", "-c", "exec -a devin-e2e sleep 300"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.procs.append(proc)
        (self.locks_dir / f"{sid}.lock").write_text(str(proc.pid))

    def close(self) -> None:
        for proc in self.procs:
            try:
                proc.kill()
            except OSError:
                pass


@pytest.fixture()
def e2e(tmp_path):
    world = _E2EWorld(tmp_path)
    yield world
    world.close()


# --- GWT-1: live devin acp, push on a scratch session -> done -----------------


def _live_acp_skip_reason() -> str | None:
    missing = []
    if shutil.which("devin") is None:
        missing.append("devin binary is not on PATH")
    if not os.path.isfile(DEFAULT_CREDENTIALS_PATH):
        missing.append("devin credentials.toml is missing")
    return "; ".join(missing) or None


_LIVE_SKIP_REASON = _live_acp_skip_reason()


@pytest.mark.skipif(
    _LIVE_SKIP_REASON is not None,
    reason=_LIVE_SKIP_REASON or "live devin acp unavailable",
)
def test_item08_gwt1_live_acp_push_reaches_done(
    tmp_path, scratch_repo, scratch_sessions
):
    """Live acp: ``bridge push`` on a scratch session runs to done.

    The scratch session is registered for ``devin rm --force``
    teardown immediately after creation.
    """
    db_path = str(tmp_path / "state.db")
    db.init(db_path)
    client = AcpClient()  # real devin acp + real credentials.toml
    try:
        sid = client.new_session(cwd=str(scratch_repo))
        scratch_sessions.append((sid, str(scratch_repo)))
        # session/new alone does not persist the session into devin's
        # sessions.db - one real turn materializes the row so a second
        # acp process (the _work child) can session/load it
        seeded = client.prompt(
            sid,
            "Reply with exactly: BRIDGE-E2E-OK",
            timeout=180,
        )
        assert seeded.stop_reason == "end_turn", (
            f"scratch session seed prompt failed: {seeded.stop_reason}"
        )
        try:
            client.close_session(sid)
        except Exception:
            pass
    finally:
        client.terminate()
    # belt and suspenders for the dispatch check: the bridge sessions
    # row makes the session known even if devin's sessions.db lags
    db.upsert_session(sid, cwd=str(scratch_repo), path=db_path)
    env = dict(os.environ)
    prev = env.get("PYTHONPATH")
    env["PYTHONPATH"] = (
        f"{SRC_DIR}{os.pathsep}{prev}" if prev else str(SRC_DIR)
    )
    env["SESSIONS_BRIDGE_DB"] = db_path
    env["SESSIONS_BRIDGE_QUEUE_DIR"] = str(tmp_path / "queue")
    env["SESSIONS_BRIDGE_WORK_TIMEOUT"] = "240"
    res = subprocess.run(
        [
            sys.executable,
            "-m",
            "session_bridge.cli.bridge",
            "--db",
            db_path,
            "push",
            sid,
            "--task",
            "Reply with exactly: BRIDGE-E2E-OK",
            "--json",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert res.returncode == 0, res.stderr
    out = json.loads(res.stdout.strip())
    tid = out["data"]["task_id"]
    # the detached _work child loads, prompts and closes; poll the db
    row = _wait_for_status(
        db_path,
        tid,
        {TaskStatus.DONE.value, TaskStatus.FAILED.value},
        timeout=300,
    )
    assert row is not None
    assert row["status"] == TaskStatus.DONE.value, (
        f"task did not reach done: {dict(row)}"
    )
    assert row["last_assistant_message"]


# --- GWT-2: queued task picked up by the Stop hook (block chain) ---------------


def test_item08_gwt2_locked_session_task_picked_up_via_stop(
    e2e, scratch_repo
):
    """Hooks installed into a scratch repo by install_hooks.sh; a Stop
    event on a locked session claims the queued task (block chain)."""
    # install the real hook wiring into the scratch repo
    install_env = e2e.env()
    install_env["SESSIONS_BRIDGE_STOP_TIMEOUT"] = "60"
    install_env["SESSIONS_BRIDGE_CYCLE_SECONDS"] = "5"
    install_env["SESSIONS_BRIDGE_CYCLES_MAX"] = "2"
    res = subprocess.run(
        ["bash", str(INSTALL_HOOKS), str(scratch_repo)],
        env=install_env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert res.returncode == 0, res.stderr
    hooks_file = scratch_repo / ".devin" / "hooks.v1.json"
    hooks = json.loads(hooks_file.read_text())["hooks"]
    stop_commands = [
        h["command"]
        for entry in hooks.get("Stop", [])
        for h in entry.get("hooks", [])
        if "session_bridge/hooks/worker.py" in h.get("command", "")
    ]
    assert stop_commands, "Stop hook command was not installed"

    def stop_hook(payload: dict) -> subprocess.CompletedProcess:
        """Fire the installed Stop hook command like devin does: one
        JSON payload on stdin, optional JSON answer on stdout."""
        return subprocess.run(
            ["bash", "-c", stop_commands[0]],
            env=e2e.env(),
            input=json.dumps(payload),
            capture_output=True,
            text=True,
            timeout=30,
        )

    e2e.lock_session(SID)
    res = e2e.bridge("push", SID, "--task", "summarize the ledger",
                     "--json")
    assert res.returncode == 0, res.stderr
    out = json.loads(res.stdout.strip())
    tid = out["data"]["task_id"]
    row = db.get_task(tid, path=e2e.db)
    # dispatch chose the hook transport and woke the parked Stop hook
    assert row["status"] == TaskStatus.QUEUED.value
    assert row["transport"] == Transport.HOOK_QUEUE.value
    trigger = e2e.qdir / f"{SID}.json"
    assert json.loads(trigger.read_text())["session_id"] == SID
    # the Stop hook fires inside the session: the queued task is
    # claimed and handed to the agent as a block reason
    res = stop_hook(_stop_payload(SID))
    assert res.returncode == 0, res.stderr
    out = json.loads(res.stdout.strip())
    assert out["decision"] == "block"
    assert out["reason"].startswith(PARKED_TASK_PREFIX)
    assert "summarize the ledger" in out["reason"]
    row = db.get_task(tid, path=e2e.db)
    assert row["status"] == TaskStatus.RUNNING.value
    assert row["lease_owner"].startswith("hook-")
    # the block chain ends when the agent's turn stops again: the
    # hook-delivered task parks as awaiting_user
    res = stop_hook(_stop_payload(SID, active=True))
    assert res.returncode == 0, res.stderr
    assert res.stdout.strip() == ""
    row = db.get_task(tid, path=e2e.db)
    assert row["status"] == TaskStatus.AWAITING_USER.value
    assert row["lease_owner"] is None


# --- GWT-3: acp worker death mid-running -> pump recovery -----------------------


def test_item08_gwt3_killed_worker_recovered_and_redispatched(e2e):
    """kill -9 on a running _work; the next pump recovers the task and
    a replacement worker finishes it."""
    e2e.register_session(SID, cwd=str(e2e.tmp))
    # first prompt sleeps long enough to be killed mid-flight; the
    # replacement worker replays the same scenario to a clean end_turn
    scenario = _ok_scenario(SID, prompt_sleep=8)
    tid = db.enqueue(SID, "do the thing", path=e2e.db)
    assert tid is not None
    owner = "acp-dead-1"
    assert db.claim_queued(tid, owner, Transport.ACP_PUSH, path=e2e.db)
    assert db.mark_running(tid, owner, path=e2e.db)
    log = open(e2e.tmp / "worker1.log", "wb")
    try:
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "session_bridge.cli.bridge",
                "--db",
                e2e.db,
                "--queue-dir",
                str(e2e.qdir),
                "_work",
                str(tid),
                "--owner",
                owner,
                "--timeout",
                "60",
            ],
            env=e2e.env(scenario),
            stdout=log,
            stderr=log,
        )
        # handshake (~1.5s) + load + prompt in flight; the fake answer
        # cannot arrive before ~10s, so a kill here lands mid-running
        time.sleep(4)
        proc.kill()  # SIGKILL, like a dead acp worker
        proc.wait(timeout=10)
    finally:
        log.close()
    # a dead worker can no longer renew: simulate the expired lease
    # that lazy recovery keys on
    _set_task_fields(e2e.db, tid, lease_expires_at=int(time.time()) - 1)
    res = e2e.bridge("status", "--json", scenario=scenario)
    assert res.returncode == 0, res.stderr
    row = db.get_task(tid, path=e2e.db)
    # recovered: requeued (attempt 1 < MAX) and redispatched in the
    # same pump pass
    assert row["attempt"] == 1
    assert row["status"] in (
        TaskStatus.QUEUED.value,
        TaskStatus.RUNNING.value,
    )
    row = _wait_for_status(
        e2e.db,
        tid,
        {TaskStatus.DONE.value, TaskStatus.FAILED.value},
        timeout=45,
    )
    assert row["status"] == TaskStatus.DONE.value, (
        f"redispatched task did not finish: {dict(row)}"
    )
    assert row["last_assistant_message"] == "e2e reply body"


def test_item08_gwt3_stale_lease_fails_at_max_attempts(e2e):
    """Lease expiry on the last attempt lands on terminal failed."""
    e2e.register_session(SID, cwd=str(e2e.tmp))
    tid = db.enqueue(SID, "do the thing", path=e2e.db)
    assert tid is not None
    owner = "acp-dead-2"
    assert db.claim_queued(tid, owner, Transport.ACP_PUSH, path=e2e.db)
    assert db.mark_running(tid, owner, path=e2e.db)
    # dead worker on its last attempt: attempt+1 reaches MAX_ATTEMPTS
    _set_task_fields(
        e2e.db,
        tid,
        attempt=MAX_ATTEMPTS - 1,
        lease_expires_at=int(time.time()) - 1,
    )
    res = e2e.bridge("status", "--json")
    assert res.returncode == 0, res.stderr
    row = db.get_task(tid, path=e2e.db)
    assert row["status"] == TaskStatus.FAILED.value
    assert row["attempt"] == MAX_ATTEMPTS
    assert row["lease_owner"] is None
    out = json.loads(res.stdout.strip())
    assert any(
        e["kind"] == "FAILED" and e["task_id"] == tid
        for e in out["events"]
    )


# --- hardening: _work ownership re-check ----------------------------------------


def test_item08_hardening_stale_owner_exits_without_prompt(e2e):
    """A _work process whose --owner does not hold the lease exits
    without side effects - devin acp is never even spawned."""
    e2e.register_session(SID, cwd=str(e2e.tmp))
    tid = db.enqueue(SID, "do the thing", path=e2e.db)
    assert tid is not None
    owner = "acp-live-1"
    assert db.claim_queued(tid, owner, Transport.ACP_PUSH, path=e2e.db)
    assert db.mark_running(tid, owner, path=e2e.db)
    # no FAKE_ACP_SCENARIO on purpose: if the stale worker still tried
    # acp, the shim would fail loudly after logging the invocation
    res = e2e.bridge(
        "_work", str(tid), "--owner", "acp-stale-9", "--json"
    )
    assert res.returncode == 0, res.stderr
    out = json.loads(res.stdout.strip())
    assert out["status"] == TaskStatus.RUNNING.value
    assert "do not retry" in out["agent_instruction"]
    # no prompt, no side effects: devin was never spawned and the
    # live owner's row is untouched
    assert not e2e.devin_log.exists()
    row = db.get_task(tid, path=e2e.db)
    assert row["status"] == TaskStatus.RUNNING.value
    assert row["lease_owner"] == owner
    assert row["lease_expires_at"] > int(time.time())
    assert row["last_assistant_message"] is None
    # positive control: the real owner runs the same task to done
    res = e2e.bridge(
        "_work",
        str(tid),
        "--owner",
        owner,
        "--json",
        scenario=_ok_scenario(SID),
    )
    assert res.returncode == 0, res.stderr
    out = json.loads(res.stdout.strip())
    assert out["status"] == TaskStatus.DONE.value
    assert e2e.devin_log.exists()
    row = db.get_task(tid, path=e2e.db)
    assert row["status"] == TaskStatus.DONE.value
    assert row["last_assistant_message"] == "e2e reply body"
