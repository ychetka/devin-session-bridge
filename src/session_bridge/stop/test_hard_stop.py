"""Tests for the layer-2 attributed orphan sweep (issue #164).

SAFETY INVARIANT: no test in this file ever enumerates or signals a
real process. ``plan_and_sweep``/``sweep`` receive a fabricated
``procs`` list and fake ``signaler``/``gone``/``sleep`` functions -
the pids are invented numbers inside a fake process table.
"""

import os
import signal
import tempfile

import pytest

from session_bridge.stop import hard_stop
from session_bridge.stop.hard_stop import (
    ProcInfo,
    SweepContext,
    classify,
    descendants,
    is_devin_acp,
    marker_hits,
    resolve_backend,
    sweep,
)

SID = "s1"
LOCK_PID = 100
BACKEND_PID = 101
BACKEND_START = 1000.0


def P(pid, ppid, argv, cwd=None, start=2000.0, env_sid=None, tty=0):
    return ProcInfo(
        pid=pid,
        ppid=ppid,
        argv=tuple(argv),
        cwd=cwd,
        start_seconds=start,
        devin_session_id=env_sid,
        tty_nr=tty,
    )


def _fake_tree():
    """The verified-live host shape: supervisor -> devin acp backend
    -> {MCP servers, session exec shells, watchers, foreign work}."""
    return [
        P(LOCK_PID, 1, ["/opt/windsurf-cli", "agent", "acp-host"], "/home/u", 900.0),
        P(BACKEND_PID, LOCK_PID, ["/opt/devin/bin/devin", "acp"], "/home/u", BACKEND_START),
        # long-lived MCP/tool servers, spawned with the backend
        P(102, BACKEND_PID, ["/usr/bin/python3", "/prj/mcp_partner.py"], "/home/u", 1001.0),
        P(103, BACKEND_PID, ["/home/u/.venv/bin/devin-telegram", "mcp"], "/home/u", 1002.0),
        # session-attributable work
        P(104, BACKEND_PID, ["bash", "-c", "make test"], "/repo/w1/build", 2000.0),
        P(105, BACKEND_PID, ["python3", "/brg/worker.py", "queue/s1"], "/home/u", 2000.0),
        P(106, 104, ["sleep", "60"], "/repo/w1", 2100.0),  # grandchild
        P(107, BACKEND_PID, ["bridge", "wait", "3", "--session", "s1"], "/home/u", 2000.0),
        # a different session's work + markers
        P(108, BACKEND_PID, ["bash", "-c", "build"], "/repo/w2", 2000.0),
        P(109, BACKEND_PID, ["bridge", "wait", "7", "--session", "s2"], "/home/u", 2000.0),
        # infra: cwd inside the workspace but spawned in the backend's
        # startup window -> not session work
        P(110, BACKEND_PID, ["/usr/bin/helper", "--serve"], "/repo/w1", 1005.0),
        # strong marker inside the startup window -> still session work
        P(111, BACKEND_PID, ["python3", "x.py", "queue/s1"], "/repo/w1", 1003.0),
    ]


def _ctx(**kw):
    args = {
        "sid": SID,
        "session_cwd": "/repo/w1",
        "mailbox_dir": "/state/queue/s1",
        "protected_pids": frozenset({LOCK_PID, BACKEND_PID}),
        "backend_pids": frozenset({BACKEND_PID}),
        "backend_start": BACKEND_START,
        "foreign_sessions": (("s2", "/repo/w2"),),
    }
    args.update(kw)
    return SweepContext(**args)


def _start_of(procs, overrides=None):
    """Fake /proc starttime reader for the PID-reuse guard: returns
    the enumeration-time start of each fabricated process (a pid not
    in the table reads as gone). ``overrides`` simulates a recycled
    pid - a DIFFERENT start read back at signal time."""
    starts = {p.pid: p.start_seconds for p in procs}
    if overrides:
        starts.update(overrides)
    return lambda pid: starts.get(pid)


# --- backend resolution --------------------------------------------------------


def test_issue164_gwt1_resolve_backend_finds_devin_acp_child():
    """[ISSUE-164.GWT-1] the lockfile pid is the acp-host supervisor;
    the backend is its direct child whose cmdline is 'devin acp'."""
    procs = _fake_tree()
    backends = resolve_backend(LOCK_PID, procs)
    assert [b.pid for b in backends] == [BACKEND_PID]


def test_issue164_gwt1b_resolve_backend_fallback_lock_pid_is_acp():
    """Installs without the supervisor split: the lockfile pid itself
    IS devin acp."""
    procs = [P(50, 1, ["/opt/bin/devin", "acp"], None, 100.0)]
    assert [b.pid for b in resolve_backend(50, procs)] == [50]


def test_issue164_gwt1c_resolve_backend_no_match():
    procs = _fake_tree()
    assert resolve_backend(999, procs) == []


def test_issue164_gwt2_is_devin_acp_cmdline_shapes():
    """[ISSUE-164.GWT-2] argv pair match plus wrapper/path shapes; a
    plain 'devin rm' or unrelated binary is not the backend."""
    assert is_devin_acp(P(1, 0, ["/opt/bin/devin", "acp"], None, 0))
    assert is_devin_acp(P(1, 0, ["devin", "acp"], None, 0))
    assert is_devin_acp(P(1, 0, ["/usr/bin/env", "devin", "acp", "--flag"], None, 0))
    assert not is_devin_acp(P(1, 0, ["devin", "rm", "s1"], None, 0))
    assert not is_devin_acp(P(1, 0, ["/usr/bin/bash", "-c", "echo devin acpfoo"], None, 0))


def test_issue164_gwt3_descendants_recursive():
    """[ISSUE-164.GWT-3] the sweep enumerates the whole backend tree -
    grandchildren included; the supervisor is not a descendant."""
    tree = {p.pid for p in descendants({BACKEND_PID}, _fake_tree())}
    assert tree == {102, 103, 104, 105, 106, 107, 108, 109, 110, 111}


# --- attribution predicate table ------------------------------------------------


@pytest.mark.parametrize(
    ("pid", "verdict", "detail_prefix"),
    [
        (102, "excluded", "mcp_server"),
        (103, "excluded", "mcp_server"),
        (104, "candidate", "cwd_under:"),
        (105, "candidate", "cmdline:queue/s1"),
        (106, "candidate", "cwd_under:"),
        (107, "candidate", "cmdline:--session s1"),
        (108, "excluded", "other_session:s2"),
        (109, "excluded", "other_session:s2"),
        (110, "excluded", "infra_spawn_window"),
        (111, "candidate", "cmdline:queue/s1"),
    ],
)
def test_issue164_gwt4_attribution_table(pid, verdict, detail_prefix):
    """[ISSUE-164.GWT-4] the predicate table: cwd match, marker match,
    MCP whitelist, foreign-session and spawn-window exclusions, and
    strong markers beating the spawn window."""
    procs = {p.pid: p for p in _fake_tree()}
    got, detail = classify(procs[pid], _ctx())
    assert got == verdict
    assert detail.startswith(detail_prefix)


def test_issue164_gwt4b_protected_pids_never_candidates():
    """The backend and the acp-host supervisor are excluded even when
    they somehow carry a marker - a fleet kill dressed as a session
    kill is worse than no kill."""
    ctx = _ctx()
    backend = P(BACKEND_PID, LOCK_PID, ["/opt/bin/devin", "acp", "queue/s1"], "/repo/w1", 2000.0)
    assert classify(backend, ctx) == ("excluded", "protected_infrastructure")
    host = P(LOCK_PID, 1, ["supervisor"], "/repo/w1", 2000.0)
    assert classify(host, ctx) == ("excluded", "protected_infrastructure")


def test_issue164_gwt5_marker_boundaries():
    """[ISSUE-164.GWT-5] marker matching is segment/token bounded:
    'queue/s12' and '--session s12' do NOT attribute to 's1', while
    'queue/s1/...' and '--session=s1' do."""
    ctx = _ctx()
    assert classify(P(1, 0, ["x", "queue/s12"], None, 2000.0), ctx) == ("unrelated", "")
    assert classify(P(1, 0, ["bridge", "wait", "--session", "s12"], None, 2000.0), ctx) == (
        "unrelated",
        "",
    )
    assert marker_hits(
        P(1, 0, ["w", "/state/queue/s1/msg_9.txt"], None, 2000.0), SID, "/state/queue/s1"
    )
    assert marker_hits(P(1, 0, ["w", "--session=s1"], None, 2000.0), SID, None)


def test_issue164_gwt5b_foreign_cwd_excludes_nested_attribution():
    """A process inside BOTH workspaces (nesting) is claimed by the
    foreign session - ambiguous attribution always loses."""
    ctx = _ctx(foreign_sessions=(("s2", "/repo/w1/sub"),))
    proc = P(1, 0, ["bash", "-c", "x"], "/repo/w1/sub/deep", 2000.0)
    verdict, detail = classify(proc, ctx)
    assert verdict == "excluded" and detail == "other_session:s2"


def test_issue164_gwt6_trivial_workspace_no_cwd_attribution():
    """[ISSUE-164.GWT-6] a session registered at '/' (or home) gets no
    cwd attribution - markers still work."""
    ctx = _ctx(session_cwd="/")
    proc = P(1, 0, ["bash", "-c", "x"], "/anything", 2000.0)
    assert classify(proc, ctx) == ("unrelated", "")


# --- architect review r1 (issue #179) ------------------------------------------


def test_issue179_gwt1_env_marker_outranks_mcp_token():
    """[ISSUE-179.R1-1] an explicit DEVIN_SESSION_ID env marker is
    unambiguous ownership - it must outrank the coincidental 'mcp'
    cmdline substring (a session running MCP tests is still session
    work, review r1 finding 9)."""
    ctx = _ctx()
    proc = P(1, 0, ["python3", "tests/test_mcp_server.py"], "/repo/w1", 2000.0, env_sid=SID)
    assert classify(proc, ctx) == ("candidate", "env:DEVIN_SESSION_ID")


def test_issue179_gwt2_shared_user_daemon_excluded_even_when_marked():
    """[ISSUE-179.R1-2] shared host-level user daemons inherit the
    spawning session's env marker but belong to the HOST, not the
    session - killing them inside a session sweep breaks the fleet's
    git/ssh stack (review r1 finding 11)."""
    ctx = _ctx()
    for argv in (
        ["/usr/bin/gpg-agent", "--daemon"],
        ["/usr/bin/ssh-agent", "-s"],
        ["/usr/bin/dbus-launch", "--exit-with-session"],
        ["git", "fsmonitor--daemon", "start"],
        # wrapped forms: the token hides inside one argv element or
        # under the hyphenated binary name (review r2)
        ["sh", "-c", "git fsmonitor--daemon start"],
        ["/usr/lib/git-core/git-fsmonitor--daemon"],
    ):
        proc = P(1, 0, argv, "/repo/w1", 2000.0, env_sid=SID)
        assert classify(proc, ctx) == ("excluded", "shared_user_daemon"), argv


def test_issue179_gwt3_interactive_tty_excluded_on_cwd_leg():
    """[ISSUE-179.R1-3] a human's interactive shell (controlling
    terminal) sitting in the shared workspace survives the weak cwd
    leg - while a marker-attributed process is still swept (review r1
    finding 10)."""
    ctx = _ctx()
    assert classify(P(1, 0, ["bash"], "/repo/w1", 2000.0, tty=1024), ctx) == (
        "excluded",
        "interactive_tty",
    )
    # strong attribution still beats the tty exclusion: a marked
    # session process holding a pty remains a sweep candidate
    marked = P(1, 0, ["bridge", "wait", "--session", "s1"], "/repo/w1", 2000.0, tty=1024)
    verdict, detail = classify(marked, ctx)
    assert verdict == "candidate" and detail.startswith("cmdline:")


# --- sweep: dry-run vs kill ordering ---------------------------------------------


def test_issue164_gwt7_dry_run_enumerates_never_signals():
    """[ISSUE-164.GWT-7] --dry-run returns every would-be victim with
    its attribution reason, plus the exclusion list - and signals
    nothing."""
    ctx = _ctx()
    calls = []
    procs = _fake_tree()
    res = sweep(
        ctx,
        procs,
        signaler=lambda pid, sig: calls.append((pid, sig)),
        start_of=_start_of(procs),
        dry_run=True,
    )
    assert calls == []
    assert res["status"] == "dry_run"
    assert {c["pid"] for c in res["candidates"]} == {104, 105, 106, 107, 111}
    reasons = {c["pid"]: c["reason"] for c in res["candidates"]}
    assert reasons[104].startswith("cwd_under:")
    assert reasons[105] == "cmdline:queue/s1"
    assert reasons[107] == "cmdline:--session s1"
    excluded = {s["pid"]: s["exclusion"] for s in res["skipped"]}
    assert excluded[102] == "mcp_server" and excluded[103] == "mcp_server"
    assert excluded[108].startswith("other_session:")
    assert excluded[110] == "infra_spawn_window"


def test_issue164_gwt8_term_then_kill_ordering():
    """[ISSUE-164.GWT-8] every candidate gets SIGTERM first (deepest
    tree members before their parents); only survivors get SIGKILL
    after the grace."""
    ctx = _ctx()
    calls = []
    procs = _fake_tree()
    alive = {105, 106}  # 106's parent 104 dies on TERM; 106 + 105 survive
    res = sweep(
        ctx,
        procs,
        signaler=lambda pid, sig: calls.append((pid, sig)),
        gone=lambda pid: pid not in alive,
        start_of=_start_of(procs),
        sleep=lambda s: calls.append(("sleep", s)),
        grace_seconds=2.0,
    )
    terms = [c for c in calls if c[0] != "sleep" and c[1] == signal.SIGTERM]
    kills = [c for c in calls if c[0] != "sleep" and c[1] == signal.SIGKILL]
    term_pids = [pid for pid, _ in terms]
    # all candidates terminated before the grace sleep, then kills
    assert set(term_pids) == {104, 105, 106, 107, 111}
    assert ("sleep", 2.0) in calls
    sleep_idx = calls.index(("sleep", 2.0))
    assert all(calls.index(t) < sleep_idx for t in terms)
    # only the survivors were killed
    assert {pid for pid, _ in kills} == {105, 106}
    # grandchildren signal before their parents
    assert term_pids.index(106) < term_pids.index(104)
    by_pid = {e["pid"]: e for e in res["swept"]}
    assert by_pid[104]["signal"] == "SIGTERM" and by_pid[104]["outcome"] == "gone"
    assert by_pid[106]["signal"] == "SIGKILL" and by_pid[106]["outcome"] == "still_alive"
    assert by_pid[105]["signal"] == "SIGKILL" and by_pid[105]["outcome"] == "still_alive"


def test_issue164_gwt9_plan_and_sweep_full_report():
    """[ISSUE-164.GWT-9] the top-level plan reports the resolved
    backend + acp-host pids and runs the sweep over the injected
    tree - foreign sessions still excluded."""
    calls = []
    procs = _fake_tree()
    res = hard_stop.plan_and_sweep(
        lock_pid=LOCK_PID,
        sid=SID,
        session_cwd="/repo/w1",
        mailbox_dir="/state/queue/s1",
        registered=[("s1", "/repo/w1"), ("s2", "/repo/w2")],
        procs=procs,
        signaler=lambda pid, sig: calls.append((pid, sig)),
        gone=lambda pid: True,
        start_of=_start_of(procs),
        sleep=lambda s: None,
    )
    assert res["status"] == "ok"
    assert res["backend_pid"] == BACKEND_PID
    assert res["acp_host_pid"] == LOCK_PID
    assert LOCK_PID not in [c["pid"] for c in res["candidates"]]
    assert BACKEND_PID not in [c["pid"] for c in res["candidates"]]
    assert {e["pid"] for e in res["swept"]} == {104, 105, 106, 107, 111}
    assert not any(pid in (LOCK_PID, BACKEND_PID, 102, 103, 108, 109, 110) for pid, _ in calls)


def test_issue164_gwt10_no_backend_is_honest_not_a_guess():
    """[ISSUE-164.GWT-10] when no 'devin acp' child resolves, the
    report is status=no_backend - never a sweep against a guessed
    pid."""
    procs = [p for p in _fake_tree() if p.pid != BACKEND_PID]
    calls = []
    res = hard_stop.plan_and_sweep(
        lock_pid=LOCK_PID,
        sid=SID,
        session_cwd="/repo/w1",
        mailbox_dir=None,
        registered=[("s1", "/repo/w1")],
        procs=procs,
        signaler=lambda pid, sig: calls.append((pid, sig)),
        start_of=_start_of(procs),
    )
    assert res["status"] == "no_backend"
    assert res["acp_host_pid"] == LOCK_PID
    assert calls == []


def test_issue164_gwt11_term_error_recorded_not_raised():
    """[ISSUE-164.GWT-11] a signal failure (pid already gone) is
    recorded on the swept entry, not raised past the report."""

    def flaky(pid, sig):
        if pid == 105:
            raise ProcessLookupError("gone")

    procs = _fake_tree()
    res = hard_stop.plan_and_sweep(
        lock_pid=LOCK_PID,
        sid=SID,
        session_cwd="/repo/w1",
        mailbox_dir=None,
        registered=[("s1", "/repo/w1")],
        procs=procs,
        signaler=flaky,
        gone=lambda pid: True,
        start_of=_start_of(procs),
        sleep=lambda s: None,
    )
    by_pid = {e["pid"]: e for e in res["swept"]}
    assert by_pid[105]["outcome"] == "term_error:ProcessLookupError"
    assert by_pid[104]["outcome"] == "gone"


def test_issue164_gwt12_real_lister_reads_proc():
    """[ISSUE-164.GWT-12] the stdlib enumerator parses a fake /proc
    tree (stat ppid/starttime, NUL cmdline, cwd readlink) - the real
    lister against a fabricated root, never the host table."""
    root = tempfile.mkdtemp()
    for pid, ppid, argv, start in (
        (10, 1, b"/opt/bin/devin\0acp\0", 500),
        (11, 10, b"bash\0-c\0work\0", 2000),
    ):
        d = os.path.join(root, str(pid))
        os.makedirs(d)
        with open(os.path.join(d, "stat"), "wb") as fh:
            fh.write(f"{pid} (comm) S {ppid} 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 {start}".encode())
        with open(os.path.join(d, "cmdline"), "wb") as fh:
            fh.write(argv)
        os.symlink("/repo/w1", os.path.join(d, "cwd"))
    procs = hard_stop.list_processes(root)
    by_pid = {p.pid: p for p in procs}
    assert by_pid[10].ppid == 1 and is_devin_acp(by_pid[10])
    assert by_pid[11].argv == ("bash", "-c", "work")
    assert by_pid[11].cwd == "/repo/w1"
    assert by_pid[11].start_seconds > by_pid[10].start_seconds


# --- review fixes: pid-reuse guard, orphan pass, foreign-cwd guard --------------


def test_issue164_gwt13_pid_reused_skips_sigterm():
    """[ISSUE-164.GWT-13] PID-reuse race (review WARNING-1): a victim
    dying mid-enumeration may yield its pid to a foreign process -
    before SIGTERM the starttime is re-read and a mismatch skips the
    pid entirely, recorded as pid_reused:skipped."""
    ctx = _ctx()
    procs = _fake_tree()
    calls = []
    # pid 105 was recycled: its /proc starttime no longer matches the
    # enumeration-time capture
    res = sweep(
        ctx,
        procs,
        signaler=lambda pid, sig: calls.append((pid, sig)),
        gone=lambda pid: True,
        start_of=_start_of(procs, {105: 9999.0}),
        sleep=lambda s: None,
    )
    signalled = {pid for pid, _ in calls}
    assert 105 not in signalled
    by_pid = {e["pid"]: e for e in res["swept"]}
    assert by_pid[105]["outcome"] == "pid_reused:skipped"
    # the signal field records delivery, not intent: nothing was sent
    assert "signal" not in by_pid[105]
    assert by_pid[104]["signal"] == "SIGTERM" and by_pid[104]["outcome"] == "gone"


def test_issue164_gwt14_pid_reused_skips_sigkill():
    """[ISSUE-164.GWT-14] the identity check also guards the SIGKILL
    follow-up: a victim that dies inside the grace and gets recycled
    is skipped - the recycled pid never eats the kill."""
    ctx = _ctx()
    procs = _fake_tree()
    calls = []
    # 105 survives the TERM (not gone) but reads back a different
    # starttime at KILL time - a recycled pid
    res = sweep(
        ctx,
        procs,
        signaler=lambda pid, sig: calls.append((pid, sig)),
        gone=lambda pid: pid != 105,
        start_of=_start_of(procs, {105: 8888.0}),
        sleep=lambda s: None,
        grace_seconds=1.0,
    )
    terms = [pid for pid, sig in calls if sig == signal.SIGTERM]
    kills = [pid for pid, sig in calls if sig == signal.SIGKILL]
    # wait: TERM also checks identity first - 105 is skipped there too
    assert 105 not in terms and 105 not in kills
    by_pid = {e["pid"]: e for e in res["swept"]}
    assert by_pid[105]["outcome"] == "pid_reused:skipped"


def test_issue164_gwt14b_recycled_during_grace_skips_sigkill():
    """The mid-grace variant: pid is identical at TERM (so the TERM
    went out), then recycled before the KILL leg - identity is
    re-read per signal, so the recycled pid is skipped there too."""
    ctx = _ctx()
    procs = _fake_tree()
    calls = []
    starts = {p.pid: p.start_seconds for p in procs}
    recycled = {"armed": False}

    def start_of(pid):
        if pid == 105 and recycled["armed"]:
            return 7777.0
        return starts.get(pid)

    def signaler(pid, sig):
        calls.append((pid, sig))
        if pid == 105 and sig == signal.SIGTERM:
            recycled["armed"] = True  # died on TERM, pid recycled

    res = sweep(
        ctx,
        procs,
        signaler=signaler,
        gone=lambda pid: False,  # the pid exists - but it is not our victim
        start_of=start_of,
        sleep=lambda s: None,
        grace_seconds=1.0,
    )
    kills = [pid for pid, sig in calls if sig == signal.SIGKILL]
    assert 105 not in kills
    by_pid = {e["pid"]: e for e in res["swept"]}
    assert by_pid[105]["outcome"] == "pid_reused:skipped"


def test_issue164_gwt15_foreign_root_cwd_no_blanket_exclusion():
    """[ISSUE-164.GWT-15] review MINOR: a foreign session registered
    at '/' must not nullify every cwd-attributed candidate - the cwd
    leg of the foreign check carries the same _cwd_usable guard as
    the target leg."""
    ctx = _ctx(foreign_sessions=(("sX", "/"),))
    proc = P(200, BACKEND_PID, ["bash", "-c", "x"], "/repo/w1/sub", 2000.0)
    verdict, detail = classify(proc, ctx)
    assert verdict == "candidate" and detail == "cwd_under:/repo/w1"
    # marker legs stay unconditional even at a trivial foreign cwd
    marked = P(201, BACKEND_PID, ["bridge", "wait", "--session", "sX"], "/repo/w1", 2000.0)
    assert classify(marked, ctx) == ("excluded", "other_session:sX")


# --- issue #168: DEVIN_SESSION_ID env-marker attribution -------------------------


def test_issue168_gwt1_env_marker_strong_attribution():
    """[ISSUE-168.GWT-1] a process carrying DEVIN_SESSION_ID=<sid> in
    its /proc environ is a strong candidate - reason
    env:DEVIN_SESSION_ID - even with no cmdline marker and a neutral
    cwd, and even inside the backend spawn window."""
    ctx = _ctx()
    # plain argv, foreign-looking cwd, spawned inside the infra window -
    # nothing but the env marker ties it to s1
    proc = P(200, BACKEND_PID, ["bash", "-c", "x"], "/home/u", 1003.0, env_sid=SID)
    verdict, detail = classify(proc, ctx)
    assert verdict == "candidate"
    assert detail == "env:DEVIN_SESSION_ID"


def test_issue168_gwt2_foreign_env_is_exclusion():
    """[ISSUE-168.GWT-2] env naming a DIFFERENT registered session is a
    foreign exclusion exactly like a foreign cmdline marker - even
    when the process also sits under our workspace cwd."""
    ctx = _ctx()
    proc = P(200, BACKEND_PID, ["bash", "-c", "x"], "/repo/w1/sub", 2000.0, env_sid="s2")
    assert classify(proc, ctx) == ("excluded", "other_session:s2")


def test_issue168_gwt3_markerless_falls_back_to_heuristics():
    """[ISSUE-168.GWT-3] no env marker -> the existing cwd/marker
    heuristics are unchanged: cwd attribution, spawn-window exclusion
    and unrelated verdicts behave exactly as before."""
    ctx = _ctx()
    assert classify(P(200, BACKEND_PID, ["bash", "-c", "x"], "/repo/w1", 2000.0), ctx) == (
        "candidate",
        "cwd_under:/repo/w1",
    )
    assert classify(P(201, BACKEND_PID, ["/usr/bin/h", "--serve"], "/repo/w1", 1005.0), ctx) == (
        "excluded",
        "infra_spawn_window",
    )
    assert classify(P(202, BACKEND_PID, ["bash", "-c", "x"], "/other", 2000.0), ctx) == (
        "unrelated",
        "",
    )


def test_issue168_gwt4_env_plus_foreign_marker_resolves_to_exclusion():
    """[ISSUE-168.GWT-4] env naming OUR sid but cmdline naming a
    foreign session: exclusions run before attribution, so the
    conflict resolves to exclusion - a misattributed kill is a second
    incident, never a candidate."""
    ctx = _ctx()
    proc = P(
        200,
        BACKEND_PID,
        ["bridge", "wait", "--session", "s2"],
        "/home/u",
        2000.0,
        env_sid=SID,
    )
    assert classify(proc, ctx) == ("excluded", "other_session:s2")


def test_issue168_gwt5_unregistered_env_value_is_not_attribution():
    """An env marker naming a sid the bridge does not know (not the
    target, not a foreign-registered session) is dead text: the
    heuristics decide."""
    ctx = _ctx()
    proc = P(200, BACKEND_PID, ["bash", "-c", "x"], "/other", 2000.0, env_sid="s9")
    assert classify(proc, ctx) == ("unrelated", "")
    # ... and under our workspace cwd it is just a cwd candidate
    proc2 = P(201, BACKEND_PID, ["bash", "-c", "x"], "/repo/w1", 2000.0, env_sid="s9")
    assert classify(proc2, ctx) == ("candidate", "cwd_under:/repo/w1")


def test_issue168_gwt6_env_marker_orphan_second_pass():
    """[ISSUE-168.GWT-6] pass 2 finds an env-marked orphan reparented
    outside the backend tree - the env marker is a STRONG marker for
    the orphan pass and records orphan_env:DEVIN_SESSION_ID; an
    env-marked orphan claimed by a foreign marker still loses."""
    ctx = _ctx()
    procs = [
        *_fake_tree(),
        # env-marked orphan, no cmdline marker at all
        P(300, 1, ["bash", "-c", "watcher"], "/home/u", 2000.0, env_sid=SID),
        # env-marked orphan BUT carrying a foreign cmdline marker
        P(
            301,
            1,
            ["bridge", "wait", "--session", "s2"],
            "/home/u",
            2000.0,
            env_sid=SID,
        ),
        # foreign env only: silently skipped like foreign cmdline markers
        P(302, 1, ["bash", "-c", "x"], "/home/u", 2000.0, env_sid="s2"),
        # no markers at all: pass 2 must not cwd-attribute it
        P(303, 1, ["bash", "-c", "x"], "/repo/w1", 2000.0),
    ]
    calls = []
    res = sweep(
        ctx,
        procs,
        signaler=lambda pid, sig: calls.append((pid, sig)),
        gone=lambda pid: True,
        start_of=_start_of(procs),
        sleep=lambda s: None,
    )
    by_pid = {c["pid"]: c["reason"] for c in res["candidates"]}
    assert by_pid[300] == "orphan_env:DEVIN_SESSION_ID"
    assert 303 not in by_pid  # env-less orphan: heuristics do not reach pass 2
    assert 302 not in by_pid  # foreign-env orphan is never our candidate
    excluded = {s["pid"]: s["exclusion"] for s in res["skipped"]}
    assert excluded[301] == "other_session:s2"


def test_issue168_gwt7_env_candidate_recorded_and_swept():
    """[ISSUE-168.GWT-7] end-to-end through sweep(): an env-marked
    descendant is recorded with the env reason in candidates and
    receives the SIGTERM volley like any marker hit."""
    ctx = _ctx()
    procs = [
        *_fake_tree(),
        P(200, BACKEND_PID, ["bash", "-c", "x"], "/tmp", 2000.0, env_sid=SID),
    ]
    calls = []
    res = sweep(
        ctx,
        procs,
        signaler=lambda pid, sig: calls.append((pid, sig)),
        gone=lambda pid: True,
        start_of=_start_of(procs),
        sleep=lambda s: None,
    )
    reasons = {c["pid"]: c["reason"] for c in res["candidates"]}
    assert reasons[200] == "env:DEVIN_SESSION_ID"
    assert (200, signal.SIGTERM) in calls
    by_pid = {e["pid"]: e for e in res["swept"]}
    assert by_pid[200]["signal"] == "SIGTERM" and by_pid[200]["outcome"] == "gone"


def test_issue168_gwt8_read_proc_parses_environ():
    """[ISSUE-168.GWT-8] the real lister reads DEVIN_SESSION_ID from a
    fabricated /proc/<pid>/environ (NUL-separated K=V); a missing
    environ file degrades to None, never an error."""
    root = tempfile.mkdtemp()
    d = os.path.join(root, "10")
    os.makedirs(d)
    with open(os.path.join(d, "stat"), "wb") as fh:
        fh.write(b"10 (comm) S 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 500")
    with open(os.path.join(d, "cmdline"), "wb") as fh:
        fh.write(b"bash\0-c\0work\0")
    os.symlink("/repo/w1", os.path.join(d, "cwd"))
    with open(os.path.join(d, "environ"), "wb") as fh:
        fh.write(b"PATH=/bin\0DEVIN_SESSION_ID=s1\0HOME=/u\0")
    procs = hard_stop.list_processes(root)
    assert procs[0].devin_session_id == "s1"
    # a pid dir without an environ file parses with marker None
    d2 = os.path.join(root, "11")
    os.makedirs(d2)
    with open(os.path.join(d2, "stat"), "wb") as fh:
        fh.write(b"11 (comm) S 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 600")
    with open(os.path.join(d2, "cmdline"), "wb") as fh:
        fh.write(b"bash\0")
    procs = {p.pid: p for p in hard_stop.list_processes(root)}
    assert procs[11].devin_session_id is None


def test_issue164_gwt16_orphan_marker_second_pass():
    """[ISSUE-164.GWT-16] review WARNING-2: a setsid/double-forked
    victim reparents to init and is invisible to the descendant walk
    - pass 2 finds it by STRONG sid markers only (cwd deliberately
    NOT used), records orphan_marker:<marker>, and signals it."""
    ctx = _ctx()
    procs = [
        *_fake_tree(),
        P(300, 1, ["python3", "/brg/worker.py", "queue/s1"], "/repo/w1", 2000.0),
        P(301, 1, ["bridge", "wait", "9", "--session", "s1"], "/home/u", 2000.0),
        # an orphan with ONLY the workspace cwd is NOT attributed in
        # pass 2 - strong markers are the contract
        P(302, 1, ["bash", "-c", "x"], "/repo/w1", 2000.0),
        # an orphan carrying OUR marker AND a foreign marker is
        # claimed by the foreign session - ambiguous attribution
        # always loses
        P(303, 1, ["python3", "w.py", "queue/s1", "--session", "s2"], "/repo/w2", 2000.0),
    ]
    calls = []
    res = sweep(
        ctx,
        procs,
        signaler=lambda pid, sig: calls.append((pid, sig)),
        gone=lambda pid: True,
        start_of=_start_of(procs),
        sleep=lambda s: None,
    )
    by_pid = {c["pid"]: c["reason"] for c in res["candidates"]}
    assert by_pid[300] == "orphan_marker:queue/s1"
    assert by_pid[301] == "orphan_marker:--session s1"
    assert 302 not in by_pid  # cwd alone never attributes an orphan
    excluded = {s["pid"]: s["exclusion"] for s in res["skipped"]}
    assert excluded[303] == "other_session:s2"
    assert {pid for pid, _ in calls} == {104, 105, 106, 107, 111, 300, 301}


def test_issue183_r2_tty_depth0_excluded_depth1_swept():
    """[ISSUE-183.R2-1] the interactive-tty exclusion applies only to
    DIRECT backend children (depth 0): a pty allocated by a deeper
    tool call - ``script``/``expect``/test-runner - is session work
    and must stay sweepable (architect review r2, #4)."""
    ctx = _ctx()
    direct = P(400, BACKEND_PID, ["bash"], "/repo/w1", 2000.0, tty=1)
    deep = P(401, 104, ["script", "-q", "out.txt"], "/repo/w1", 2000.0, tty=2)
    assert classify(direct, ctx, depth=0) == ("excluded", "interactive_tty")
    assert classify(deep, ctx, depth=1) == ("candidate", "cwd_under:/repo/w1")
    # unknown depth keeps the conservative exclusion (pre-r2 shape)
    assert classify(deep, ctx)[0] == "excluded"


def test_issue183_r2_deep_pty_swept_end_to_end():
    """A pty grandchild inside the backend tree is signalled; a
    direct-child interactive shell is not."""
    ctx = _ctx()
    procs = [
        *_fake_tree(),
        P(402, BACKEND_PID, ["bash"], "/repo/w1", 2000.0, tty=1),
        P(403, 104, ["expect", "run.exp"], "/repo/w1", 2000.0, tty=3),
    ]
    calls = []
    res = sweep(
        ctx,
        procs,
        signaler=lambda pid, sig: calls.append((pid, sig)),
        gone=lambda pid: True,
        start_of=_start_of(procs),
        sleep=lambda s: None,
    )
    signalled = {pid for pid, _ in calls}
    assert 403 in signalled and 402 not in signalled
    excluded = {s["pid"]: s["exclusion"] for s in res["skipped"]}
    assert excluded[402] == "interactive_tty"


def test_issue183_r2_bridge_daemon_protected():
    """[ISSUE-183.R2-4] the fleet-wide bridge daemon is excluded:
    a foreground ``bridge daemon run|once`` started from a session
    shell (inheriting its markers and cwd) must never be signalled
    (architect review r2, #3). Both argv forms are covered; a
    session command merely named like the daemon gets no free pass.
    """
    ctx = _ctx()
    for argv in (
        ["/home/u/.venv/bin/bridge", "daemon", "run"],
        ["session-bridge", "daemon", "once"],
        ["python3", "-m", "session_bridge", "daemon", "run"],
        ["python3", "-m", "session_bridge.cli.bridge", "daemon", "run"],
        # global flags may precede the subcommand (review round):
        ["bridge", "--db", "/x/state.db", "daemon", "run"],
        ["bridge", "--json", "--db=/x", "daemon", "once"],
    ):
        proc = P(410, BACKEND_PID, argv, "/repo/w1", 2000.0, env_sid=SID)
        assert classify(proc, ctx, depth=0) == ("excluded", "bridge_daemon"), argv
    # 'daemon' elsewhere in the argv does not buy protection: the
    # subcommand must be the first positional token
    for argv in (
        ["bridge", "status"],
        ["bridge", "--db", "daemon", "status"],  # 'daemon' is a flag VALUE
        ["bridge", "task", "daemon", "run"],  # decoy token after a subcommand
        ["python3", "-m", "session_bridge.cli.bridge", "status", "--json"],
    ):
        plain = P(411, BACKEND_PID, argv, "/repo/w1", 2000.0)
        assert classify(plain, ctx, depth=0) == ("candidate", "cwd_under:/repo/w1"), argv


def test_issue183_r2_weak_suspect_reported_not_swept():
    """[ISSUE-183.R2-5] pass 2 sweeps strong markers only, but a
    marker-less orphan attributable through the weak cwd leg is
    still REPORTED in weak_suspects - visible, never signalled
    (architect review r2, #9)."""
    ctx = _ctx()
    procs = [
        *_fake_tree(),
        # detached from the backend tree: no marker, cwd-only
        P(500, 1, ["bash", "-c", "x"], "/repo/w1", 2000.0),
        # a foreign-session cwd-only orphan is not even a suspect
        P(501, 1, ["bash", "-c", "y"], "/repo/w2", 2000.0),
    ]
    calls = []
    res = sweep(
        ctx,
        procs,
        signaler=lambda pid, sig: calls.append((pid, sig)),
        gone=lambda pid: True,
        start_of=_start_of(procs),
        sleep=lambda s: None,
    )
    assert [w["pid"] for w in res["weak_suspects"]] == [500]
    assert res["weak_suspects"][0]["reason"].startswith("weak_orphan:cwd_under:")
    signalled = {pid for pid, _ in calls}
    assert 500 not in signalled and 501 not in signalled
    assert 500 not in {c["pid"] for c in res["candidates"]}
