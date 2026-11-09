"""Tests for the host ingress layer (issue #92).

Covers the spec parser, purpose embedding, the reconciler's
spawn/keep/stop semantics and an end-to-end TCP relay through a real
spawned forwarder. ns-* targets need a real child netns, so they are
exercised only at spec level here.
"""

import json
import os
import socket
import threading
import time

import pytest

from session_bridge.cli.contract import ExitCode
from session_bridge.ingress import spec as spec_mod
from session_bridge.ingress import sync as sync_mod
from session_bridge.ingress.spec import parse_fwd_spec, parse_purpose
from session_bridge.state import db


def test_parse_tcp():
    s = parse_fwd_spec("tcp:127.0.0.1:9000")
    assert (s.kind, s.arg, s.port, s.bind) == ("tcp", "127.0.0.1", 9000, "0.0.0.0")
    assert s.spec_text == "tcp:127.0.0.1:9000"


def test_parse_ns_pidfile():
    s = parse_fwd_spec("ns-pidfile:/run/x/inner.pid:5678", bind="127.0.0.1")
    assert (s.kind, s.arg, s.port, s.bind) == ("ns-pidfile", "/run/x/inner.pid", 5678, "127.0.0.1")


def test_parse_ns_pid():
    s = parse_fwd_spec("ns-pid:4242:80")
    assert s.arg == "4242" and s.port == 80


def test_parse_rejects_garbage():
    for bad in [
        "",
        "bogus",
        "bogus:1:2",
        "tcp:127.0.0.1",
        "tcp::80",
        "ns-pid:abc:80",
        "tcp:127.0.0.1:0",
        "tcp:127.0.0.1:70000",
        "ns-pidfile:relative.pid:80",
    ]:
        with pytest.raises(ValueError):
            parse_fwd_spec(bad)


def test_parse_purpose_tokens():
    s = parse_purpose("fwd=tcp:127.0.0.1:9000 bind=127.0.0.1 some human text")
    assert s is not None
    assert (s.kind, s.bind) == ("tcp", "127.0.0.1")
    assert parse_purpose("no declaration here") is None
    assert parse_purpose(None) is None


def test_format_purpose_roundtrip():
    p = spec_mod.format_purpose("tcp:127.0.0.1:9000", "127.0.0.1", "my purpose")
    s = parse_purpose(p)
    assert s.spec_text == "tcp:127.0.0.1:9000" and s.bind == "127.0.0.1"
    # default bind is not rendered
    p2 = spec_mod.format_purpose("tcp:127.0.0.1:9000", "0.0.0.0", "txt")
    assert "bind=" not in p2 and "fwd=" in p2 and "txt" in p2


def test_resolve_target(tmp_path):
    pf = tmp_path / "inner.pid"
    pf.write_text("123")
    s = parse_fwd_spec(f"ns-pidfile:{pf}:5678")
    assert spec_mod.resolve_target(s) == "ns:123:5678"
    pf.write_text("456")
    assert spec_mod.resolve_target(s) == "ns:456:5678"
    pf.unlink()
    assert spec_mod.resolve_target(s) == ""
    assert spec_mod.resolve_target(parse_fwd_spec("tcp:h:9")) == "tcp:h:9"


def _rows(*specs):
    """Fake allocation rows: (port, purpose) pairs."""
    return [
        {"resource_type": "tcp_port", "resource_id": str(p), "purpose": purp, "project_id": "t"}
        for p, purp in specs
    ]


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def test_desired_forwards_filters():
    d, errs = sync_mod.desired_forwards(
        [
            *_rows((80, "fwd=tcp:127.0.0.1:9 x"), (81, "plain row"), (82, None)),
            {"resource_type": "netns_id", "resource_id": "n", "purpose": "fwd=tcp:x:1"},
        ]
    )
    assert set(d) == {80} and errs == []


def test_desired_forwards_row_isolation():
    """A poisoned row degrades to a per-row error, not a crash of the
    whole reconcile (review r1: stray fwd= in free text must not
    DoS the timer for every other project)."""
    d, errs = sync_mod.desired_forwards(
        _rows(
            (80, "fwd=bogus-spec"),
            (81, "fwd=tcp:127.0.0.1:9 good"),
            (82, "note: fwd=with:bad"),
        )
    )
    assert set(d) == {81}
    assert {e["row"] for e in errs} == {"80", "82"}


def test_desired_forwards_kinds_filter():
    """SESSIONS_BRIDGE_INGRESS_KINDS limits which spec kinds a pass
    owns - the host timer (=tcp) must not even see ns-* rows."""
    rows = _rows((80, "fwd=tcp:127.0.0.1:9 x"), (81, "fwd=ns-pid:1:9 y"))
    d, _ = sync_mod.desired_forwards(rows, {"tcp"})
    assert set(d) == {80}
    d, _ = sync_mod.desired_forwards(rows, None)
    assert set(d) == {80, 81}


def test_kinds_filter_env(monkeypatch):
    monkeypatch.setenv("SESSIONS_BRIDGE_INGRESS_KINDS", "tcp, ns-pidfile")
    assert sync_mod._kinds_filter() == {"tcp", "ns-pidfile"}
    monkeypatch.delenv("SESSIONS_BRIDGE_INGRESS_KINDS")
    assert sync_mod._kinds_filter() is None
    monkeypatch.setenv("SESSIONS_BRIDGE_INGRESS_KINDS", "  ")
    assert sync_mod._kinds_filter() is None


def test_kinds_filter_env_validates(monkeypatch, capsys):
    """Unknown/typo'd kinds are dropped with a warning - degrading to
    the unfiltered path rather than silently no-op'ing the pass."""
    monkeypatch.setenv("SESSIONS_BRIDGE_INGRESS_KINDS", "TCP")
    assert sync_mod._kinds_filter() == {"tcp"}  # case-normalized
    monkeypatch.setenv("SESSIONS_BRIDGE_INGRESS_KINDS", "tcp,typo")
    assert sync_mod._kinds_filter() == {"tcp"}
    assert "typo" in capsys.readouterr().err
    monkeypatch.setenv("SESSIONS_BRIDGE_INGRESS_KINDS", "bogus")
    assert sync_mod._kinds_filter() is None  # nothing valid -> unfiltered


def test_sync_kinds_filter_leaves_foreign_tracked(tmp_path, monkeypatch):
    """A tracked forwarder whose kind is filtered out belongs to the
    other reconciler context - this pass must not kill it."""
    state_dir = str(tmp_path / "state")
    tcp_port = _free_port()
    rows = _rows((tcp_port, "fwd=tcp:127.0.0.1:1"), (5678, "fwd=ns-pid:999999999:9"))
    out = sync_mod.sync(state_dir, rows)
    assert out["started"] == [tcp_port]  # ns-pid:1 unresolvable -> error, not spawn
    # pretend a foreign context spawned+tracked the ns forwarder
    st = sync_mod._load_state(state_dir)
    st["forwards"]["5678"] = {
        "pid": 999999,
        "spec": "ns-pid:999999999:9",
        "bind": "0.0.0.0",
        "resolved": "1",
        "started_at": 1,
    }
    sync_mod._save_state(state_dir, st)
    monkeypatch.setenv("SESSIONS_BRIDGE_INGRESS_KINDS", "tcp")
    out = sync_mod.sync(state_dir, rows)
    assert out["stopped"] == []
    assert "5678" in sync_mod._load_state(state_dir)["forwards"]
    # unfiltered pass reclaims it
    monkeypatch.delenv("SESSIONS_BRIDGE_INGRESS_KINDS")
    sync_mod.sync(state_dir, rows)
    assert "5678" not in sync_mod._load_state(state_dir)["forwards"]
    sync_mod.sync(state_dir, [])


def test_sync_kinds_filter_leaves_unparseable_tracked(tmp_path, monkeypatch):
    """A tracked record whose stored spec this bridge version cannot
    parse (e.g. a kind added by a newer bridge and tracked by the
    project context) is treated as foreign under a filter - the pass
    must not kill a forwarder it cannot classify."""
    state_dir = str(tmp_path / "state")
    tcp_port = _free_port()
    rows = _rows((tcp_port, "fwd=tcp:127.0.0.1:1"))
    sync_mod.sync(state_dir, rows)
    st = sync_mod._load_state(state_dir)
    st["forwards"]["9999"] = {
        "pid": 999999,
        "spec": "future-kind:x:9",  # unknown to this bridge version
        "bind": "0.0.0.0",
        "resolved": "1",
        "started_at": 1,
    }
    sync_mod._save_state(state_dir, st)
    monkeypatch.setenv("SESSIONS_BRIDGE_INGRESS_KINDS", "tcp")
    out = sync_mod.sync(state_dir, rows)
    assert out["stopped"] == []
    assert "9999" in sync_mod._load_state(state_dir)["forwards"]
    monkeypatch.delenv("SESSIONS_BRIDGE_INGRESS_KINDS")
    sync_mod.sync(state_dir, rows)
    assert "9999" not in sync_mod._load_state(state_dir)["forwards"]
    sync_mod.sync(state_dir, [])


def test_sync_bad_row_does_not_block_good_forward(tmp_path):
    state_dir = str(tmp_path / "state")
    good = _free_port()
    rows = _rows((good, "fwd=tcp:127.0.0.1:1"), (88, "fwd=bogus"))
    out = sync_mod.sync(state_dir, rows)
    assert out["started"] == [good]
    assert any(e["row"] == "88" for e in out["errors"])
    sync_mod.sync(state_dir, [])


def test_corrupt_state_file_degrades(tmp_path):
    state_dir = str(tmp_path / "state")
    os.makedirs(state_dir)
    open(sync_mod._state_path(state_dir), "w").write("{not json")
    port = _free_port()
    out = sync_mod.sync(state_dir, _rows((port, "fwd=tcp:127.0.0.1:1")))
    assert out["started"] == [port]
    sync_mod.sync(state_dir, [])


def test_unresolved_target_stops_old_forwarder(tmp_path):
    """A forwarder whose target vanishes is torn down, not stranded
    against a dead netns."""
    state_dir = str(tmp_path / "state")
    port = _free_port()
    out = sync_mod.sync(state_dir, _rows((port, "fwd=tcp:127.0.0.1:1")))
    assert out["started"] == [port]
    pid = json.load(open(sync_mod._state_path(state_dir)))["forwards"][str(port)]["pid"]

    # spec flips to an unresolvable ns-pidfile -> forwarder dies
    missing = tmp_path / "gone.pid"
    out = sync_mod.sync(state_dir, _rows((port, f"fwd=ns-pidfile:{missing}:80")))
    assert out["stopped"] == [port] and len(out["errors"]) == 1
    deadline = time.time() + 3
    while time.time() < deadline and sync_mod._is_our_forwarder(pid, str(port)):
        time.sleep(0.05)
    assert not sync_mod._is_our_forwarder(pid, str(port))


def test_bind_validation():
    with pytest.raises(ValueError):
        parse_fwd_spec("tcp:127.0.0.1:9", bind="not-an-ip")
    with pytest.raises(ValueError):
        parse_fwd_spec("tcp:127.0.0.1:9", bind="hostname.local")
    assert parse_fwd_spec("tcp:127.0.0.1:9", bind="::1").bind == "::1"


def test_sync_spawn_keep_stop(tmp_path):
    state_dir = str(tmp_path / "state")
    upstream_port = _free_port()
    listen_port = _free_port()
    rows = _rows((listen_port, f"fwd=tcp:127.0.0.1:{upstream_port} test"))

    out = sync_mod.sync(state_dir, rows)
    assert out["started"] == [listen_port] and not out["errors"]
    rec = json.load(open(sync_mod._state_path(state_dir)))["forwards"][str(listen_port)]
    pid = rec["pid"]
    assert os.path.exists(f"/proc/{pid}")

    # second sync keeps the healthy forwarder
    out = sync_mod.sync(state_dir, rows)
    assert out["kept"] == [listen_port] and out["started"] == []

    # dropping the row stops the forwarder
    out = sync_mod.sync(state_dir, [])
    assert out["stopped"] == [listen_port]
    deadline = time.time() + 3
    while time.time() < deadline and sync_mod._is_our_forwarder(pid, str(listen_port)):
        time.sleep(0.05)
    assert not sync_mod._is_our_forwarder(pid, str(listen_port))
    assert json.load(open(sync_mod._state_path(state_dir)))["forwards"] == {}


def test_sync_respawns_on_target_change(tmp_path):
    state_dir = str(tmp_path / "state")
    pf = tmp_path / "inner.pid"
    pf.write_text("1")
    listen_port = _free_port()
    # ns-pidfile target is not connectable here, but spawning the
    # forwarder must still succeed (it binds before entering the ns
    # only for real pids; pid 1 fails setns -> child dies). Use a
    # spec we can keep alive instead: tcp spec then a spec change.
    rows = _rows((listen_port, "fwd=tcp:127.0.0.1:1"))
    out = sync_mod.sync(state_dir, rows)
    assert out["started"] == [listen_port]
    pid1 = json.load(open(sync_mod._state_path(state_dir)))["forwards"][str(listen_port)]["pid"]

    rows2 = _rows((listen_port, "fwd=tcp:127.0.0.1:2"))
    out = sync_mod.sync(state_dir, rows2)
    pid2 = json.load(open(sync_mod._state_path(state_dir)))["forwards"][str(listen_port)]["pid"]
    assert out["started"] == [listen_port] and pid2 != pid1
    sync_mod.sync(state_dir, [])


def test_sync_unresolved_target_errors(tmp_path):
    state_dir = str(tmp_path / "state")
    missing = tmp_path / "nope.pid"
    out = sync_mod.sync(
        state_dir,
        _rows((9991, f"fwd=ns-pidfile:{missing}:80")),
    )
    assert out["started"] == [] and len(out["errors"]) == 1
    assert "unresolved" in out["errors"][0]["error"]


def test_sync_concurrent_lock_skips(tmp_path):
    """A second sync while the lock is held reports skipped, not a
    crash - the timer and a manual call may legitimately race."""
    import fcntl

    state_dir = str(tmp_path / "state")
    os.makedirs(state_dir)
    lock = open(os.path.join(state_dir, sync_mod.LOCK_FILE), "w")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        out = sync_mod.sync(state_dir, _rows((9992, "fwd=tcp:127.0.0.1:1")))
        assert out["status"] == "skipped"
    finally:
        fcntl.flock(lock, fcntl.LOCK_UN)
        lock.close()


def test_reserve_bind_requires_fwd(tmp_path):
    from session_bridge.cli import bridge

    db_path = str(tmp_path / "state.db")
    db.init(db_path)
    rc = bridge.main(
        [
            "--db",
            db_path,
            "reserve",
            "tcp_port",
            "59997",
            "--project",
            "t",
            "--bind",
            "127.0.0.1",
        ]
    )
    assert rc == ExitCode.USAGE


def test_reserve_bind_rejects_non_ip(tmp_path):
    from session_bridge.cli import bridge

    db_path = str(tmp_path / "state.db")
    db.init(db_path)
    rc = bridge.main(
        [
            "--db",
            db_path,
            "reserve",
            "tcp_port",
            "59996",
            "--project",
            "t",
            "--fwd",
            "tcp:127.0.0.1:9",
            "--bind",
            "hostname.local",
        ]
    )
    assert rc == ExitCode.USAGE


def test_e2e_tcp_relay(tmp_path):
    """Real forwarder relays bytes to a live upstream listener."""
    state_dir = str(tmp_path / "state")
    upstream_port, listen_port = _free_port(), _free_port()
    received = []

    upstream = socket.socket()
    upstream.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    upstream.bind(("127.0.0.1", upstream_port))
    upstream.listen(1)

    def serve():
        conn, _ = upstream.accept()
        received.append(conn.recv(100))
        conn.sendall(b"pong")
        conn.close()

    t = threading.Thread(target=serve, daemon=True)
    t.start()

    rows = _rows((listen_port, f"fwd=tcp:127.0.0.1:{upstream_port}"))
    try:
        out = sync_mod.sync(state_dir, rows)
        assert out["started"] == [listen_port]
        deadline = time.time() + 5
        while True:
            try:
                c = socket.create_connection(("127.0.0.1", listen_port), timeout=1)
                break
            except OSError:
                assert time.time() < deadline
                time.sleep(0.05)
        c.sendall(b"ping")
        assert c.recv(100) == b"pong"
        c.close()
        t.join(timeout=3)
        assert received == [b"ping"]
    finally:
        sync_mod.sync(state_dir, [])


def test_reserve_fwd_flag(tmp_path, capsys):
    """CLI: reserve --fwd embeds the spec in stored purpose."""
    from session_bridge.cli import bridge

    db_path = str(tmp_path / "state.db")
    db.init(db_path)
    port = _free_port()
    rc = bridge.main(
        [
            "--db",
            db_path,
            "reserve",
            "tcp_port",
            str(port),
            "--project",
            "t",
            "--fwd",
            "tcp:127.0.0.1:9000",
            "--bind",
            "127.0.0.1",
            "--purpose",
            "demo",
        ]
    )
    assert rc == ExitCode.OK
    rows = db.list_allocations(resource_type="tcp_port", path=db_path)
    s = parse_purpose(rows[0]["purpose"])
    assert s.spec_text == "tcp:127.0.0.1:9000" and s.bind == "127.0.0.1"
    assert "demo" in rows[0]["purpose"]


def test_reserve_fwd_rejects_non_port(tmp_path):
    from session_bridge.cli import bridge

    db_path = str(tmp_path / "state.db")
    db.init(db_path)
    rc = bridge.main(
        [
            "--db",
            db_path,
            "reserve",
            "netns_id",
            "x",
            "--project",
            "t",
            "--fwd",
            "tcp:127.0.0.1:9",
        ]
    )
    assert rc == ExitCode.USAGE
