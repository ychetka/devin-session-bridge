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
    for bad in ["", "bogus", "bogus:1:2", "tcp:127.0.0.1", "tcp::80",
                "ns-pid:abc:80", "tcp:127.0.0.1:0", "tcp:127.0.0.1:70000",
                "ns-pidfile:relative.pid:80"]:
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
    d = sync_mod.desired_forwards(
        [
            *_rows((80, "fwd=tcp:127.0.0.1:9 x"), (81, "plain row"), (82, None)),
            {"resource_type": "netns_id", "resource_id": "n", "purpose": "fwd=tcp:x:1"},
        ]
    )
    assert set(d) == {80}


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
            "--db", db_path,
            "reserve", "tcp_port", str(port),
            "--project", "t",
            "--fwd", "tcp:127.0.0.1:9000",
            "--bind", "127.0.0.1",
            "--purpose", "demo",
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
            "--db", db_path,
            "reserve", "netns_id", "x",
            "--project", "t",
            "--fwd", "tcp:127.0.0.1:9",
        ]
    )
    assert rc == ExitCode.USAGE
