"""Standalone TCP forwarder for the host ingress layer.

    python -m session_bridge.ingress.forwarder <bind> <port> <spec>

Binds <bind>:<port> in the forwarder's own network namespace first,
then for ``ns-*`` specs setns() into the target netns and relays each
accepted connection to the spec's target inside it. Self-contained on
purpose: forwarder processes are spawned detached and must not import
the rest of the package at runtime (spec.parse_fwd_spec only).

Exits non-zero on any startup failure (bind conflict, missing
pidfile, setns failure) - the reconciler records the death and
retries on the next sync.
"""

from __future__ import annotations

import ctypes
import os
import socket
import sys
import threading

from session_bridge.ingress.spec import parse_fwd_spec

CLONE_NEWNET = 0x40000000
libc = ctypes.CDLL("libc.so.6", use_errno=True)


def _enter_netns(pid: str) -> None:
    fd = os.open(f"/proc/{pid}/ns/net", os.O_RDONLY)
    try:
        if libc.setns(fd, CLONE_NEWNET) != 0:
            raise OSError(ctypes.get_errno(), "setns failed")
    finally:
        os.close(fd)


def _pump(a: socket.socket, b: socket.socket) -> None:
    try:
        while True:
            d = a.recv(65536)
            if not d:
                break
            b.sendall(d)
    except OSError:
        pass
    finally:
        for s in (a, b):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


def main(argv: list[str]) -> int:
    if len(argv) != 4:
        sys.stderr.write("usage: forwarder <bind> <port> <spec>\n")
        return 2
    bind, listen_port, spec_text = argv[1], int(argv[2]), argv[3]
    spec = parse_fwd_spec(spec_text, bind=bind)

    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((bind, listen_port))
    srv.listen(64)

    if spec.kind == "tcp":
        host, port = spec.arg, spec.port
    else:
        pid = spec.arg if spec.kind == "ns-pid" else open(spec.arg).read().strip()
        _enter_netns(pid)
        host, port = "127.0.0.1", spec.port

    print(f"ingress-fwd listening {bind}:{listen_port} -> {spec_text}", flush=True)

    def handle(conn: socket.socket) -> None:
        try:
            up = socket.create_connection((host, port), timeout=10)
        except OSError:
            conn.close()
            return
        threading.Thread(target=_pump, args=(up, conn), daemon=True).start()
        _pump(conn, up)

    while True:
        conn, _ = srv.accept()
        threading.Thread(target=handle, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
