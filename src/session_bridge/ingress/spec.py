"""Forward-spec parsing for the ingress layer.

A ``tcp_port`` allocation may carry a forward declaration inside its
``purpose`` text:

    fwd=<spec> [bind=<addr>] <free text>

spec grammar:

    tcp:<host>:<port>           relay to host:port in the forwarder's
                                own network namespace
    ns-pid:<pid>:<port>         setns() into /proc/<pid>/ns/net, dial
                                127.0.0.1:<port> inside it
    ns-pidfile:<path>:<port>    same, resolving the pid from a file at
                                forwarder start (and on each reconcile)

``bind=<addr>`` (default ``0.0.0.0``) selects the host listen address.
Everything not matching the two tokens is ordinary free text.
"""

from __future__ import annotations

import ipaddress
import re
from dataclasses import dataclass

FWD_TOKEN = re.compile(r"\bfwd=(\S+)")
BIND_TOKEN = re.compile(r"\bbind=(\S+)")

_KINDS = ("tcp", "ns-pid", "ns-pidfile")
DEFAULT_BIND = "0.0.0.0"


@dataclass(frozen=True)
class ForwardSpec:
    """Parsed forward declaration."""

    kind: str  # tcp | ns-pid | ns-pidfile
    arg: str  # host | pid | pidfile path (kind-dependent)
    port: int  # target port (for tcp it is host:arg:port)
    bind: str = DEFAULT_BIND

    @property
    def spec_text(self) -> str:
        """Canonical spec string passed to the forwarder child."""
        return f"{self.kind}:{self.arg}:{self.port}"


def _port(raw: str, what: str) -> int:
    try:
        port = int(raw)
    except ValueError:
        raise ValueError(f"fwd spec: {what} must be a port number, got {raw!r}") from None
    if not 1 <= port <= 65535:
        raise ValueError(f"fwd spec: {what} out of range: {port}")
    return port


def validate_bind(bind: str) -> str:
    """Bind addresses are literal IPs - a hostname would silently
    turn the forwarder into a DNS-dependent deploy step."""
    try:
        ipaddress.ip_address(bind)
    except ValueError:
        raise ValueError(f"bind must be a literal IP address, got {bind!r}") from None
    return bind


def parse_fwd_spec(spec: str, *, bind: str = DEFAULT_BIND) -> ForwardSpec:
    """Parse a bare ``<kind>:<arg>:<port>`` spec string."""
    validate_bind(bind)
    parts = spec.split(":")
    if len(parts) < 3 or parts[0] not in _KINDS:
        raise ValueError(
            f"fwd spec must be one of {', '.join(_KINDS)}:<arg>:<port>, got {spec!r}"
        )
    kind = parts[0]
    if kind == "ns-pidfile":
        arg = ":".join(parts[1:-1])  # paths may contain ':'
        if not arg.startswith("/"):
            raise ValueError(f"fwd spec: ns-pidfile needs an absolute path, got {arg!r}")
    else:
        if len(parts) != 3:
            raise ValueError(f"fwd spec: {kind} takes exactly <arg>:<port>, got {spec!r}")
        arg = parts[1]
        if kind == "ns-pid" and not arg.isdigit():
            raise ValueError(f"fwd spec: ns-pid needs a numeric pid, got {arg!r}")
        if kind == "tcp" and not arg:
            raise ValueError("fwd spec: tcp needs a non-empty host")
    return ForwardSpec(kind=kind, arg=arg, port=_port(parts[-1], "target port"), bind=bind)


def parse_purpose(purpose: str | None) -> ForwardSpec | None:
    """Extract ``fwd=<spec>`` / ``bind=<addr>`` tokens from a purpose
    string. Returns None when no forward declaration is present."""
    if not purpose:
        return None
    m = FWD_TOKEN.search(purpose)
    if m is None:
        return None
    b = BIND_TOKEN.search(purpose)
    return parse_fwd_spec(m.group(1), bind=b.group(1) if b else DEFAULT_BIND)


def resolve_target(spec: ForwardSpec) -> str:
    """Identity of the concrete forwarding target at this moment.

    For ``ns-pidfile`` this is the pid currently in the file - when the
    owning stack restarts and the file gains a new pid, the resolved
    value changes and the reconciler respawns the forwarder. For
    ``ns-pid`` and ``tcp`` the identity is the spec itself.
    """
    if spec.kind == "ns-pidfile":
        try:
            pid = int(open(spec.arg).read().strip())
        except (OSError, ValueError):
            return ""  # unreadable pidfile -> target unknown
        return f"ns:{pid}:{spec.port}"
    if spec.kind == "ns-pid":
        return f"ns:{spec.arg}:{spec.port}"
    return f"tcp:{spec.arg}:{spec.port}"


def format_purpose(spec_text: str, bind: str | None, purpose: str | None) -> str:
    """Compose the stored purpose string: fwd token first, optional
    bind token, then the human description."""
    tokens = [f"fwd={spec_text}"]
    if bind and bind != DEFAULT_BIND:
        tokens.append(f"bind={bind}")
    if purpose:
        tokens.append(purpose)
    return " ".join(tokens)
