"""Host-resource allocation commands (task #210 split).

reserve / release / allocations / ingress.
"""

from session_bridge.cli.common import _emit, _state_dir
from session_bridge.cli.contract import (
    ExitCode,
    JsonResponse,
)
from session_bridge.ingress import spec as ingress_spec
from session_bridge.ingress import sync as ingress_sync
from session_bridge.state import db
from session_bridge.state.verdict import (
    ReleaseNotFound,
    ReleaseRefusedOwner,
    ReleaseRemoved,
    ReserveConflict,
    ReserveGranted,
)


def _cmd_reserve(args) -> int:
    """``bridge reserve <type> <id> --project P``: atomic allocation.

    The host-level resource registry (directive 426): a project
    declares each host resource it needs BEFORE deploying - the
    PRIMARY KEY on (type, id) is the mutex, so the verdict is
    atomic: granted (new or same-owner renew) or conflict echoing
    the incumbent owner. Deploy scripts treat any non-granted exit
    as a hard fail and escalate to the orchestrator - never retry
    or force their way past a conflict.

    ``--fwd`` (tcp_port only) embeds a machine-readable forward
    declaration into the stored purpose - the neutral ingress layer
    materializes a host listener from it (docs/ALLOCATIONS.md).
    """
    purpose = args.purpose
    if args.fwd is not None:
        try:
            fwd = ingress_spec.parse_fwd_spec(args.fwd, bind=args.bind or ingress_spec.DEFAULT_BIND)
        except ValueError as e:
            resp = JsonResponse(status="usage", error=str(e))
            return _emit(args, resp, ExitCode.USAGE, f"usage: {e}")
        if args.resource_type != "tcp_port":
            resp = JsonResponse(status="usage", error="--fwd is only valid for tcp_port")
            return _emit(args, resp, ExitCode.USAGE, "usage: --fwd is only valid for tcp_port")
        purpose = ingress_spec.format_purpose(
            fwd.spec_text,
            args.bind,
            args.purpose,
        )
    elif args.bind is not None:
        resp = JsonResponse(status="usage", error="--bind requires --fwd")
        return _emit(args, resp, ExitCode.USAGE, "usage: --bind requires --fwd")
    try:
        verdict = db.reserve(
            args.resource_type,
            args.resource_id,
            args.project,
            purpose=purpose,
            session_id=args.session,
            ttl=args.ttl,
            path=args.db,
        )
    except ValueError as e:
        resp = JsonResponse(status="usage", error=str(e))
        return _emit(args, resp, ExitCode.USAGE, f"usage: {e}")
    if isinstance(verdict, ReserveConflict):
        resp = JsonResponse(
            status="conflict",
            error=f"{args.resource_type}/{verdict.resource_id} held by {verdict.owner}",
            data={
                "owner": verdict.owner,
                "incumbent_resource_id": verdict.resource_id,
                "purpose": verdict.purpose,
                "created_at": verdict.created_at,
                "expires_at": verdict.expires_at,
            },
        )
        # the incumbent key differs from the request only on a
        # wildcard (systemd_unit_prefix `P-*`) conflict - name it so
        # release --force arbitration needs no second query
        key_note = (
            f" via wildcard {verdict.resource_id}"
            if verdict.resource_id != args.resource_id
            else ""
        )
        return _emit(
            args,
            resp,
            ExitCode.CONFLICT,
            f"conflict: {args.resource_type}/{args.resource_id} held by {verdict.owner}"
            f"{key_note}" + (f" ({verdict.purpose})" if verdict.purpose else ""),
        )
    assert isinstance(verdict, ReserveGranted)
    resp = JsonResponse(
        status="ok",
        data={
            "resource_type": args.resource_type,
            "resource_id": args.resource_id,
            "project": args.project,
            "renewed": verdict.renewed,
            "expires_at": verdict.expires_at,
        },
    )
    verb = "renewed" if verdict.renewed else "reserved"
    return _emit(
        args,
        resp,
        ExitCode.OK,
        f"{verb}: {args.resource_type}/{args.resource_id} -> {args.project}",
    )


def _cmd_release(args) -> int:
    """``bridge release <type> <id> --project P [--force]``: drop the
    allocation. Owner-only; ``--force`` is the orchestrator's
    arbitration override for genuinely contested resources - deploy
    scripts must never pass it."""
    try:
        verdict = db.release(
            args.resource_type,
            args.resource_id,
            args.project,
            force=args.force,
            path=args.db,
        )
    except ValueError as e:
        resp = JsonResponse(status="usage", error=str(e))
        return _emit(args, resp, ExitCode.USAGE, f"usage: {e}")
    if isinstance(verdict, ReleaseNotFound):
        resp = JsonResponse(status="ok", data={"released": False})
        return _emit(args, resp, ExitCode.OK, "no allocation at that key")
    if isinstance(verdict, ReleaseRefusedOwner):
        resp = JsonResponse(
            status="conflict",
            error=f"held by {verdict.owner} (use --force only for orchestrator arbitration)",
            data={"owner": verdict.owner},
        )
        return _emit(
            args,
            resp,
            ExitCode.CONFLICT,
            f"conflict: held by {verdict.owner} - orchestrator arbitration only (--force)",
        )
    assert isinstance(verdict, ReleaseRemoved)
    resp = JsonResponse(status="ok", data={"released": True, "owner": verdict.owner})
    return _emit(
        args,
        resp,
        ExitCode.OK,
        f"released: {args.resource_type}/{args.resource_id} (was {verdict.owner})",
    )


def _cmd_allocations(args) -> int:
    """``bridge allocations [--type T] [--project P]``: live registry."""
    rows = db.list_allocations(resource_type=args.type, project_id=args.project, path=args.db)
    items = [
        {
            "resource_type": r["resource_type"],
            "resource_id": r["resource_id"],
            "project": r["project_id"],
            "purpose": r["purpose"],
            "session": r["session_id"],
            "created_at": r["created_at"],
            "expires_at": r["expires_at"],
        }
        for r in rows
    ]
    resp = JsonResponse(status="ok", data={"allocations": items})
    if not items:
        return _emit(args, resp, ExitCode.OK, "no allocations")
    lines = [
        f"{r['resource_type']}/{r['resource_id']} -> {r['project']}"
        + (f" ({r['purpose']})" if r["purpose"] else "")
        + (f" [expires {r['expires_at']}]" if r["expires_at"] else "")
        for r in items
    ]
    return _emit(args, resp, ExitCode.OK, "\n".join(lines))


def _cmd_ingress(args) -> int:
    """``bridge ingress sync|status``: the neutral host ingress layer.

    sync reconciles running forwarder processes with tcp_port rows
    carrying a fwd= declaration; status shows desired vs running.
    """
    rows = db.list_allocations(resource_type="tcp_port", path=args.db)
    sdir = _state_dir(args)
    if args.ingress_action == "sync":
        summary = ingress_sync.sync(sdir, rows)
        ok = summary.get("status") == "ok"
        errors = summary.get("errors", [])
        resp = JsonResponse(
            status="ok" if ok else "skipped",
            data=summary,
            error=None if ok else summary.get("reason"),
        )
        lines = [
            f"started={summary.get('started', [])} "
            f"stopped={summary.get('stopped', [])} "
            f"kept={summary.get('kept', [])} "
            f"errors={len(errors)}"
        ]
        if not ok:
            lines.append(f"skipped: {summary.get('reason')}")
        for err in errors:
            detail = err.get("spec") or err.get("row") or "?"
            lines.append(f"  port {err.get('port', detail)}: {err['error']}")
        # partial failures are visible to exit-code-gated callers:
        # some forwards materialized, some failed -> not a clean OK
        code = ExitCode.OK if ok and not errors else ExitCode.INTERNAL
        return _emit(args, resp, code, "\n".join(lines))
    view = ingress_sync.status(sdir, rows)
    resp = JsonResponse(status="ok", data=view)
    if not view["forwards"]:
        return _emit(args, resp, ExitCode.OK, "no ingress forwards declared")
    lines = [
        f"{f['port']} {'UP' if f['alive'] else 'DOWN'} "
        f"{f['bind']}:{f['port']} -> {f['spec']} pid={f['forwarder_pid']}"
        for f in view["forwards"]
    ]
    for port in view["orphaned"]:
        lines.append(f"{port} ORPHAN (tracked but row gone)")
    for err in view.get("row_errors", []):
        lines.append(f"row {err['row']}: BAD FWD ({err['error']})")
    return _emit(args, resp, ExitCode.OK, "\n".join(lines))
