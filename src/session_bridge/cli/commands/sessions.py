"""Session-domain commands (task #210 split).

register / arm / disarm / unregister / halt / resume / kill / ack /
heartbeat / wake.
"""

import os
import shutil
import time

from session_bridge.cli.commands import ops as _ops
from session_bridge.cli.common import (
    _emit,
    _merge_dir_no_clobber,
    _queue_depth,
    _queue_dir,
    _reap_unregister_tombstones,
    _session_cwd,
    _state_dir,
    _tombstone_path,
)
from session_bridge.cli.contract import (
    ExitCode,
    JsonResponse,
)
from session_bridge.cli.status import _all_session_rows
from session_bridge.daemon import core as daemon
from session_bridge.mailbox import mailbox_dir
from session_bridge.state import db
from session_bridge.state.contract import (
    NotificationKind,
    SessionMode,
    SessionStatus,
)
from session_bridge.state.verdict import (
    KillNotFound,
    TaskKillNotFound,
    TaskKillRefused,
    TaskWithdrawNotFound,
    TaskWithdrawRefused,
    UnregisterNotFound,
    UnregisterRefusedFreshHeartbeat,
    UnregisterRefusedLiveTask,
)
from session_bridge.stop import hard_stop
from session_bridge.wake import (
    SESSION_ABSENT,
    WakeError,
    cancel_turn,
    close_session,
    find_lockfile,
    verify_sessions,
    wake_session,
)

_DISARM_BODY = "managed mode disarmed: stop re-arming the mailbox watcher"

#: hard-stop layer timings (issue #164): the cancel confirm window IS
#: the layer-0 grace the design calls for (~5s) - after it the forced
#: close fires; the sweep holds its own TERM->KILL grace.
_STOP_CANCEL_CONFIRM = 5.0
_STOP_CLOSE_CONFIRM = 3.0
_SWEEP_TERM_GRACE = 2.0


def _cmd_arm(args) -> int:
    """``bridge arm <sid>``: enroll a session into managed mode."""
    db.set_session_mode(args.session_id, SessionMode.MANAGED, path=args.db)
    resp = JsonResponse(
        session=args.session_id,
        status=SessionMode.MANAGED.value,
        queue_depth=_queue_depth(args),
        agent_instruction=(
            "session armed: deliveries go through the mailbox watcher"
            " (SessionStart/kick injects the mailbox duty)"
        ),
    )
    return _emit(args, resp, ExitCode.OK, f"session {args.session_id} armed (managed)")


def _cmd_disarm(args) -> int:
    """``bridge disarm <sid>|--all``: leave managed mode + DISARM notify.

    The durable outbox DISARM row is what a live watcher receives once
    the daemon relays it - mode is cleared first so a restarted or
    re-arming session sees unmanaged immediately. With the daemon
    down the command runs the relay + stale-control janitor itself
    (issue #119) so the DISARM lands and stale control files are
    pruned in the same call.
    """
    if args.all:
        sids = [
            r["session_id"]
            for r in _all_session_rows(args)
            if r["mode"] == SessionMode.MANAGED.value
        ]
    elif args.session_id:
        sids = [args.session_id]
    else:
        resp = JsonResponse(
            status="usage",
            error="disarm requires a session id or --all",
        )
        return _emit(
            args,
            resp,
            ExitCode.USAGE,
            "usage: bridge disarm <session_id> | bridge disarm --all",
        )
    for sid in sids:
        db.set_session_mode(sid, SessionMode.UNMANAGED, path=args.db)
        # leaving managed mode drops the managed-only halt flag too -
        # a disarmed session no longer owes drain duty, and a stray
        # halted_at would still refuse its claims after a re-arm
        db.set_session_halted(sid, False, path=args.db)
        db.notify(sid, NotificationKind.DISARM, _DISARM_BODY, path=args.db)
    data: dict = {"disarmed": sids}
    # daemon-down delivery parity (issue #119): a stale control file
    # must not outlive the fresh DISARM when no loop can prune it
    try:
        if _sync_control_delivery(args, _state_dir(args)):
            data["relayed"] = True
    except Exception as e:
        data["relay_error"] = f"{type(e).__name__}: {e}"
    resp = JsonResponse(
        session=None if args.all else args.session_id,
        status=SessionMode.UNMANAGED.value,
        queue_depth=_queue_depth(args),
        data=data,
    )
    if args.all:
        human = f"disarmed {len(sids)} managed session(s)"
        if sids:
            human += f": {', '.join(sids)}"
    else:
        human = f"session {args.session_id} disarmed (unmanaged)"
    return _emit(args, resp, ExitCode.OK, human)


_HALT_BODY = (
    "HALT: finish your current atomic action only, save state"
    " (commit/push WIP, notes in the task file), then end your turn."
    " Do not claim new queued tasks - they persist and resume later."
)

_RESUME_BODY = "RESUME: the halt is lifted - normal mailbox duty resumes"


def _cmd_unregister(args) -> int:
    """``bridge unregister <sid> [--force]``: remove a dead registration (task #198).

    Ghost registrations - a sid that registered once and vanished -
    keep an active sessions row, stale outbox rows and an undrained
    mailbox that haunts every fleet status. Removal is guarded by
    three refusals:

    - a live task still owns the lifecycle: fail/requeue it first;
      deleting it mid-flight is a kill, not a cleanup. Enforced
      inside the deletion transaction - no --force override.
    - a fresh heartbeat means a live client is on the bus right now
      (authoritative for unmanaged sessions); also enforced inside
      the transaction so a heartbeat stamped between the legs and
      the delete cannot be lost to a TOCTOU gap.
    - the acp-host still lists the session: ANY listed entry -
      including a statusless one - is a resumable Devin session and
      therefore alive evidence no stale row can refute. The host
      check is best-effort and cannot live in the write txn
      (network I/O): a missing lockfile or failed attach proves
      nothing either way, so it does not refuse (``host_check`` in
      the output records which leg ran); ``--force`` skips this leg
      and the heartbeat leg for the case the operator knows better.

    Deletion covers every session-scoped row plus the mailbox
    directory (delivery artifacts, not history); ``audit_events``
    and the decision journal keep the audit trail.
    """
    sid = args.session_id
    srow = db.get_session(sid, path=args.db)
    if srow is None:
        resp = JsonResponse(
            session=sid,
            status="not_found",
            error=f"no registration for {sid}",
        )
        return _emit(args, resp, ExitCode.NOT_FOUND, f"not registered: {sid}")

    host_check = "force"
    if not args.force:
        lock_data = find_lockfile()
        if lock_data is None:
            host_check = "no_lockfile"
        else:
            try:
                st = verify_sessions([sid], lock_data=lock_data).get(sid)
            except WakeError as e:
                host_check = f"verify_failed:{e.reason}"
            else:
                # listed at all - even statusless (None) - is alive
                # evidence: the host can still resume it
                if st != SESSION_ABSENT:
                    resp = JsonResponse(
                        session=sid,
                        status="conflict",
                        error=f"verifiably alive in acp-host (status={st}) (--force to override)",
                    )
                    return _emit(
                        args,
                        resp,
                        ExitCode.CONFLICT,
                        f"conflict: {sid} alive in acp-host status={st} (--force to override)",
                    )
                host_check = "absent"

    # tombstone-rename the mailbox BEFORE the db delete (audit F1,
    # task #203): between the delete commit and a direct rmtree of
    # <sid>, a relay could materialize new mail or a same-sid
    # re-register could create a fresh mailbox that the late rmtree
    # would wipe. An atomic rename parks the pre-delete mailbox
    # under a name no writer targets; a refusal restores it, a
    # success deletes the tombstone. Mail landing in a recreated
    # <sid> dir during the delete belongs to the post-delete world
    # and is left alone.
    mdir = mailbox_dir(sid, db_path=args.db, queue_dir=args.queue_dir)
    _reap_unregister_tombstones(sid, mdir)
    tombstone = None
    rename_failed = False
    if os.path.isdir(mdir):
        tombstone = _tombstone_path(sid, mdir)
        try:
            os.rename(mdir, tombstone)
        except OSError:
            tombstone = None
            rename_failed = True

    def _restore_tombstone() -> str:
        """Put the parked mailbox back - the registration survived.

        Returns 'restored' | 'merged' | 'none' | 'failed:<err>' so a
        restore failure lands in the response instead of silently
        stranding the mail.
        """
        nonlocal tombstone
        if tombstone is None:
            return "none"
        try:
            if not os.path.isdir(mdir):
                os.rename(tombstone, mdir)
                result = "restored"
            else:
                # a writer recreated <sid> in the window: merge the
                # parked files into the live dir without clobbering
                # post-delete writes, then drop the park
                _merge_dir_no_clobber(tombstone, mdir)
                shutil.rmtree(tombstone)
                result = "merged"
        except OSError as e:
            result = f"failed:{e}"
        tombstone = None
        return result

    try:
        verdict = db.unregister_session(sid, force=args.force, path=args.db)
    except Exception:
        # the delete never committed: the registration is intact, so
        # the parked mailbox must come back before the error surfaces
        _restore_tombstone()
        raise

    if isinstance(verdict, UnregisterNotFound):
        # raced with a concurrent unregister: the row is already gone,
        # which is the outcome the caller wanted. The parked mailbox
        # is restored ONLY when the sid re-registered in the window
        # (it belongs to the new registration); otherwise it is the
        # deleted session's last mailbox - remove it rather than
        # resurrecting an orphan dir nothing drains.
        if db.get_session(sid, path=args.db) is not None:
            restored = _restore_tombstone()
        else:
            if tombstone is not None:
                shutil.rmtree(tombstone, ignore_errors=True)
                tombstone = None
            # the row is gone: any <sid> dir left (e.g. the tombstone
            # rename failed earlier, or a writer slipped files into a
            # recreated dir) is orphaned dead-session mail - remove it
            # instead of leaving an undrainable artifact behind
            if os.path.isdir(mdir):
                shutil.rmtree(mdir, ignore_errors=True)
            restored = "removed"
        resp = JsonResponse(
            session=sid,
            status="not_found",
            error=f"no registration for {sid} (removed concurrently)",
            data={"mailbox_restored": restored},
        )
        return _emit(
            args,
            resp,
            ExitCode.NOT_FOUND,
            f"not registered: {sid} (removed concurrently)",
        )
    if isinstance(verdict, UnregisterRefusedLiveTask):
        restored = _restore_tombstone()
        resp = JsonResponse(
            session=sid,
            status="conflict",
            error=(
                f"task {verdict.task_id} is {verdict.task_status} - close it before unregistering"
            ),
            data={"mailbox_restored": restored},
        )
        return _emit(
            args,
            resp,
            ExitCode.CONFLICT,
            f"conflict: task {verdict.task_id} still {verdict.task_status} (mailbox {restored})",
        )
    if isinstance(verdict, UnregisterRefusedFreshHeartbeat):
        restored = _restore_tombstone()
        resp = JsonResponse(
            session=sid,
            status="conflict",
            error=(
                f"fresh heartbeat {verdict.age_seconds}s ago"
                " - session is alive (--force to override)"
            ),
            data={"mailbox_restored": restored},
        )
        return _emit(
            args,
            resp,
            ExitCode.CONFLICT,
            f"conflict: {sid} heartbeat {verdict.age_seconds}s ago (--force to override)"
            f" (mailbox {restored})",
        )
    counts = verdict.counts
    mail_files = 0
    if tombstone is not None:
        try:
            mail_files = len(os.listdir(tombstone))
        except OSError:
            mail_files = None
        shutil.rmtree(tombstone, ignore_errors=True)
        mailbox_removed = not os.path.isdir(tombstone)
    elif os.path.isdir(mdir):
        # the tombstone rename failed earlier - degraded to the old
        # delete-in-place path, which keeps the F1 race window
        try:
            mail_files = len(os.listdir(mdir))
        except OSError:
            mail_files = None
        shutil.rmtree(mdir, ignore_errors=True)
        mailbox_removed = not os.path.isdir(mdir)
    else:
        mailbox_removed = True
    recreated = os.path.isdir(mdir)
    daemon._decision(
        _state_dir(args),
        "session_unregistered",
        sid,
        f"counts={counts} mail_files={mail_files} host={host_check}",
    )
    resp = JsonResponse(
        session=sid,
        status="unregistered",
        queue_depth=_queue_depth(args),
        data={
            "counts": counts,
            "mailbox_files_removed": mail_files,
            "mailbox_removed": mailbox_removed,
            "mailbox_tombstoned": tombstone is not None,
            "mailbox_rename_failed": rename_failed,
            "mailbox_dir_recreated": recreated,
            "host_check": host_check,
        },
    )
    tail = "" if mailbox_removed else " (mailbox dir survived removal!)"
    files_text = "unknown" if mail_files is None else str(mail_files)
    return _emit(
        args,
        resp,
        ExitCode.OK,
        f"unregistered {sid} ({files_text} mailbox file(s), host={host_check}){tail}",
    )


def _halt_targets(args) -> tuple[list[str], int | None]:
    """Session ids the halt/resume applies to; empty + usage rc.

    ``--all`` broadcasts to every managed+active session - the same
    fleet set ``bridge disarm --all`` addresses.
    """
    if args.all:
        return [
            r["session_id"]
            for r in _all_session_rows(args)
            if r["mode"] == SessionMode.MANAGED.value and r["status"] == SessionStatus.ACTIVE.value
        ], None
    if args.session_id:
        return [args.session_id], None
    resp = JsonResponse(
        status="usage",
        error=f"{args.command} requires a session id or --all",
    )
    rc = _emit(
        args,
        resp,
        ExitCode.USAGE,
        f"usage: bridge {args.command} <session_id> | bridge {args.command} --all",
    )
    return [], rc


def _sync_control_delivery(args, sdir: str) -> bool:
    """Materialize control mail + prune stale files when the daemon
    is down (issue #119 review): the staleness guard lives in the
    daemon, so during an outage a session draining its mailbox would
    still meet - and execute - a superseded control file. Running the
    relay and janitor synchronously at command time keeps delivery
    semantics identical with or without the loop: the fresh command
    lands as a file and the stale one is unlinked in the same call.
    Returns True when the synchronous legs ran (daemon absent).
    """
    if daemon.daemon_running(sdir):
        return False
    now = int(time.time())
    daemon._relay_pass(args.db, _queue_dir(args), sdir, now)
    daemon._stale_control_pass(args.db, _queue_dir(args), sdir, now)
    return True


def _mailbox_file_count(args, sid: str) -> int:
    """Unconsumed ``msg_*.txt`` files in the sid's mailbox - the
    "whose mail is undrained" column of the halt snapshot."""
    try:
        return sum(
            1
            for name in os.listdir(os.path.join(_queue_dir(args), sid))
            if name.startswith("msg_") and name.endswith(".txt")
        )
    except OSError:
        return 0


def _halt_apply(args, sid: str, halt: bool) -> dict:
    """Flag + notify + wake one session; fold the result into a row.

    HALT is a drain-on-boundary contract (task #180): the flag and
    notification persist regardless of wake outcome, so a mid-turn
    session picks it up at its own boundary - the wake is a
    best-effort speedup, not the delivery path.
    """
    sess = db.get_session(sid, path=args.db)
    row = {"session_id": sid}
    if sess is None or sess["status"] != SessionStatus.ACTIVE.value:
        row["status"] = "skipped"
        row["reason"] = "not registered" if sess is None else f"is {sess['status']}"
        return row
    if halt and sess["mode"] != SessionMode.MANAGED.value:
        row["status"] = "skipped"
        row["reason"] = "not managed - halt duty is managed-only"
        return row
    if halt and sess["halted_at"] is not None:
        row["status"] = "skipped"
        row["reason"] = "already halted"
        return row
    if not halt and sess["halted_at"] is None:
        row["status"] = "skipped"
        row["reason"] = "not halted"
        return row
    # per-target fault isolation (PR #79 review): a failure on one
    # broadcast member must not abort the rest of the fleet
    try:
        db.set_session_halted(sid, halt, path=args.db)
        kind = NotificationKind.HALT if halt else NotificationKind.RESUME
        body = _HALT_BODY if halt else _RESUME_BODY
        if args.reason:
            body += f" ({args.reason})"
        row["notification_id"] = db.notify(sid, kind, body, path=args.db)
        try:
            row["wake"] = wake_session(sid, _session_cwd(sid, args))
        except WakeError as e:
            row["wake"] = f"failed:{e.reason}"
        row["mailbox_files"] = _mailbox_file_count(args, sid)
        task = db.get_live_task(sid, path=args.db)
        row["live_task"] = task["id"] if task is not None else None
        row["halted"] = halt
        row["status"] = "halted" if halt else "resumed"
    except Exception as e:
        row["status"] = "error"
        row["reason"] = f"{type(e).__name__}: {e}"
        try:
            # the flag may have been applied before the failing step
            # (e.g. notify raised after set_session_halted) - report
            # the true durable state, not just the exception
            row["halted"] = db.get_session(sid, path=args.db)["halted_at"] is not None
        except Exception:
            pass
    return row


def _cmd_halt(args) -> int:
    """``bridge halt <sid>|--all [--stop-daemon]``: graceful fleet stop.

    Sets the durable halted flag, enqueues the HALT semantic
    notification and best-effort wakes each target; ``--all`` adds a
    fleet snapshot (who is mid-turn, whose mail is undrained, which
    tasks are in flight and will resume) and ``--stop-daemon`` SIGTERMs
    the relay loop afterwards.
    """
    sids, rc = _halt_targets(args)
    if rc is not None:
        return rc
    rows = [_halt_apply(args, sid, True) for sid in sids]
    data: dict = {"sessions": rows}
    daemon_state = None
    if getattr(args, "stop_daemon", False):
        sdir = _state_dir(args)
        daemon_state = _ops._sigterm_daemon(sdir)
        data["daemon"] = daemon_state
        if daemon_state in ("stopped", "not_running"):
            # review fix (PR #79): a SIGTERM right after the outbox
            # write would kill the relay before the HALT rows ever
            # materialize as mailbox files - the woken sessions would
            # drain nothing and skip the save-state step. Relay them
            # synchronously now that the lock is free. The janitor
            # rides along (issue #119): a stale RESUME sitting in a
            # mailbox must not survive the fresh HALT.
            try:
                now = int(time.time())
                daemon._relay_pass(args.db, _queue_dir(args), sdir, now)
                daemon._stale_control_pass(args.db, _queue_dir(args), sdir, now)
                data["relayed"] = True
            except Exception as e:
                # state is already safe (flag set + outbox rows exist);
                # report instead of tracebacking past the JSON emit
                data["relay_error"] = f"{type(e).__name__}: {e}"
    else:
        # same delivery guarantee without --stop-daemon (issue #119):
        # with no loop running, materialize the HALT + drop stale
        # control files now or a drain meets only the stale epoch
        try:
            if _sync_control_delivery(args, _state_dir(args)):
                data["relayed"] = True
        except Exception as e:
            data["relay_error"] = f"{type(e).__name__}: {e}"
    resp = JsonResponse(status="ok", data=data)
    lines = [
        f"halt {r['session_id']}: {r['status']}"
        + (f" ({r['reason']})" if r.get("reason") else "")
        + (f" wake={r['wake']}" if r.get("wake") else "")
        + (
            f" mail={r['mailbox_files']} task=#{r['live_task']}"
            if r.get("status") == "halted"
            else ""
        )
        for r in rows
    ]
    if daemon_state is not None:
        lines.append(f"daemon {daemon_state}")
    return _emit(args, resp, ExitCode.OK, "\n".join(lines))


def _cmd_resume(args) -> int:
    """``bridge resume <sid>|--all``: lift the fleet halt.

    Clears halted_at, enqueues the RESUME semantic and wakes the
    session so queued work flows again.
    """
    sids, rc = _halt_targets(args)
    if rc is not None:
        return rc
    rows = [_halt_apply(args, sid, False) for sid in sids]
    data: dict = {"sessions": rows}
    # daemon-down delivery parity (issue #119): without a loop the
    # RESUME row would sit in the outbox while a stale HALT file
    # still waits to execute at the next drain - relay + prune now
    try:
        if _sync_control_delivery(args, _state_dir(args)):
            data["relayed"] = True
    except Exception as e:
        data["relay_error"] = f"{type(e).__name__}: {e}"
    resp = JsonResponse(status="ok", data=data)
    lines = [
        f"resume {r['session_id']}: {r['status']}"
        + (f" ({r['reason']})" if r.get("reason") else "")
        + (f" wake={r['wake']}" if r.get("wake") else "")
        for r in rows
    ]
    return _emit(args, resp, ExitCode.OK, "\n".join(lines))


def _cmd_kill(args) -> int:
    """``bridge kill <sid|task_id>``: force-terminate a session (task #210)
    or force-fail one live task (issue #113).

    A purely numeric positional addresses a task; anything else is a
    session id (sids are generated word-word names, never numeric).
    ``isascii`` joins ``isdigit`` because unicode digits like '²'
    pass isdigit yet explode in ``int()`` (review F1), and values
    past int64 would raise OverflowError at the SQLite bind, so
    oversized numerics short-circuit to a session not_found instead
    of a traceback (review r2 F1). The 19-char bound runs first:
    ``int()`` itself raises ValueError on digit strings past
    ``sys.get_int_max_str_digits()`` (Python 3.11+).
    """
    if (
        len(args.session_id) <= 19
        and args.session_id.isascii()
        and args.session_id.isdigit()
        and int(args.session_id) < 2**63
    ):
        return _cmd_kill_task(args)
    return _cmd_kill_session(args)


def _cmd_kill_task(args) -> int:
    """``bridge kill <task_id>``: operator force-fail of one live task.

    CAS ``running|dispatch_decided > failed`` ignoring ``lease_owner``
    (``db.kill_task``): the wedged step dies without waiting out the
    lease or tearing the session down with it. The owning session's
    NOTICE commits inside the same transaction (``verdict.notice_id``),
    so its mailbox wakes it to drop the dead work - discovering the
    CAS loss at ``bridge finish`` would burn a full turn on a task
    already failed.
    """
    task_id = int(args.session_id)
    verdict = db.kill_task(task_id, path=args.db)
    if isinstance(verdict, TaskKillNotFound):
        resp = JsonResponse(
            session=None,
            status="not_found",
            error=f"no task {task_id}",
        )
        return _emit(args, resp, ExitCode.NOT_FOUND, f"not found: task {task_id}")
    if isinstance(verdict, TaskKillRefused):
        resp = JsonResponse(
            session=None,
            status="refused",
            error=f"task {task_id} is {verdict.status}, not running/dispatch_decided",
            agent_instruction=(
                "only running/dispatch_decided tasks force-fail; "
                "queued tasks are cancelled, terminal tasks are already done"
            ),
        )
        return _emit(
            args,
            resp,
            ExitCode.CONFLICT,
            f"refused: task {task_id} is {verdict.status}",
        )
    sid = verdict.session_id
    resp = JsonResponse(
        session=sid,
        status="ok",
        data={
            "killed": True,
            "task_id": task_id,
            "status_before": verdict.status_before,
            "notification_id": verdict.notice_id,
        },
    )
    human = f"killed task {task_id}: status {verdict.status_before}->failed" + (
        f", notice {verdict.notice_id} queued for {sid}" if verdict.notice_id else ""
    )
    return _emit(args, resp, ExitCode.OK, human)


def _run_sweep(args, sid: str, *, dry_run: bool) -> dict:
    """Layer 2 of the hard-stop design: the attributed orphan sweep.

    Resolves the acp-host lockfile pid, hands the bridge session
    registry (every registered sid + cwd feeds the foreign-session
    exclusion) to ``hard_stop.plan_and_sweep`` and reports honestly:
    ``no_lockfile``/``no_backend`` are sweep outcomes, not guesses.
    """
    out = {
        "sweep_status": "skipped",
        "swept": [],
        "candidates": [],
        "sweep_excluded": [],
        "acp_host_pid": None,
        "backend_pid": None,
    }
    lock_data = find_lockfile()
    if lock_data is None:
        out["sweep_status"] = "no_lockfile"
        return out
    registered = [(r["session_id"], r["cwd"]) for r in _all_session_rows(args)]
    res = hard_stop.plan_and_sweep(
        lock_pid=lock_data.get("pid"),
        sid=sid,
        session_cwd=_session_cwd(sid, args),
        mailbox_dir=mailbox_dir(sid, db_path=args.db, queue_dir=args.queue_dir),
        registered=registered,
        grace_seconds=_SWEEP_TERM_GRACE,
        dry_run=dry_run,
    )
    out["sweep_status"] = res["status"]
    out["swept"] = res["swept"]
    out["candidates"] = res["candidates"]
    out["sweep_excluded"] = res["skipped"]
    out["acp_host_pid"] = res.get("acp_host_pid")
    out["backend_pid"] = res.get("backend_pid")
    return out


def _run_stop_layers(args, sid: str, *, hard: bool, dry_run: bool) -> dict:
    """Layers 0-2 of the layered hard-stop (issue #164).

    Order is the escalation contract: ``session/cancel`` first (the
    cancel's own confirm window is the ~5s grace the design asks
    for), forced ``session/close`` next, the attributed process
    sweep last and only under ``--hard``. ``dry_run`` makes the whole
    ladder side-effect-free: layers 0-1 report ``dry_run`` (nothing
    is sent) and the sweep enumerates without signalling - the only
    safe way to preview a real stop on a live host.
    """
    report = {
        "hard": hard,
        "dry_run": dry_run,
        "cancel_outcome": "dry_run" if dry_run else None,
        "close_outcome": "dry_run" if dry_run else None,
    }
    if not dry_run:
        try:
            report["cancel_outcome"] = cancel_turn(sid, confirm_timeout=_STOP_CANCEL_CONFIRM)
        except WakeError as e:
            report["cancel_outcome"] = f"failed:{e.reason}"
        try:
            report["close_outcome"] = close_session(
                sid, force=True, confirm_timeout=_STOP_CLOSE_CONFIRM
            )
        except WakeError as e:
            report["close_outcome"] = f"failed:{e.reason}"
    report.update(
        _run_sweep(args, sid, dry_run=dry_run)
        if hard
        else {
            "sweep_status": "skipped",
            "swept": [],
            "candidates": [],
            "sweep_excluded": [],
            "acp_host_pid": None,
            "backend_pid": None,
        }
    )
    return report


def _stop_status(report: dict) -> str:
    """Fold per-layer outcomes into one honest verdict.

    ``stopped`` is claimed only when a layer OBSERVED the session
    gone or idle (never inferred from a sent request); ``requested``
    means at least one layer delivered without confirmation - or a
    dry-run enumeration is all that ran; ``error`` means every layer
    failed before delivery.
    """
    if report["dry_run"]:
        return "dry_run"
    outcomes = (report["cancel_outcome"], report["close_outcome"])
    if "confirmed" in outcomes or outcomes == ("idle", "idle"):
        return "stopped"
    if "idle" in outcomes or "requested" in outcomes or report["swept"]:
        return "requested"
    return "error"


def _stop_event_body(report: dict) -> str:
    swept = report["swept"]
    pids = ",".join(str(e.get("pid")) for e in swept[:20])
    return (
        f"cancel={report['cancel_outcome']} close={report['close_outcome']}"
        f" sweep={report['sweep_status']} swept_pids=[{pids}]"
        f" dry_run={report['dry_run']} status={_stop_status(report)}"
    )


def _cmd_stop(args) -> int:
    """``bridge stop <sid> [--hard] [--dry-run]``: layered session stop (issue #164).

    The session-level counterpart of ``bridge halt``: actually stops
    what the session is DOING instead of parking its bridge duty.
    Layers run in escalation order and each reports an honest
    outcome:

    - layer 0: ``session/cancel`` via acp-host (``idle`` /
      ``confirmed`` / ``requested`` / ``failed:<reason>``) - proven
      ~3s on a live busy session;
    - layer 1: forced ``session/close`` (``cognition.ai/force``) -
      the strongest session-scoped primitive the host exposes;
    - layer 2 (``--hard`` only): the attributed orphan sweep -
      SIGTERM -> grace -> SIGKILL for backend-tree processes
      attributable to this session (workspace cwd or explicit sid
      markers), with the backend, acp-host, MCP servers and foreign
      sessions excluded. ``--dry-run`` enumerates the would-be
      victims with reasons and kills nothing - and suppresses layers
      0-1 too, so a dry run never touches the live session.

    The bus row is untouched: ``stop`` is process/session level only.
    For the full teardown chain ``bridge kill <sid> --hard-stop``
    runs these layers first and ``db.kill_session`` LAST, folding the
    layer outcomes into the ``session_killed`` event. Every stop
    writes a ``session_stopped`` decision event carrying the
    per-layer outcomes.
    """
    sid = args.session_id
    if db.get_session(sid, path=args.db) is None:
        resp = JsonResponse(
            session=sid,
            status="not_found",
            error=f"no registration for {sid}",
        )
        return _emit(args, resp, ExitCode.NOT_FOUND, f"not registered: {sid}")
    report = _run_stop_layers(args, sid, hard=args.hard, dry_run=args.dry_run)
    status = _stop_status(report)
    db.record_event(sid, "session_stopped", _stop_event_body(report), path=args.db)
    data = {
        "cancel_outcome": report["cancel_outcome"],
        "close_outcome": report["close_outcome"],
        "swept": report["swept"],
        "sweep_status": report["sweep_status"],
        "candidates": report["candidates"],
        "sweep_excluded": report["sweep_excluded"],
        "status": status,
    }
    if report["backend_pid"] is not None:
        data["backend_pid"] = report["backend_pid"]
        data["acp_host_pid"] = report["acp_host_pid"]
    resp = JsonResponse(session=sid, status=status, data=data)
    swept = report["swept"]
    human = (
        f"stop {sid}: cancel={report['cancel_outcome']}"
        f" close={report['close_outcome']}"
        + (f" sweep={report['sweep_status']} swept={len(swept)}" if args.hard else "")
        + (" [dry-run]" if args.dry_run else "")
    )
    if swept:
        human += " pids=" + ",".join(str(e.get("pid")) for e in swept[:20])
    # every layer failed before delivery is a real failure, not an
    # ok-with-caveats: scripts read the exit code before the JSON
    rc = ExitCode.INTERNAL if status == "error" else ExitCode.OK
    return _emit(args, resp, rc, human)


def _cmd_kill_session(args) -> int:
    """``bridge kill <sid> [--hard-stop]``: force-terminate a session's
    live state (task #210, hard-stop option issue #164).

    The detach path for a session that is gone or must be cut loose
    NOW, without ``unregister``'s liveness refusals and row deletion:
    the session row flips to ``dead``, live tasks requeue with their
    leases released (``queued`` is the only claimable re-entry state
    - a resurrected session re-dispatches them through the normal
    pump/claim path), staged ``dispatch_decided`` rows fold back to
    ``queued``, and the heartbeat row is removed.
    Every effect lands inside one transaction together with a
    ``session_killed`` ``decision_events`` row, so the durable audit
    cannot separate from the effect.

    ``--hard-stop`` (issue #164) runs the layered stop first - turn
    cancel, forced ``session/close``, attributed orphan sweep - and
    the bus teardown LAST, so a failed sweep never leaves the
    registry claiming alive while the session is dead; the per-layer
    outcomes fold into the ``session_killed`` event body.
    ``--dry-run`` previews the layers and the sweep without touching
    the session or the bus row.
    """
    sid = args.session_id
    want_hard = getattr(args, "hard_stop", False)
    dry_run = getattr(args, "dry_run", False)
    stop_report = None
    if want_hard or dry_run:
        if db.get_session(sid, path=args.db) is None:
            resp = JsonResponse(
                session=sid,
                status="not_found",
                error=f"no registration for {sid}",
            )
            return _emit(args, resp, ExitCode.NOT_FOUND, f"not registered: {sid}")
        try:
            stop_report = _run_stop_layers(args, sid, hard=want_hard, dry_run=dry_run)
        except Exception as e:
            # teardown-last is the contract: an unexpected layer
            # failure must NOT strand the registry alive over a
            # half-killed tree - record the failure verbatim and
            # still run db.kill_session. Nothing is swallowed: the
            # exception lands in the JSON, the human line and the
            # session_killed event body.
            stop_report = {
                "hard": want_hard,
                "dry_run": dry_run,
                "cancel_outcome": "unknown",
                "close_outcome": "unknown",
                "sweep_status": "unknown",
                "swept": [],
                "candidates": [],
                "sweep_excluded": [],
                "acp_host_pid": None,
                "backend_pid": None,
                "stop_exception": f"{e.__class__.__name__}: {e}",
            }
        if dry_run:
            status = "dry_run"
            resp = JsonResponse(
                session=sid,
                status=status,
                data={
                    "cancel_outcome": stop_report["cancel_outcome"],
                    "close_outcome": stop_report["close_outcome"],
                    "swept": stop_report["swept"],
                    "sweep_status": stop_report["sweep_status"],
                    "candidates": stop_report["candidates"],
                    "sweep_excluded": stop_report["sweep_excluded"],
                    "status": status,
                },
            )
            if stop_report.get("stop_exception"):
                resp.data["stop_exception"] = stop_report["stop_exception"]
            human = (
                f"kill {sid} [dry-run]: cancel={stop_report['cancel_outcome']}"
                f" close={stop_report['close_outcome']}"
                f" sweep={stop_report['sweep_status']}"
                f" candidates={len(stop_report['candidates'])} - bus state untouched"
            )
            if stop_report.get("stop_exception"):
                human += f"; stop_exception={stop_report['stop_exception']}"
            return _emit(args, resp, ExitCode.OK, human)
    verdict = db.kill_session(sid, stop_report=stop_report, path=args.db)
    if isinstance(verdict, KillNotFound):
        resp = JsonResponse(
            session=sid,
            status="not_found",
            error=f"no registration for {sid}",
        )
        return _emit(args, resp, ExitCode.NOT_FOUND, f"not registered: {sid}")
    data = {
        "killed": True,
        "tasks_requeued": verdict.tasks_requeued,
        "staged_folded": verdict.staged_folded,
        "heartbeat_removed": verdict.heartbeat_removed,
        "status_before": verdict.status_before,
        "mode": verdict.mode,
    }
    if stop_report:
        data["stop"] = {
            "cancel_outcome": stop_report["cancel_outcome"],
            "close_outcome": stop_report["close_outcome"],
            "swept": stop_report["swept"],
            "sweep_status": stop_report["sweep_status"],
        }
        if stop_report.get("stop_exception"):
            data["stop"]["stop_exception"] = stop_report["stop_exception"]
    resp = JsonResponse(
        session=sid,
        status="ok",
        data=data,
    )
    human = (
        f"killed {sid}: status {verdict.status_before}->dead,"
        f" {verdict.tasks_requeued} task(s) requeued,"
        f" {verdict.staged_folded} staged folded,"
        f" heartbeat {'removed' if verdict.heartbeat_removed else 'absent'}"
    )
    if stop_report:
        human += (
            f"; stop layers: cancel={stop_report['cancel_outcome']}"
            f" close={stop_report['close_outcome']}"
            f" swept={len(stop_report['swept'])}"
        )
        if stop_report.get("stop_exception"):
            human += f" stop_exception={stop_report['stop_exception']}"
    return _emit(args, resp, ExitCode.OK, human)


def _cmd_withdraw(args) -> int:
    """``bridge withdraw <task_id>``: audited orchestrator scope-pull
    (issue #142).

    The benign counterpart of ``bridge kill <task_id>``: every
    still-actionable state (queued, dispatch_decided, running,
    awaiting_user, paused) folds to terminal ``cancelled`` with
    actor+reason journaled into ``task_withdrawn`` - the audit is
    authored by the withdrawing side, never impersonating the worker.
    Terminal rows (done/failed/cancelled) are refused, so a task that
    finished between read and write keeps its real outcome and a
    repeat withdraw reports the status instead of a duplicate event.
    The lease/step/quota slots clear in the same transaction - the
    session's live-task index slot frees atomically (no ghost slot).

    ``--stop-turn`` is an explicit, separate action: after the CAS
    commits, a best-effort ``session/cancel`` goes to the owning
    session via acp-host (empirically proven, issue #137). The reply
    distinguishes ``idle`` (nothing to stop), ``confirmed`` (turn
    observed stopped) and ``requested`` (delivered, unconfirmed) -
    it never claims the process died. A transport failure is
    reported, not hidden: the task is already cancelled, only the
    optional interrupt failed.
    """
    task_id = args.task_id
    verdict = db.withdraw_task(
        task_id,
        actor=args.actor,
        reason=args.reason,
        stop_turn=args.stop_turn,
        path=args.db,
    )
    if isinstance(verdict, TaskWithdrawNotFound):
        resp = JsonResponse(
            session=None,
            status="not_found",
            error=f"no task {task_id}",
        )
        return _emit(args, resp, ExitCode.NOT_FOUND, f"not found: task {task_id}")
    if isinstance(verdict, TaskWithdrawRefused):
        resp = JsonResponse(
            session=None,
            status="refused",
            error=f"task {task_id} is already {verdict.status}",
            agent_instruction=(
                "terminal tasks need no withdraw; a repeat withdraw of a"
                " cancelled task is this same refusal - the audit row from"
                " the first call stands"
            ),
        )
        return _emit(
            args,
            resp,
            ExitCode.CONFLICT,
            f"refused: task {task_id} is {verdict.status}",
        )
    sid = verdict.session_id
    data = {
        "withdrawn": True,
        "task_id": task_id,
        "status_before": verdict.status_before,
        "notification_id": verdict.notice_id,
    }
    human = f"withdrew task {task_id}: status {verdict.status_before}->cancelled" + (
        f", notice {verdict.notice_id} queued for {sid}" if verdict.notice_id else ""
    )
    if args.stop_turn:
        try:
            outcome = cancel_turn(sid) if sid else "idle"
        except WakeError as e:
            outcome = f"failed:{e.reason}"
        data["stop_turn"] = outcome
        human += f", turn-stop {outcome}"
    resp = JsonResponse(session=sid, status="ok", data=data)
    return _emit(args, resp, ExitCode.OK, human)


def _cmd_register(args) -> int:
    """``bridge register <sid>``: join the bus as worker/orchestrator.

    Orchestrator registrations go through the CAS singleton in
    ``db.register`` - a refused takeover (fresh-heartbeat holder or
    managed-active holder) maps to CONFLICT with a ``--force`` hint
    instead of a bare error.
    """
    result = db.register(
        args.session_id,
        args.workspace or os.getcwd(),
        role=args.role,
        force=args.force,
        path=args.db,
    )
    if not result.accepted:
        fresh = (
            result.heartbeat_age is not None and result.heartbeat_age < db.HEARTBEAT_STALE_SECONDS
        )
        if fresh:
            why = (
                f"orchestrator {result.holder} holds a fresh heartbeat"
                f" ({result.heartbeat_age}s ago)"
            )
        else:
            why = (
                f"orchestrator {result.holder} is a managed-active holder"
                " - heartbeat age cannot demote it"
            )
        resp = JsonResponse(
            session=args.session_id,
            status="refused",
            error=why,
            agent_instruction=("retry with --force to take over the orchestrator role"),
            data={
                "holder": result.holder,
                "heartbeat_age": result.heartbeat_age,
            },
        )
        human = f"refused: {why}; use --force to take over"
        return _emit(args, resp, ExitCode.CONFLICT, human)
    # a crash inside a previous unregister can leave the mailbox
    # parked as .unregistered-<sid>-* - merge it back so the new
    # registration drains its pre-crash mail (task #203 reaper)
    _reap_unregister_tombstones(
        result.session_id,
        mailbox_dir(result.session_id, db_path=args.db, queue_dir=args.queue_dir),
    )
    resp = JsonResponse(
        session=result.session_id,
        status="registered",
        data={"role": result.role, "generation": result.generation},
    )
    human = f"{result.session_id} registered as {result.role} (generation {result.generation})"
    return _emit(args, resp, ExitCode.OK, human)


def _cmd_ack(args) -> int:
    """``bridge ack``: advance the restart watermark after mailbox consume."""
    acked = db.ack_notifications(args.session, args.up_to, path=args.db)
    resp = JsonResponse(
        session=args.session,
        status="ok",
        data={"acked": acked, "up_to_id": args.up_to},
    )
    human = f"{args.session}: acked {acked} notification(s)"
    if args.up_to is not None:
        human += f" up to {args.up_to}"
    return _emit(args, resp, ExitCode.OK, human)


def _cmd_heartbeat(args) -> int:
    """``bridge heartbeat <sid>``: the liveness metronome (item_13g).

    Deliberately lighter than ``register``: no role change and no
    orchestrator CAS - a watcher cycle can keep its own session alive
    without ever taking the orchestrator role over. For unmanaged
    sessions it also revives a dead-flagged row (item_122): heartbeat
    is their dead-marking signal, so fresh heartbeat activity is
    authoritative recovery. Managed rows are not revived here - their
    dead verdict is session/list-derived, which a heartbeat cannot
    refute; they recover through ``register``. The bumped generation
    is returned so a holder detects supersession.
    """
    generation = db.heartbeat(args.session_id, path=args.db)
    resp = JsonResponse(
        session=args.session_id,
        status="ok",
        data={"generation": generation},
    )
    human = f"{args.session_id} heartbeat ok (generation {generation})"
    return _emit(args, resp, ExitCode.OK, human)


def _cmd_wake(args) -> int:
    """``bridge wake <sid>``: manual acp-host wake nudge (item_16).

    Attaches to the local acp-host and injects the fixed drain nudge
    into the session - the same path the daemon's ``_wake_pass`` uses.
    Bypasses the daemon cooldown but keeps the busy check: a session
    mid-turn is never prompted. ``--prompt`` overrides the nudge text.

    ``waiting_quota`` suppression (task #216): a session whose live
    task is parked on a provider rate limit is refused before the
    attach - poking it would spend the very quota that is exhausted.
    The daemon's quota pass lifts the park at ``quota_reset_at`` and
    fires the single control probe; ``bridge quota-resume`` overrides.
    """
    sid = args.session_id
    parked = db.waiting_quota_task(sid, int(time.time()), path=args.db)
    if parked is not None:
        reset = parked["quota_reset_at"]
        reset_iso = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(reset))
        resp = JsonResponse(
            session=sid,
            status="suppressed",
            data={"task_id": parked["id"], "quota_reset_at": reset},
            agent_instruction=(
                "do not retry before reset; the daemon quota pass probes once after reset_at"
            ),
        )
        return _emit(
            args,
            resp,
            ExitCode.OK,
            f"wake {sid}: suppressed: waiting_quota until {reset_iso}",
        )
    try:
        outcome = wake_session(
            sid,
            _session_cwd(sid, args),
            nudge=args.prompt or None,
        )
    except WakeError as e:
        resp = JsonResponse(
            session=sid,
            status="error",
            error=f"wake failed ({e.reason}): {e}",
        )
        return _emit(args, resp, ExitCode.CONFLICT, f"wake {sid}: {e.reason} - {e}")
    resp = JsonResponse(session=sid, status=outcome)
    if outcome == "busy":
        return _emit(
            args,
            resp,
            ExitCode.CONFLICT,
            f"wake {sid}: session is mid-turn - not prompted",
        )
    human = f"wake {sid}: prompt {outcome}"
    return _emit(args, resp, ExitCode.OK, human)
