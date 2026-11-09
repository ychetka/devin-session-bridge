"""Typed verdicts for db->caller results (task #203, item_37a).

The db layer historically answered compound operations with
stringly-typed dicts (``None`` | ``{"refused": "live_task", ...}`` |
a counts bag): the contract was implicit, validated by nothing, and
checked by string comparison at call sites scattered through the
CLI. Verdicts returned from db functions are pydantic tagged unions
- the ``kind`` discriminator is the machine-checked contract, and
the caller's dispatch is ``isinstance``/exhaustive ``match`` instead
of ``counts.get("refused") == "live_task"``.

First consumer: ``unregister_session`` - its three refusal/not-found
shapes plus the success-counts shape are now
``UnregisterVerdict`` members. New compound returns belong here, not
in new ad-hoc dict shapes.
"""

from typing import Annotated, Literal

from pydantic import BaseModel, Field

__all__ = [
    "KillNotFound",
    "KillVerdict",
    "Killed",
    "ReleaseNotFound",
    "ReleaseRefusedOwner",
    "ReleaseRemoved",
    "ReleaseVerdict",
    "ReserveConflict",
    "ReserveGranted",
    "ReserveVerdict",
    "UnregisterDeleted",
    "UnregisterNotFound",
    "UnregisterRefusedFreshHeartbeat",
    "UnregisterRefusedLiveTask",
    "UnregisterVerdict",
]


class _Verdict(BaseModel):
    """Shared base: ``kind`` is the tag, extra wire fields are free."""

    kind: str


class UnregisterNotFound(_Verdict):
    """The sid is not registered (unknown or concurrently removed)."""

    kind: Literal["not_found"] = "not_found"


class UnregisterRefusedLiveTask(_Verdict):
    """A live task still owns the lifecycle; no force override."""

    kind: Literal["refused_live_task"] = "refused_live_task"
    task_id: int
    task_status: str


class UnregisterRefusedFreshHeartbeat(_Verdict):
    """A heartbeat younger than the stale threshold = live client."""

    kind: Literal["refused_fresh_heartbeat"] = "refused_fresh_heartbeat"
    age_seconds: int


class UnregisterDeleted(_Verdict):
    """All session-scoped rows deleted; per-table row counts."""

    kind: Literal["deleted"] = "deleted"
    counts: dict[str, int]


UnregisterVerdict = Annotated[
    UnregisterNotFound
    | UnregisterRefusedLiveTask
    | UnregisterRefusedFreshHeartbeat
    | UnregisterDeleted,
    Field(discriminator="kind"),
]


class ReserveGranted(_Verdict):
    """The (type, id) key is now owned by the requesting project.

    ``renewed`` is true when the same project already held the key -
    a re-reserve by the owner is an idempotent TTL/purpose touch, not
    a conflict.
    """

    kind: Literal["granted"] = "granted"
    renewed: bool
    expires_at: int | None


class ReserveConflict(_Verdict):
    """A different project holds the key; its row is echoed so the
    caller can report the existing owner - and on a wildcard
    ``systemd_unit_prefix`` conflict, so the orchestrator's
    ``release --force`` arbitration learns the incumbent's actual
    key without a second query."""

    kind: Literal["conflict"] = "conflict"
    owner: str
    resource_id: str
    purpose: str | None
    created_at: int
    expires_at: int | None


ReserveVerdict = Annotated[
    ReserveGranted | ReserveConflict,
    Field(discriminator="kind"),
]


class ReleaseRemoved(_Verdict):
    """The row is gone."""

    kind: Literal["removed"] = "removed"
    owner: str


class ReleaseNotFound(_Verdict):
    """No live allocation at that key (never held, or TTL-swept)."""

    kind: Literal["not_found"] = "not_found"


class ReleaseRefusedOwner(_Verdict):
    """A different project owns the key; ``--force`` is the override
    the orchestrator uses when arbitrating a contested resource."""

    kind: Literal["refused_owner"] = "refused_owner"
    owner: str


ReleaseVerdict = Annotated[
    ReleaseRemoved | ReleaseNotFound | ReleaseRefusedOwner,
    Field(discriminator="kind"),
]


class KillNotFound(_Verdict):
    """The sid is not registered - nothing to detach."""

    kind: Literal["not_found"] = "not_found"


class Killed(_Verdict):
    """Live bridge state detached: session dead, leases released,
    live tasks demoted, heartbeat row removed."""

    kind: Literal["killed"] = "killed"
    tasks_demoted: int
    heartbeat_removed: bool
    status_before: str
    mode: str | None


KillVerdict = Annotated[
    KillNotFound | Killed,
    Field(discriminator="kind"),
]
