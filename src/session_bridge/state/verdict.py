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
