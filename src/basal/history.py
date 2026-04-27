"""Catalog operation history built on icechunk's ops_log.

Events are identified via the reserved ``EVENT_KEY`` stored in commit
metadata at write time. Falls back to commit-message prefix parsing for
entries committed before the reserved key was introduced.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import icechunk


EVENT_KEY = "__event__"
"""Reserved key in snapshot metadata identifying the catalog event type."""


def collect_history(
    repo: icechunk.Repository,
    name: str | None = None,
    limit: int = 100,
) -> list[dict]:
    """Return catalog operation history, newest first.

    Events: ``registered``, ``updated``, ``deregistered``.
    """
    from icechunk.ops import UpdateType

    records: list[dict] = []
    for update in repo.ops_log():
        if len(records) >= limit:
            break

        k = update.kind
        entry_name: str | None = None
        event: str | None = None
        snapshot_id: str | None = None

        if isinstance(k, UpdateType.NewCommit):
            if k.branch == "main":
                continue
            entry_name = k.branch
            snapshot_id = k.new_snap_id
            info = repo.lookup_snapshot(snapshot_id)
            meta = info.metadata or {}
            if EVENT_KEY in meta:
                event = meta[EVENT_KEY]
            else:
                msg = getattr(info, "message", "") or ""
                event = "registered" if msg.startswith("register ") else "updated"

        elif isinstance(k, UpdateType.BranchDeleted):
            if k.name == "main":
                continue
            entry_name = k.name
            event = "deregistered"

        else:
            continue

        if name is not None and entry_name != name:
            continue

        records.append(
            {
                "event": event,
                "name": entry_name,
                "timestamp": update.updated_at,
                "snapshot_id": snapshot_id,
            }
        )

    return records
