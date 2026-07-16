"""Federated (multi-catalog) read layer.

A ``FederatedCatalog`` presents a union of member ``Catalog`` objects behind the
same read API (``list``/``get``/``filter``/``facets``/``search``/``sql`` …). It
is read-only by design: it never writes, syncs, or owns metadata — each member
stays its own source of truth and keeps its own register/update/expire lifecycle.

Two ways to combine:
- Live (this class): lazy union, always fresh, one metadata GET per member.
- Persistent: ``materialize()`` snapshots the union into a new standalone
  ``Catalog`` (an export convenience built on ``register()``).

Entries are namespaced by member alias (``alias/name``) so names stay globally
unique and ``Entry.source`` records provenance. The alias defaults to the
member's own ``catalog.name`` (catalog-level identity stored on its ``main``
HEAD); pass an explicit alias to override or when a member is unnamed.
"""

from __future__ import annotations

import warnings
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from typing import TYPE_CHECKING

from .catalog import Catalog, CatalogReadMixin
from .entry import Entry

if TYPE_CHECKING:
    import icechunk

    from .storage import StorageSpec


def _alias_for(catalog: Catalog) -> str:
    name = catalog.name
    if not name:
        raise ValueError(
            "Catalog has no name to use as a federation alias. Either set one "
            "(Catalog.create(storage, name=...) or catalog.set_info(name=...)) "
            "or pass an explicit alias, e.g. FederatedCatalog({'alias': catalog})."
        )
    return name


def _normalize_members(
    members: dict[str, Catalog] | list[Catalog] | list[tuple[str, Catalog]],
) -> dict[str, Catalog]:
    """Coerce the accepted member forms into an ``{alias: Catalog}`` dict."""
    out: dict[str, Catalog] = {}
    items: list[tuple[str, Catalog]]
    if isinstance(members, dict):
        items = list(members.items())
    else:
        items = []
        for m in members:
            if isinstance(m, tuple):
                items.append(m)
            else:
                items.append((_alias_for(m), m))
    for alias, cat in items:
        if "/" in alias:
            raise ValueError(
                f"Member alias {alias!r} may not contain '/': aliases are the "
                "first path segment of a federated name, so a slash would make "
                "get() route ambiguously."
            )
        if alias in out:
            raise ValueError(f"Duplicate member alias {alias!r}")
        out[alias] = cat
    return out


class FederatedCatalog(CatalogReadMixin):
    """A read-only union of member ``Catalog`` objects, keyed by alias."""

    def __init__(
        self,
        members: dict[str, Catalog] | list[Catalog] | list[tuple[str, Catalog]],
        *,
        max_workers: int = 8,
        strict: bool = True,
    ) -> None:
        self._members = _normalize_members(members)
        self._max_workers = max_workers
        self._strict = strict

    @classmethod
    def open(
        cls,
        members: dict[str, StorageSpec | icechunk.Storage]
        | list[StorageSpec | icechunk.Storage],
        *,
        readonly: bool = True,
        max_workers: int = 8,
        strict: bool = True,
    ) -> FederatedCatalog:
        """Open each member from its storage, then federate.

        ``members`` maps alias -> storage, or is a bare list of storages (alias
        then defaults to each catalog's ``name``). Members open ``readonly`` by
        default — federation is a discovery layer, not a writer. ``strict``
        controls member-failure handling at read time (see ``list``).
        """
        opened: dict[str, Catalog] | list[Catalog]
        if isinstance(members, dict):
            opened = {a: Catalog.open(s, readonly=readonly) for a, s in members.items()}
        else:
            opened = [Catalog.open(s, readonly=readonly) for s in members]
        return cls(opened, max_workers=max_workers, strict=strict)

    # --- membership ---

    @property
    def members(self) -> dict[str, Catalog]:
        return dict(self._members)

    def add(self, alias: str, catalog: Catalog) -> None:
        if "/" in alias:
            raise ValueError(f"Member alias {alias!r} may not contain '/'")
        if alias in self._members:
            raise ValueError(f"Member alias {alias!r} already present")
        self._members[alias] = catalog

    def remove(self, alias: str) -> None:
        del self._members[alias]

    # --- reads (CatalogReadMixin requires list + get) ---

    def list(self) -> list[Entry]:
        """Union of every member's entries, namespaced ``alias/name``.

        When ``strict`` (the default) a failing member raises and aborts the
        union. When ``strict=False`` a failing member is skipped with a warning
        so a single unreachable catalog doesn't blind discovery of the rest.
        """

        def _member_entries(
            item: tuple[str, Catalog],
        ) -> tuple[str, list[Entry] | None, Exception | None]:
            alias, cat = item
            try:
                entries = cat.list()
            except Exception as err:  # noqa: BLE001 - reported per strict policy
                return alias, None, err
            namespaced = [
                replace(e, name=f"{alias}/{e.name}", source=alias) for e in entries
            ]
            return alias, namespaced, None

        items = list(self._members.items())
        if not items:
            return []
        with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            results = list(pool.map(_member_entries, items))
        out: list[Entry] = []
        for alias, entries, err in results:
            if err is not None:
                if self._strict:
                    raise RuntimeError(f"member {alias!r} failed list()") from err
                warnings.warn(
                    f"federation member {alias!r} skipped — list() failed: {err}",
                    stacklevel=2,
                )
                continue
            out.extend(entries or [])
        return out

    def get(self, name: str, include_deleted: bool = False) -> Entry:
        alias, sep, sub = name.partition("/")
        if not sep or alias not in self._members:
            raise KeyError(
                f"Federated name {name!r} must be '<alias>/<entry>' with a known "
                f"alias. Known aliases: {sorted(self._members)}"
            )
        e = self._members[alias].get(sub, include_deleted=include_deleted)
        return replace(e, name=name, source=alias)

    # --- persistent merge ---

    def materialize(self, storage: icechunk.Storage, *, create: bool = True) -> Catalog:
        """Snapshot the union into a new standalone ``Catalog`` at ``storage``.

        Re-registers every entry (with ``inspect=False``, reusing each entry's
        stored ``storage_config``/``store_config`` — no store IO). Names stay
        namespaced (``alias/entry``) to preserve provenance, and each member's
        catalog-level identity (``name``/``owner``/``description``/…) is recorded
        under the target's ``info["sources"][alias]``. The result is a
        point-in-time copy and its own source of truth: it does not track the
        members afterward. Heavy on object storage (one commit per entry), so
        treat it as a publish/export step, not the default.
        """
        from .storage import storage_from_config

        target = Catalog.create(storage) if create else Catalog.open_or_create(storage)

        sources = {alias: cat.info for alias, cat in self._members.items()}
        if sources:
            target.set_info(sources=sources)

        for e in self.list():
            meta = e.metadata
            if e.format == "zarr":
                extra = {
                    k: v
                    for k, v in meta.items()
                    if k not in {"format", "location", "store_config", "group"}
                }
                target.register_zarr(
                    e.name,
                    meta["location"],
                    store_config=meta.get("store_config"),
                    inspect=False,
                    group=meta.get("group"),
                    **extra,
                )
            else:
                sc = meta.get("storage_config")
                storage_obj = storage_from_config(sc) if sc else None
                extra = {
                    k: v
                    for k, v in meta.items()
                    if k not in {"format", "storage_config", "group"}
                }
                # location rides through **extra (register pops it).
                target.register(
                    e.name,
                    storage=storage_obj,
                    format=meta.get("format", "icechunk"),
                    storage_config=sc,
                    inspect=False,
                    group=meta.get("group"),
                    **extra,
                )
        return target

    def __repr__(self) -> str:
        return (
            f"<FederatedCatalog {sorted(self._members)} "
            f"with {len(self.list())} entries>"
        )
