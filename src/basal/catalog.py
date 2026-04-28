from __future__ import annotations

import re
import warnings
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import icechunk

from .entry import Entry
from .history import EVENT_KEY, collect_history
from .schema import validate

_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._\-/]*$")


def _parse_iso_dt(s: str) -> datetime:
    """Parse ISO 8601 string to UTC-aware datetime. Accepts year, year-month, or full date."""
    # fromisoformat doesn't handle year-only or year-month on any CPython version
    if len(s) == 4 and s.isdigit():
        return datetime(int(s), 1, 1, tzinfo=UTC)
    if len(s) == 7 and s[4] == "-":
        year, month = int(s[:4]), int(s[5:])
        return datetime(year, month, 1, tzinfo=UTC)
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


FACET_DENYLIST = frozenset(
    {
        "location",
        "description",
        "doi",
        "dataset_snapshot_id",
        EVENT_KEY,
    }
)
"""Fields excluded from facets() — high-cardinality, free-text, or internal."""


def _validate_name(name: str) -> None:
    if name == "main":
        raise ValueError("entry name 'main' is reserved for catalog metadata")
    if not _NAME_RE.match(name):
        raise ValueError(
            f"Invalid entry name {name!r}: must start with alphanumeric and "
            "contain only letters, digits, '.', '_', '-', '/'"
        )


def _strip_internal(meta: dict) -> dict:
    """Remove reserved ``__*`` keys from metadata shown to callers."""
    return {k: v for k, v in meta.items() if not k.startswith("__")}


def _derive_metadata_from_store(
    storage: icechunk.Storage,
    branch: str = "main",
    config: icechunk.RepositoryConfig | None = None,
    derive_extent: bool = False,
) -> dict:
    """Inspect a dataset store and return stable derived attrs + snapshot id."""
    from .inspect import inspect_store, stable_attrs

    info = inspect_store(
        storage, branch=branch, config=config, derive_extent=derive_extent
    )
    derived = stable_attrs(info)
    derived["dataset_snapshot_id"] = info["dataset_snapshot_id"]
    if "virtual_chunk_containers" in info:
        derived["virtual_chunk_containers"] = info["virtual_chunk_containers"]
    if config is not None:
        containers = config.virtual_chunk_containers
        if containers:
            derived["virtual_chunk_containers"] = list(containers.keys())
    return derived


class IcechunkCatalog:
    """Dataset catalog backed by a single Icechunk repository.

    Each registered dataset is an orphan-style branch whose HEAD snapshot
    carries the entry's metadata. Reads use ``inspect_repo_info`` for a
    single atomic fetch of all entries.
    """

    def __init__(self, repo: icechunk.Repository) -> None:
        self._repo = repo

    @classmethod
    def create(cls, storage: icechunk.Storage) -> IcechunkCatalog:
        repo = icechunk.Repository.create(storage)
        session = repo.writable_session("main")
        session.commit("init catalog", allow_empty=True)
        return cls(repo)

    @classmethod
    def open(cls, storage: icechunk.Storage) -> IcechunkCatalog:
        repo = icechunk.Repository.open(storage)
        return cls(repo)

    @classmethod
    def open_or_create(cls, storage: icechunk.Storage) -> IcechunkCatalog:
        if icechunk.Repository.exists(storage):
            return cls.open(storage)
        return cls.create(storage)

    # --- mutations ---

    def register(
        self,
        name: str,
        storage: icechunk.Storage,
        format: str = "icechunk",
        branch: str = "main",
        config: icechunk.RepositoryConfig | None = None,
        storage_config: dict | None = None,
        derive_extent: bool = False,
        **metadata: Any,
    ) -> None:
        """Register a dataset.

        Opens the store via ``storage`` (icechunk validates it exists), then
        auto-extracts CF attrs, dataset_snapshot_id, virtual_chunk_containers,
        location, and storage_config. Explicit kwargs win over derived attrs.

        Parameters
        ----------
        storage:
            icechunk.Storage for the dataset store. Use icechunk.s3_storage(),
            icechunk.local_filesystem_storage(), etc. to build.
        storage_config:
            Optional serializable dict overriding the auto-derived storage config.
            Useful for private stores where from_env credentials must be recorded
            explicitly to enable no-arg to_xarray() at read time.
        config:
            Optional icechunk.RepositoryConfig. Required for stores with virtual
            chunks — basal serializes the VirtualChunkContainer settings so
            to_xarray() can reconstruct config and credentials automatically.
            Build with icechunk.RepositoryConfig and set_virtual_chunk_container().
        derive_extent:
            If True, read coordinate arrays to auto-populate ``bbox``,
            ``start_datetime``, and ``end_datetime``. Reads 1-D coord arrays
            only — no chunk data. Explicit kwargs still win.
        **metadata:
            Arbitrary metadata fields. Common optional fields: owner, title,
            license, tags. Pass location= to override the auto-derived URL.
        """
        from .storage import (
            _virtual_chunk_container_to_config,
            storage_to_config,
            storage_to_location,
        )

        _validate_name(name)

        derived = _derive_metadata_from_store(
            storage, branch=branch, config=config, derive_extent=derive_extent
        )

        derived_storage_config = storage_config or storage_to_config(storage)
        derived_location = metadata.pop("location", None) or storage_to_location(
            storage
        )

        # Serialize VirtualChunkContainer details from config when provided.
        virtual_chunk_containers_config = None
        if config is not None:
            containers = config.virtual_chunk_containers
            if containers:
                virtual_chunk_containers_config = [
                    _virtual_chunk_container_to_config(vc) for vc in containers.values()
                ]

        entry_meta: dict[str, Any] = {
            "location": derived_location,
            "format": format,
            **derived,
            **metadata,
        }
        if derived_storage_config:
            entry_meta["storage_config"] = derived_storage_config
        if virtual_chunk_containers_config is not None:
            entry_meta["virtual_chunk_containers_config"] = (
                virtual_chunk_containers_config
            )
        validate(entry_meta)

        if name in self._repo.list_branches():
            raise ValueError(
                f"Dataset '{name}' already registered. Use deregister first."
            )

        main_snap = self._repo.lookup_branch("main")
        self._repo.create_branch(name, main_snap)
        session = self._repo.writable_session(name)
        session.commit(
            f"register {name}",
            metadata={**entry_meta, EVENT_KEY: "registered"},
            allow_empty=True,
        )

    def register_or_update(
        self,
        name: str,
        storage: icechunk.Storage,
        format: str = "icechunk",
        branch: str = "main",
        config: icechunk.RepositoryConfig | None = None,
        storage_config: dict | None = None,
        derive_extent: bool = False,
        **metadata: Any,
    ) -> str:
        """Register a dataset, or update its metadata if already registered.

        Accepts the same arguments as register(). Returns "registered" or
        "updated" — useful for logging in bulk registration scripts.
        """
        try:
            self.register(
                name,
                storage=storage,
                format=format,
                branch=branch,
                config=config,
                storage_config=storage_config,
                derive_extent=derive_extent,
                **metadata,
            )
            return "registered"
        except ValueError as e:
            if "already registered" in str(e):
                self.update(name, **metadata)
                return "updated"
            raise

    def update(self, name: str, **fields: Any) -> None:
        """Merge ``fields`` into the current metadata (new values win)."""
        entry = self.get(name)
        merged = {**entry.metadata, **fields}
        validate(merged)

        session = self._repo.writable_session(name)
        session.commit(
            f"update {name}",
            metadata={**merged, EVENT_KEY: "updated"},
            allow_empty=True,
        )

    def update_from_store(
        self,
        name: str,
        branch: str = "main",
        storage: icechunk.Storage | None = None,
        config: icechunk.RepositoryConfig | None = None,
        derive_extent: bool = False,
        **fields: Any,
    ) -> None:
        """Refresh stable CF attrs + ``dataset_snapshot_id`` from the live store.

        Explicit ``fields`` are applied on top of freshly derived attrs.
        If ``storage`` is not provided, reconstructs from entry's stored
        storage_config (requires entry was registered with storage_config=).
        Pass ``config`` to update virtual_chunk_container prefixes in metadata.
        Pass ``derive_extent=True`` to also refresh bbox and temporal bounds.
        """
        entry = self.get(name)
        resolved = entry._resolve_storage(storage)
        derived = _derive_metadata_from_store(
            resolved, branch=branch, config=config, derive_extent=derive_extent
        )
        self.update(name, **{**derived, **fields})

    def deregister(self, name: str) -> None:
        self._repo.delete_branch(name)

    # --- reads ---

    def get(self, name: str) -> Entry:
        snapshot_id = self._repo.lookup_branch(name)
        info = self._repo.lookup_snapshot(snapshot_id)
        return Entry(
            name=name,
            snapshot_id=snapshot_id,
            metadata=_strip_internal(info.metadata or {}),
            written_at=info.written_at,
        )

    def list(self) -> list[Entry]:
        # inspect_repo_info fetches all branches + snapshot metadata in one call,
        # avoiding O(N) lookup_branch + lookup_snapshot round trips.
        info = self._repo.inspect_repo_info()
        snaps_by_id = {s["id"]: s for s in info["snapshots"]}
        entries = []
        for name, snap_id in info["branches"].items():
            if name == "main":
                continue
            snap = snaps_by_id.get(snap_id, {})
            meta = snap.get("metadata", {})
            if not meta.get("location"):
                continue
            entries.append(
                Entry(
                    name=name,
                    snapshot_id=snap_id,
                    metadata=_strip_internal(meta),
                    written_at=snap.get("flushed_at"),
                )
            )
        return entries

    def history(self, name: str | None = None, limit: int = 100) -> list[dict]:
        """Return catalog operation history, newest first. See ``history.collect_history``."""
        return collect_history(self._repo, name=name, limit=limit)

    # --- search ---

    def search(self, query: str) -> list[tuple]:
        """Run DuckDB SQL over entries(name, snapshot_id, metadata JSON)."""
        from .search import sql

        return sql(self, query)

    def similar_to(
        self,
        name: str,
        n: int = 5,
        embed_fn: Callable[[list[str]], Any] | None = None,
    ) -> list[tuple[Entry, float]]:
        """Find entries most similar to ``name``, excluding ``name`` itself."""
        from .search import _entry_text, similar

        entry = self.get(name)
        query = _entry_text(entry)
        results = similar(self, query, embed_fn=embed_fn, top_k=n + 1)
        return [(e, s) for e, s in results if e.name != name][:n]

    # --- field discovery ---

    def fields(self) -> set[str]:
        """Return union of all metadata keys across entries."""
        out: set[str] = set()
        for e in self.list():
            out.update(e.metadata.keys())
        return out

    def values(self, field: str) -> list[Any]:
        """Distinct values for ``field``, list-valued fields flattened."""
        hashable_seen: set = set()
        unhashable_seen: list = []
        ordered: list[Any] = []
        for e in self.list():
            v = e.metadata.get(field)
            if v is None:
                continue
            items = v if isinstance(v, (list | tuple)) else [v]
            for item in items:
                try:
                    if item in hashable_seen:
                        continue
                    hashable_seen.add(item)
                except TypeError:
                    if item in unhashable_seen:
                        continue
                    unhashable_seen.append(item)
                ordered.append(item)
        return ordered

    def facets(self) -> dict[str, Counter]:
        """``{field: Counter(value -> freq)}`` for scalar + list-valued fields.

        Excludes high-cardinality / free-text fields listed in ``FACET_DENYLIST``.
        """
        out: dict[str, Counter] = {}
        for e in self.list():
            for k, v in e.metadata.items():
                if k in FACET_DENYLIST:
                    continue
                items = v if isinstance(v, (list | tuple)) else [v]
                for item in items:
                    if isinstance(item, (str | int | float | bool)):
                        out.setdefault(k, Counter())[item] += 1
        return out

    # --- filter ---

    def filter(
        self,
        *,
        time_start: str | None = None,
        time_end: str | None = None,
        bbox: tuple[float, float, float, float] | None = None,
    ) -> list[Entry]:
        """Return entries matching optional temporal and/or spatial bounds.

        Fields used: ``start_datetime`` / ``end_datetime`` (ISO 8601) and
        ``bbox`` ([west, south, east, north] WGS84) — matching STAC conventions.
        Entries missing a queried field are excluded and a warning is issued.

        Parameters
        ----------
        time_start:
            ISO 8601 string (e.g. ``"2020"`` or ``"2020-06-01"``), or ``"*"``
            for an open lower bound. Filter excludes entries whose coverage
            ends before this date.
        time_end:
            ISO 8601 string or ``"*"`` for an open upper bound. Filter
            excludes entries that start after this date.
        bbox:
            ``(west, south, east, north)`` in WGS84 decimal degrees. Entries
            must spatially intersect this rectangle.
        """
        do_temporal = time_start is not None or time_end is not None
        do_spatial = bbox is not None

        if not do_temporal and not do_spatial:
            return self.list()

        t_start = (
            _parse_iso_dt(time_start) if time_start and time_start != "*" else None
        )
        t_end = _parse_iso_dt(time_end) if time_end and time_end != "*" else None

        _EPOCH = datetime(1, 1, 1, tzinfo=UTC)
        _FAR_FUTURE = datetime(9999, 12, 31, tzinfo=UTC)

        results: list[Entry] = []
        temporal_missing: list[str] = []
        spatial_missing: list[str] = []

        for entry in self.list():
            if do_temporal:
                e_start_raw = entry.metadata.get("start_datetime")
                e_end_raw = entry.metadata.get("end_datetime")
                if e_start_raw is None and e_end_raw is None:
                    temporal_missing.append(entry.name)
                    continue
                e_start = _parse_iso_dt(e_start_raw) if e_start_raw else _EPOCH
                e_end = _parse_iso_dt(e_end_raw) if e_end_raw else _FAR_FUTURE
                # overlap: entry interval intersects filter interval
                if t_end is not None and e_start > t_end:
                    continue
                if t_start is not None and e_end < t_start:
                    continue

            if do_spatial:
                e_bbox = entry.metadata.get("bbox")
                if e_bbox is None:
                    spatial_missing.append(entry.name)
                    continue
                ew, es, ee, en = e_bbox
                fw, fs, fe, fn = bbox
                if ee <= fw or ew >= fe or en <= fs or es >= fn:
                    continue

            results.append(entry)

        if temporal_missing:
            warnings.warn(
                f"{len(temporal_missing)} entr{'y' if len(temporal_missing) == 1 else 'ies'} "
                f"skipped — no start_datetime/end_datetime: {temporal_missing}. "
                "Add with: catalog.update(name, start_datetime='2020-01-01', end_datetime='2023-12-31')",
                stacklevel=2,
            )
        if spatial_missing:
            warnings.warn(
                f"{len(spatial_missing)} entr{'y' if len(spatial_missing) == 1 else 'ies'} "
                f"skipped — no bbox: {spatial_missing}. "
                "Add with: catalog.update(name, bbox=[west, south, east, north])",
                stacklevel=2,
            )

        return results

    # --- bulk maintenance ---

    def refresh(self) -> dict[str, bool]:
        """Re-run ``is_stale()`` across all entries. Returns ``{name: bool}``.

        Entries missing ``dataset_snapshot_id`` or ``storage_config`` are
        excluded and a warning is issued listing them with remediation hints.
        """
        results: dict[str, bool] = {}
        skipped: list[str] = []

        for entry in self.list():
            try:
                results[entry.name] = entry.is_stale()
            except ValueError:
                skipped.append(entry.name)

        if skipped:
            warnings.warn(
                f"{len(skipped)} entr{'y' if len(skipped) == 1 else 'ies'} "
                f"skipped — missing dataset_snapshot_id or storage_config: {skipped}. "
                "Run catalog.update_from_store(name) to enable staleness checks.",
                stacklevel=2,
            )

        return results

    def update_all_from_store(self, branch: str = "main") -> None:
        """Refresh ``dataset_snapshot_id`` and CF attrs for all entries from their live stores.

        Entries without a stored ``storage_config`` are skipped with a warning
        — pass ``storage=`` explicitly to ``update_from_store()`` for those.
        """
        skipped: list[str] = []

        for entry in self.list():
            try:
                self.update_from_store(entry.name, branch=branch)
            except ValueError:
                skipped.append(entry.name)

        if skipped:
            warnings.warn(
                f"{len(skipped)} entr{'y' if len(skipped) == 1 else 'ies'} "
                f"skipped — no storage_config: {skipped}. "
                "Pass storage= explicitly: catalog.update_from_store(name, storage=...)",
                stacklevel=2,
            )

    # --- pretty printing ---

    def describe(self, name: str) -> None:
        """Print a rich-formatted description of a catalog entry."""
        from rich.console import Console
        from rich.panel import Panel
        from rich.table import Table

        entry = self.get(name)
        table = Table(show_header=False, box=None, pad_edge=False)
        table.add_column("field", style="bold cyan")
        table.add_column("value")

        for k, v in entry.metadata.items():
            table.add_row(k, str(v))
        table.add_row("snapshot_id", f"[dim]{entry.snapshot_id}[/dim]")
        table.add_row("written_at", str(entry.written_at))

        Console().print(
            Panel(table, title=f"[bold]{entry.name}[/bold]", border_style="blue")
        )

    def print(self) -> None:
        """Print all catalog entries as a rich table."""
        from rich.console import Console
        from rich.table import Table

        entries = sorted(self.list(), key=lambda e: e.name)
        table = Table(title=f"IcechunkCatalog ({len(entries)} entries)")
        table.add_column("name", style="bold")
        table.add_column("owner")
        table.add_column("title")
        table.add_column("location", style="dim")

        for e in entries:
            table.add_row(
                e.name,
                e.owner,
                e.metadata.get("title", ""),
                e.location,
            )
        Console().print(table)

    def __repr__(self) -> str:
        n = len([b for b in self._repo.list_branches() if b != "main"])
        return f"<IcechunkCatalog with {n} entries>"

    def _repr_html_(self) -> str:
        entries = sorted(self.list(), key=lambda e: e.name)
        rows = "".join(
            f"<tr><td><b>{e.name}</b></td><td>{e.owner}</td>"
            f"<td>{e.metadata.get('title', '')}</td>"
            f"<td><code>{e.location}</code></td></tr>"
            for e in entries
        )
        return (
            f"<table><thead><tr>"
            f"<th colspan=4>IcechunkCatalog ({len(entries)} entries)</th></tr>"
            f"<tr><th>name</th><th>owner</th><th>title</th><th>location</th></tr>"
            f"</thead><tbody>{rows}</tbody></table>"
        )
