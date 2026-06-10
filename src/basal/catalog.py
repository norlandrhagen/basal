from __future__ import annotations

import re
import warnings
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import icechunk

from .entry import Entry
from .history import EVENT_KEY, collect_history
from .schema import finalize

if TYPE_CHECKING:
    from .storage import StorageSpec

_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._\-/]*$")

DELETED_KEY = "__deleted__"
"""Marker committed at a branch HEAD to flag a deregistered (reversibly deleted) entry."""


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


def _derive_time_extent(
    storage: icechunk.Storage,
    branch: str = "main",
    config: icechunk.RepositoryConfig | None = None,
) -> dict:
    """Cheap refresh: read only the time coordinate for snapshot id + end_datetime.

    Skips full CF/variable inspection — used by the time-append fast path.
    """
    import xarray as xr

    from .inspect import (
        _TIME_NAMES,
        _find_coord,
        _np_dt_to_iso,
        suppress_numcodecs_warning,
    )

    kwargs: dict = {"config": config} if config is not None else {}
    repo = icechunk.Repository.open(storage, **kwargs)
    derived: dict = {"dataset_snapshot_id": repo.lookup_branch(branch)}
    session = repo.readonly_session(branch=branch)
    with suppress_numcodecs_warning():
        ds = xr.open_zarr(session.store, consolidated=False)
    time_da = _find_coord(ds, "time", _TIME_NAMES)
    if time_da is not None and time_da.size > 0:
        new_end = _np_dt_to_iso(time_da.values.max())
        if new_end is not None:
            derived["end_datetime"] = new_end
    return derived


class Catalog:
    """Dataset catalog backed by a single Icechunk repository.

    Each registered dataset is an orphan-style branch whose HEAD snapshot
    carries the entry's metadata. Reads use ``inspect_repo_info`` for a
    single atomic fetch of all entries.
    """

    def __init__(self, repo: icechunk.Repository, readonly: bool = False) -> None:
        self._repo = repo
        self._readonly = readonly

    @classmethod
    def create(cls, storage: icechunk.Storage) -> Catalog:
        repo = icechunk.Repository.create(storage)
        session = repo.writable_session("main")
        session.commit("init catalog", allow_empty=True)
        return cls(repo)

    @classmethod
    def open(cls, storage: icechunk.Storage, readonly: bool = False) -> Catalog:
        repo = icechunk.Repository.open(storage)
        return cls(repo, readonly=readonly)

    @classmethod
    def open_or_create(cls, storage: icechunk.Storage) -> Catalog:
        if icechunk.Repository.exists(storage):
            return cls.open(storage)
        try:
            return cls.create(storage)
        except icechunk.IcechunkError:
            # Concurrent creator won the race between exists() and create().
            if icechunk.Repository.exists(storage):
                return cls.open(storage)
            raise

    # --- mutations ---

    def _check_writable(self) -> None:
        if self._readonly:
            raise PermissionError(
                "Catalog was opened with readonly=True — mutations are disabled. "
                "Reopen with Catalog.open(storage) to write."
            )

    def register(
        self,
        name: str,
        storage: icechunk.Storage | StorageSpec,
        format: str = "icechunk",
        branch: str = "main",
        config: icechunk.RepositoryConfig | None = None,
        storage_config: dict | None = None,
        derive_extent: bool = False,
        inspect: bool = True,
        **metadata: Any,
    ) -> None:
        """Register a dataset.

        By default opens the store to auto-extract CF attrs, var_names,
        dataset_snapshot_id, and virtual_chunk_containers. Explicit kwargs
        always win over derived attrs.

        Parameters
        ----------
        storage:
            A basal.storage StorageSpec (preferred) — s3_storage(), gcs_storage(),
            local_filesystem_storage(), etc. The spec captures exact constructor
            kwargs so the config is serialized losslessly and rebuilt later. A raw
            icechunk.Storage is also accepted but only alongside storage_config=,
            since icechunk.Storage itself cannot be serialized.
        storage_config:
            Serializable storage config dict. Required when storage is a raw
            icechunk.Storage; optional override when storage is a StorageSpec.
            For private stores, record from_env credentials here so to_xarray()
            works with no args at read time.
        config:
            Optional icechunk.RepositoryConfig. Required for stores with virtual
            chunks — basal serializes the VirtualChunkContainer settings so
            to_xarray() can reconstruct config and credentials automatically.
            Build with icechunk.RepositoryConfig and set_virtual_chunk_container().
        derive_extent:
            If True, read coordinate arrays to auto-populate ``bbox``,
            ``start_datetime``, and ``end_datetime``. Reads 1-D coord arrays
            only — no chunk data. Explicit kwargs still win. Ignored when
            ``inspect=False``.
        inspect:
            If False, skip store IO entirely — no CF attrs, var_names, or
            snapshot_id are auto-derived. Caller must supply all desired
            metadata as kwargs. Use for large stores or offline registration.
        **metadata:
            Arbitrary metadata fields. Common optional fields: owner, title,
            license, tags. Pass location= to override the auto-derived URL.
        """
        from .storage import (
            StorageSpec,
            _virtual_chunk_container_to_config,
            location_from_config,
        )

        _validate_name(name)
        self._check_writable()
        # Check name availability before the (expensive) store inspection so
        # register_or_update's update path doesn't pay for a discarded inspect.
        existing = self._assert_unregistered(name)

        # icechunk.Storage has no serialization API, so a catalog entry persists a
        # serializable storage_config to reopen the dataset later. A StorageSpec yields
        # one losslessly; a raw Storage cannot, so its config must be supplied via
        # storage_config= (else the entry has none — to_xarray() then needs storage=).
        if isinstance(storage, StorageSpec):
            ic_storage = storage.build()
            spec_config = storage_config or storage.to_config()
        else:
            ic_storage = storage
            spec_config = storage_config  # None for a raw Storage with no config

        derived = (
            _derive_metadata_from_store(
                ic_storage, branch=branch, config=config, derive_extent=derive_extent
            )
            if inspect
            else {}
        )

        derived_storage_config = spec_config
        derived_location = metadata.pop("location", None) or (
            location_from_config(spec_config) if spec_config else None
        )
        if not derived_location:
            raise ValueError(
                f"Cannot determine location for '{name}'. Pass a StorageSpec, "
                "storage_config=, or location= explicitly."
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
        entry_meta = finalize(entry_meta)
        self._commit_entry(name, entry_meta, existing=existing)

    def register_zarr(
        self,
        name: str,
        location: str,
        store_config: dict | None = None,
        derive_extent: bool = False,
        inspect: bool = True,
        **metadata: Any,
    ) -> None:
        """Register a plain Zarr store (not Icechunk).

        Unlike ``register()``, this accepts a URI string and optional obstore
        ``store_config`` dict instead of an ``icechunk.Storage``. Uses obstore
        as the cloud backend — no gcsfs or s3fs required.

        Icechunk-specific features (``is_stale()``, ``open_repo()``,
        ``open_session()``, ``last_data_updated()``) are not available for
        zarr entries. Use ``to_xarray()`` to open the store.

        Parameters
        ----------
        location:
            URI to the Zarr store. Supported: ``s3://``, ``gs://`` / ``gcs://``,
            ``az://`` / ``abfs://``, or a local path.
        store_config:
            Obstore config dict — forwarded to the cloud store constructor's
            ``config=`` parameter. Use ``{"skip_signature": True}`` for
            public/anonymous access. See obstore docs for provider-specific keys.
        derive_extent:
            If True, read coordinate arrays to auto-populate ``bbox``,
            ``start_datetime``, and ``end_datetime``. Ignored when
            ``inspect=False``.
        inspect:
            If False, skip store IO entirely — no CF attrs or var_names are
            auto-derived. Caller must supply all desired metadata as kwargs.
            Use for large stores or offline registration.
        **metadata:
            Arbitrary metadata fields (owner, title, license, tags, …).
        """
        from .inspect import inspect_zarr_store, stable_attrs

        _validate_name(name)
        self._check_writable()
        existing = self._assert_unregistered(name)

        if inspect:
            info = inspect_zarr_store(
                location, store_config=store_config, derive_extent=derive_extent
            )
            derived = stable_attrs(info)
        else:
            derived = {}

        entry_meta: dict[str, Any] = {
            "location": location,
            "format": "zarr",
            **derived,
            **metadata,
        }
        if store_config:
            entry_meta["store_config"] = store_config
        entry_meta = finalize(entry_meta)
        self._commit_entry(name, entry_meta, existing=existing)

    def _assert_unregistered(self, name: str) -> bool:
        """Raise if ``name`` is actively registered; return whether its branch exists.

        A deregistered branch is reusable — returns True so the caller commits onto
        it instead of creating a new branch.
        """
        existing = name in self._repo.list_branches()
        if existing and not self._head_metadata(name).get(DELETED_KEY):
            raise ValueError(
                f"Dataset '{name}' already registered. Use deregister first."
            )
        return existing

    def _commit_entry(
        self, name: str, entry_meta: dict[str, Any], existing: bool | None = None
    ) -> None:
        """Create the entry branch (or reuse a deregistered one) and commit metadata.

        Raises if the branch exists and is not deregistered. A deregistered branch is
        reused, so re-registering that name succeeds and clears the deregistered marker.
        """
        if existing is None:
            existing = self._assert_unregistered(name)
        if not existing:
            main_snap = self._repo.lookup_branch("main")
            try:
                self._repo.create_branch(name, main_snap)
            except icechunk.IcechunkError as err:
                raise ValueError(
                    f"Dataset '{name}' already registered (created concurrently). "
                    "Use deregister first."
                ) from err
        session = self._repo.writable_session(name)
        session.commit(
            f"register {name}",
            metadata={**entry_meta, EVENT_KEY: "registered"},
            allow_empty=True,
        )

    def register_or_update(
        self,
        name: str,
        storage: icechunk.Storage | StorageSpec,
        format: str = "icechunk",
        branch: str = "main",
        config: icechunk.RepositoryConfig | None = None,
        storage_config: dict | None = None,
        derive_extent: bool = False,
        inspect: bool = True,
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
                inspect=inspect,
                **metadata,
            )
            return "registered"
        except ValueError as e:
            if "already registered" in str(e):
                self.update(name, **metadata)
                return "updated"
            raise

    def update(
        self,
        name: str,
        remove_fields: list[str] | tuple[str, ...] | None = None,
        **fields: Any,
    ) -> None:
        """Merge ``fields`` into the current metadata (new values win).

        Pass ``remove_fields=["key", ...]`` to delete metadata keys outright —
        plain merging can never remove a key. Required fields (location, format)
        cannot be removed; validation rejects the result.
        """
        self._check_writable()
        entry = self.get(name)
        merged = {**entry.metadata, **fields}
        for key in remove_fields or ():
            merged.pop(key, None)
        merged = finalize(merged)

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
        time_only: bool = False,
        **fields: Any,
    ) -> dict:
        """Refresh metadata from the live store; return a diff of changed fields.

        By default re-derives stable CF attrs + ``dataset_snapshot_id`` (and bbox +
        temporal bounds when ``derive_extent=True``). Pass ``time_only=True`` for the
        cheap append path: reads only the time coordinate to refresh ``end_datetime``
        and ``dataset_snapshot_id``, skipping CF/variable re-inspection — intended for
        operational datasets that grow in time (NWP forecasts, reanalyses).

        Explicit ``fields`` are applied on top of derived values. ``storage`` and
        ``config`` are reconstructed from the entry's stored config when omitted
        (requires the entry was registered with a storage_config).

        Returns ``{field: (old, new)}`` for every field whose value changed.
        """
        entry = self.get(name)
        resolved = entry._resolve_storage(storage)
        if time_only:
            derived = _derive_time_extent(
                resolved, branch=branch, config=entry._resolve_repo_config(config)
            )
        else:
            derived = _derive_metadata_from_store(
                resolved,
                branch=branch,
                config=entry._resolve_repo_config(config),
                derive_extent=derive_extent,
            )
        updates = {**derived, **fields}
        before = entry.metadata
        self.update(name, **updates)
        return {k: (before.get(k), v) for k, v in updates.items() if before.get(k) != v}

    def deregister(self, name: str, purge: bool = False) -> None:
        """Deregister an entry.

        Reversible by default: commits a ``deregistered`` marker at the branch HEAD,
        keeping the branch and its full commit history so the entry can be restored or
        the name re-registered. The entry is excluded from ``list()`` and ``get()``
        (unless ``include_deleted``).

        Pass ``purge=True`` to delete the branch outright — irreversible, history lost
        (the equivalent of ``git branch -D``). Reserve it for compliance erasure or
        throwaway test entries.
        """
        self._check_writable()
        if purge:
            self._repo.delete_branch(name)
            return
        entry = self.get(name, include_deleted=True)
        merged = {**entry.metadata, DELETED_KEY: True}
        session = self._repo.writable_session(name)
        session.commit(
            f"deregister {name}",
            metadata={**merged, EVENT_KEY: "deregistered"},
            allow_empty=True,
        )

    def restore(self, name: str) -> None:
        """Clear the deregistered marker, committing the entry back into the catalog."""
        self._check_writable()
        entry = self.get(name, include_deleted=True)
        if not self._head_metadata(name).get(DELETED_KEY):
            return
        session = self._repo.writable_session(name)
        session.commit(
            f"restore {name}",
            metadata={**entry.metadata, EVENT_KEY: "updated"},
            allow_empty=True,
        )

    # --- reads ---

    def _head_metadata(self, name: str) -> dict:
        """Raw HEAD commit metadata for an entry branch (internal keys retained)."""
        snapshot_id = self._repo.lookup_branch(name)
        return self._repo.lookup_snapshot(snapshot_id).metadata or {}

    def get(self, name: str, include_deleted: bool = False) -> Entry:
        try:
            snapshot_id = self._repo.lookup_branch(name)
        except icechunk.IcechunkError as err:
            raise KeyError(f"No entry named {name!r} in catalog") from err
        info = self._repo.lookup_snapshot(snapshot_id)
        meta = info.metadata or {}
        if meta.get(DELETED_KEY) and not include_deleted:
            raise KeyError(
                f"Entry {name!r} was deregistered. Pass include_deleted=True to read it, "
                "restore() it, or re-register the name."
            )
        return Entry(
            name=name,
            snapshot_id=snapshot_id,
            metadata=_strip_internal(meta),
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
            if meta.get(DELETED_KEY):
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

    def history(self, name: str | None = None, limit: int = 10) -> list[dict]:
        """Return catalog operation history, newest first. See ``history.collect_history``.

        Cost is ``limit`` snapshot lookups, each a round-trip on object storage. The
        default ``limit=10`` is bounded; a large catalog-wide ``limit`` over a deep
        history is expensive (seconds on S3). Prefer ``name=`` for per-entry history —
        it skips non-matching branches and only looks up that entry's snapshots.
        """
        if name is None and limit > 100:
            warnings.warn(
                f"history(limit={limit}) with no name= does up to {limit} snapshot "
                "lookups (one round-trip each on object storage). Pass name= for a "
                "single entry, or use a smaller limit.",
                stacklevel=2,
            )
        return collect_history(self._repo, name=name, limit=limit)

    # --- search ---

    def sql(self, query: str) -> list[tuple]:
        """Run DuckDB SQL over entries(name VARCHAR, snapshot_id VARCHAR, metadata JSON)."""
        from .search import sql

        return sql(self, query)

    def search(
        self,
        query: str,
        embed_fn=None,
        top_k: int = 5,
        use_schema: bool = False,
        pre_filter: str | None = None,
    ) -> list[tuple]:
        """Find entries most similar to a free-text query using vector cosine similarity.

        Shorthand for similar(catalog, query). Requires basal[search].

        Parameters
        ----------
        use_schema
            If True, lazily fetches the full zarr schema from each registered store
            (all da.attrs, coord attrs, global_attrs) for richer embeddings. Results
            are cached in-memory by snapshot_id. Ignored when False (default), which
            uses only the CF attrs cached at registration time.
        pre_filter
            DuckDB SQL WHERE clause on the variable-level schema table. Only used
            when use_schema=True. See similar_by_schema() for available columns.
        """
        if use_schema:
            from .search import similar_by_schema

            return similar_by_schema(
                self, query, pre_filter=pre_filter, embed_fn=embed_fn, top_k=top_k
            )

        from .search import similar

        return similar(self, query, embed_fn=embed_fn, top_k=top_k)

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
                from .stac import bbox_overlaps

                e_bbox = entry.metadata.get("bbox")
                if e_bbox is None:
                    spatial_missing.append(entry.name)
                    continue
                if not bbox_overlaps(list(e_bbox), list(bbox)):
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
            except (ValueError, NotImplementedError):
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

    def expire(
        self,
        older_than: datetime,
        *,
        garbage_collect: bool = False,
    ) -> set[str]:
        """Expire entry-history snapshots older than ``older_than``, keeping branch HEADs.

        Each ``update``/``extend`` adds a snapshot. ``list()`` reads all snapshots via
        ``inspect_repo_info``, so unbounded history slows listing. Expiring collapses old
        snapshots out of the metadata graph while preserving every entry's current HEAD.

        Branches (catalog entries) are never deleted — ``delete_expired_branches`` is
        intentionally not exposed. Pass ``garbage_collect=True`` to also reclaim the
        underlying object storage for expired snapshots.

        Returns the set of expired snapshot ids.
        """
        self._check_writable()
        expired = self._repo.expire_snapshots(older_than=older_than)
        if garbage_collect:
            self._repo.garbage_collect(older_than)
        return expired

    # --- export ---

    def to_stac(self, collection_id: str = "basal-catalog") -> dict:
        """Export catalog as a STAC Collection with Items.

        Uses the same entry -> Item conversion as the STAC API server
        (``basal.stac.entry_to_stac_item``). Entries without bbox get null
        geometry — valid per STAC spec for non-spatial datasets.

        Returns a dict with:
          - "collection": STAC Collection object
          - "items": list of STAC Item dicts

        Full STAC spec: https://github.com/radiantearth/stac-spec/
        """
        from .stac import STAC_VERSION, entry_to_stac_item, union_bbox

        items = [entry_to_stac_item(e, collection_id) for e in self.list()]

        collection = {
            "type": "Collection",
            "id": collection_id,
            "stac_version": STAC_VERSION,
            "description": "Icechunk dataset catalog exported from basal",
            "links": [],
            "extent": {
                "spatial": {"bbox": [union_bbox(items)]},
                "temporal": {"interval": [[None, None]]},
            },
            "license": "various",
        }

        return {"collection": collection, "items": items}

    # --- pretty printing ---

    def summary(self) -> None:
        """Print field coverage across all entries, flagging missing recommended fields."""
        from rich.console import Console
        from rich.table import Table

        from .schema import RECOMMENDED_FIELDS

        entries = self.list()
        n = len(entries)
        if not n:
            Console().print("[dim]Empty catalog[/dim]")
            return

        all_fields: set[str] = set()
        for e in entries:
            all_fields.update(e.metadata.keys())

        # recommended first, then remaining sorted
        ordered = list(RECOMMENDED_FIELDS) + sorted(
            f for f in all_fields if f not in RECOMMENDED_FIELDS
        )

        table = Table(title=f"Catalog summary ({n} entries)", show_header=True)
        table.add_column("field", style="bold")
        table.add_column("coverage", justify="right")
        table.add_column("bar")
        table.add_column("recommended", justify="center")

        bar_width = 20
        for field in ordered:
            if field not in all_fields:
                count = 0
            else:
                count = sum(1 for e in entries if field in e.metadata)
            frac = count / n
            filled = int(frac * bar_width)
            bar = "█" * filled + "░" * (bar_width - filled)
            coverage = f"{count}/{n}"
            is_rec = "✓" if field in RECOMMENDED_FIELDS else ""
            color = "green" if frac == 1.0 else ("yellow" if frac > 0 else "red")
            table.add_row(field, coverage, f"[{color}]{bar}[/{color}]", is_rec)

        Console().print(table)

        missing_rec = [
            f for f in RECOMMENDED_FIELDS if not all(f in e.metadata for e in entries)
        ]
        if missing_rec:
            Console().print(
                f"\n[yellow]Recommended fields with incomplete coverage:[/yellow] "
                f"{', '.join(missing_rec)}\n"
                f"[dim]See STAC spec: https://github.com/radiantearth/stac-spec/"
                f"blob/master/item-spec/item-spec.md[/dim]"
            )

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
        table = Table(title=f"Catalog ({len(entries)} entries)")
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
        # Count via list() so deregistered/locationless branches don't inflate it.
        return f"<Catalog with {len(self.list())} entries>"

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
            f"<th colspan=4>Catalog ({len(entries)} entries)</th></tr>"
            f"<tr><th>name</th><th>owner</th><th>title</th><th>location</th></tr>"
            f"</thead><tbody>{rows}</tbody></table>"
        )
