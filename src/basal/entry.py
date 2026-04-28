from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import icechunk

from .storage import _repo_config_from_virtual_chunks, default_virtual_chunk_credentials

if TYPE_CHECKING:
    from .catalog import IcechunkCatalog


@dataclass
class Entry:
    name: str
    snapshot_id: str
    metadata: dict[str, Any]
    written_at: Any = None

    @property
    def location(self) -> str:
        return self.metadata["location"]

    @property
    def owner(self) -> str | None:
        return self.metadata.get("owner")

    @property
    def format(self) -> str:
        return self.metadata.get("format", "icechunk")

    @property
    def virtual_chunk_containers(self) -> list[str]:
        """URL prefixes of virtual chunk containers recorded at registration time."""
        return self.metadata.get("virtual_chunk_containers", [])

    def _resolve_storage(
        self, storage: icechunk.Storage | None = None
    ) -> icechunk.Storage:
        """Return storage, reconstructing from metadata if not provided.

        Raises ValueError if no storage is provided and no storage_config
        is stored in this entry's metadata.
        """
        if storage is not None:
            return storage
        sc = self.metadata.get("storage_config")
        if sc is None:
            raise ValueError(
                f"Entry {self.name!r} has no storage_config. "
                "Pass storage= explicitly, or re-register to auto-derive it."
            )
        from .storage import storage_from_config

        return storage_from_config(sc)

    def _resolve_repo_config(
        self, config: icechunk.RepositoryConfig | None = None
    ) -> icechunk.RepositoryConfig | None:
        """Return config, reconstructing from stored metadata if not provided."""
        if config is not None:
            return config
        vcc = self.metadata.get("virtual_chunk_containers_config")
        if vcc:
            return _repo_config_from_virtual_chunks(vcc)
        # Fallback for entries registered without config= (prefixes only stored).
        # Infers region from storage_config and assumes anonymous=True.
        prefixes = self.metadata.get("virtual_chunk_containers", [])
        if prefixes:
            sc = self.metadata.get("storage_config") or {}
            containers = [
                {
                    "url_prefix": p,
                    **({"region": sc["region"]} if sc.get("region") else {}),
                    "anonymous": True,
                }
                for p in prefixes
            ]
            return _repo_config_from_virtual_chunks(containers)
        return None

    def _resolve_virtual_chunk_credentials(
        self,
        authorize_virtual_chunk_access: dict | None = None,
    ) -> dict | None:
        """Return credentials for virtual chunks, building from config if needed."""
        if authorize_virtual_chunk_access is not None:
            return authorize_virtual_chunk_access
        vcc = self.metadata.get("virtual_chunk_containers_config")
        containers: list = vcc if vcc else self.virtual_chunk_containers
        if not containers:
            return None
        return default_virtual_chunk_credentials(containers)

    def _open_dataset_repo(
        self,
        storage: icechunk.Storage | None = None,
        config: icechunk.RepositoryConfig | None = None,
        authorize_virtual_chunk_access: dict | None = None,
    ) -> icechunk.Repository:
        resolved_storage = self._resolve_storage(storage)
        kwargs: dict[str, Any] = {}
        resolved_config = self._resolve_repo_config(config)
        if resolved_config is not None:
            kwargs["config"] = resolved_config
        resolved_creds = self._resolve_virtual_chunk_credentials(
            authorize_virtual_chunk_access
        )
        if resolved_creds is not None:
            kwargs["authorize_virtual_chunk_access"] = resolved_creds
        return icechunk.Repository.open(resolved_storage, **kwargs)

    def open_repo(
        self,
        storage: icechunk.Storage | None = None,
        config: icechunk.RepositoryConfig | None = None,
        authorize_virtual_chunk_access: dict | None = None,
    ) -> icechunk.Repository:
        """Return the icechunk.Repository for this entry.

        Useful for manual zarr/xarray construction, writing, or accessing
        icechunk-specific APIs (tags, branches, ancestry, etc.).
        # Q: Should we enforce readonly or have an option to switch
        """
        return self._open_dataset_repo(storage, config, authorize_virtual_chunk_access)

    def open_session(
        self,
        branch: str | None = "main",
        tag: str | None = None,
        snapshot_id: str | None = None,
        storage: icechunk.Storage | None = None,
        config: icechunk.RepositoryConfig | None = None,
        authorize_virtual_chunk_access: dict | None = None,
    ) -> icechunk.Session:
        """Return a readonly icechunk.Session for this entry.

        Useful for passing session.store directly to xr.open_zarr() — e.g.
        to open a specific group or pass custom open_zarr options:

            session = entry.open_session()
            ds = xr.open_zarr(session.store, group="1x721x1440", consolidated=False)
        """
        repo = self._open_dataset_repo(storage, config, authorize_virtual_chunk_access)
        ref_args: dict[str, Any] = {}
        if snapshot_id:
            ref_args["snapshot_id"] = snapshot_id
        elif tag:
            ref_args["tag"] = tag
        else:
            ref_args["branch"] = branch
        return repo.readonly_session(**ref_args)

    def to_xarray(
        self,
        *,
        branch: str | None = "main",
        tag: str | None = None,
        snapshot_id: str | None = None,
        storage: icechunk.Storage | None = None,
        config: icechunk.RepositoryConfig | None = None,
        authorize_virtual_chunk_access: dict | None = None,
        open_kwargs: dict[str, Any] | None = None,
    ):
        import xarray as xr

        if self.format != "icechunk":
            raise NotImplementedError(
                f"to_xarray not supported for format={self.format!r}"
            )

        session = self.open_session(
            branch=branch,
            tag=tag,
            snapshot_id=snapshot_id,
            storage=storage,
            config=config,
            authorize_virtual_chunk_access=authorize_virtual_chunk_access,
        )
        return xr.open_zarr(session.store, consolidated=False, **(open_kwargs or {}))

    def is_stale(
        self,
        branch: str = "main",
        storage: icechunk.Storage | None = None,
        config: icechunk.RepositoryConfig | None = None,
        authorize_virtual_chunk_access: dict | None = None,
    ) -> bool:
        """Return True if dataset store has advanced past catalogued snapshot.

        Requires dataset_snapshot_id to be recorded — raises ValueError otherwise.
        """
        catalogued = self.metadata.get("dataset_snapshot_id")
        if not catalogued:
            raise ValueError(
                f"Entry {self.name!r} has no dataset_snapshot_id. "
                "Re-register to enable staleness checks."
            )
        repo = self._open_dataset_repo(storage, config, authorize_virtual_chunk_access)
        current = repo.lookup_branch(branch)
        return current != catalogued

    def last_data_updated(
        self,
        branch: str = "main",
        storage: icechunk.Storage | None = None,
        config: icechunk.RepositoryConfig | None = None,
        authorize_virtual_chunk_access: dict | None = None,
    ):
        """Return written_at of current HEAD of dataset store (no chunk IO)."""
        repo = self._open_dataset_repo(storage, config, authorize_virtual_chunk_access)
        snap_id = repo.lookup_branch(branch)
        info = repo.lookup_snapshot(snap_id)
        return info.written_at

    def inspect(
        self,
        branch: str = "main",
        storage: icechunk.Storage | None = None,
        config: icechunk.RepositoryConfig | None = None,
    ) -> dict:
        """Read live zarr metadata from store (no chunk IO)."""
        from .inspect import inspect_store

        resolved_storage = self._resolve_storage(storage)
        resolved_config = self._resolve_repo_config(config)
        return inspect_store(resolved_storage, branch=branch, config=resolved_config)

    def infer_extent(
        self,
        catalog: IcechunkCatalog,
        *,
        update: bool = True,
        branch: str | None = "main",
        storage: icechunk.Storage | None = None,
        config: icechunk.RepositoryConfig | None = None,
    ) -> dict[str, Any]:
        """Infer STAC-compatible bbox and temporal bounds from coordinate arrays.

        Opens the dataset via ``to_xarray()`` and reads lat/lon/time coord arrays
        to extract ``bbox``, ``start_datetime``, and ``end_datetime``. If
        ``update=True`` (default), writes any found fields back to the catalog.
        Explicit metadata set on the entry is not overwritten — call
        ``catalog.update()`` directly to force-override.

        Parameters
        ----------
        catalog:
            The catalog owning this entry — used for the optional update.
        update:
            If True, call ``catalog.update(self.name, **extent)`` with findings.
        branch:
            Dataset branch to open.
        storage:
            Override storage — falls back to stored storage_config.
        config:
            Override RepositoryConfig — used for virtual-chunk stores.

        Returns
        -------
        dict with any subset of ``bbox``, ``start_datetime``, ``end_datetime``.
        """
        from .inspect import extract_extent

        ds = self.to_xarray(branch=branch, storage=storage, config=config)
        extent = extract_extent(ds)
        if update and extent:
            catalog.update(self.name, **extent)
        return extent

    def similar(
        self,
        catalog: IcechunkCatalog,
        n: int = 5,
        embed_fn: Callable[[list[str]], Any] | None = None,
    ) -> list[tuple[Entry, float]]:
        """Find entries most similar to this entry. Shorthand for catalog.similar_to(self.name)."""
        return catalog.similar_to(self.name, n=n, embed_fn=embed_fn)

    def __repr__(self) -> str:
        return f"Entry(name={self.name!r}, owner={self.owner!r}, location={self.location!r})"

    def _repr_html_(self) -> str:
        rows = "".join(
            f"<tr><td><b>{k}</b></td><td>{v}</td></tr>"
            for k, v in self.metadata.items()
        )
        return (
            f"<table><thead><tr><th colspan=2>Entry: {self.name}</th></tr></thead>"
            f"<tbody>{rows}"
            f"<tr><td><b>snapshot_id</b></td><td><code>{self.snapshot_id}</code></td></tr>"
            f"<tr><td><b>written_at</b></td><td>{self.written_at}</td></tr>"
            f"</tbody></table>"
        )
