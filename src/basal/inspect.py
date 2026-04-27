"""On-demand zarr metadata inspection from Icechunk stores.

Reads only zarr structural metadata (manifest + .zattrs/.zarray) — zero chunk IO.
When derive_extent=True, coordinate arrays (lat/lon/time) are read for bbox and
temporal extent — negligible IO for typical 1-D coordinate arrays.
"""

from __future__ import annotations

from typing import Any

import icechunk

_LAT_NAMES = {"lat", "latitude", "y", "ylat"}
_LON_NAMES = {"lon", "longitude", "x", "xlon"}
_TIME_NAMES = {"time", "t"}


def _find_coord(ds: Any, standard_name: str, name_set: set[str]) -> Any:
    """Return first coord matching standard_name attr or a name in name_set."""
    for _, da in ds.coords.items():
        if da.attrs.get("standard_name") == standard_name:
            return da
    for name in name_set:
        if name in ds.coords:
            return ds.coords[name]
    return None


def _np_dt_to_iso(t: Any) -> str | None:
    """Convert numpy datetime64 scalar to ISO 8601 string with Z suffix."""
    try:
        import pandas as pd

        ts = pd.Timestamp(t)
        if ts is pd.NaT:
            return None
        return ts.isoformat() + "Z"
    except Exception:
        return None


def extract_extent(ds: Any) -> dict[str, Any]:
    """Extract STAC-compatible bbox and temporal extent from an open xarray Dataset.

    Reads coordinate array values (small 1-D arrays). Returns a dict with any
    subset of: bbox, start_datetime, end_datetime.
    """
    import numpy as np

    result: dict[str, Any] = {}

    lat_da = _find_coord(ds, "latitude", _LAT_NAMES)
    lon_da = _find_coord(ds, "longitude", _LON_NAMES)
    if lat_da is not None and lon_da is not None:
        lat_vals = lat_da.values
        lon_vals = lon_da.values
        result["bbox"] = [
            float(np.nanmin(lon_vals)),
            float(np.nanmin(lat_vals)),
            float(np.nanmax(lon_vals)),
            float(np.nanmax(lat_vals)),
        ]

    time_da = _find_coord(ds, "time", _TIME_NAMES)
    if time_da is not None and time_da.size > 0:
        t_vals = time_da.values
        t_min = _np_dt_to_iso(t_vals.min())
        t_max = _np_dt_to_iso(t_vals.max())
        if t_min is not None:
            result["start_datetime"] = t_min
        if t_max is not None:
            result["end_datetime"] = t_max

    return result


def inspect_store(
    storage: icechunk.Storage,
    branch: str = "main",
    config: icechunk.RepositoryConfig | None = None,
    derive_extent: bool = False,
) -> dict[str, Any]:
    """Read zarr metadata from an Icechunk store. No chunk data read.

    Parameters
    ----------
    storage:
        Explicit icechunk.Storage for the dataset store. Use s3_config(),
        gcs_config(), or local_config() from basal.storage to build.
    branch:
        Branch to read from.
    config:
        Optional RepositoryConfig — required for stores with virtual chunk
        containers. Use repo_config_from_virtual_chunks() to build from config dicts.
    derive_extent:
        If True, read lat/lon/time coordinate arrays to extract STAC-compatible
        bbox and start_datetime/end_datetime. Reads coordinate data (small arrays).

    Returns a dict with:
      - dataset_snapshot_id: current HEAD snapshot id
      - global_attrs: CF global attributes
      - variables: {name: {dtype, shape, dims, chunks, attrs}}
      - coords: {name: {dtype, shape, attrs}}
      - dims: {name: size}
      - virtual_chunk_containers: list of URL prefixes (if any)
      - bbox, start_datetime, end_datetime: (if derive_extent=True and detected)
    """
    import xarray as xr

    kwargs: dict[str, Any] = {}
    if config is not None:
        kwargs["config"] = config
    repo = icechunk.Repository.open(storage, **kwargs)
    session = repo.readonly_session(branch=branch)

    ds = xr.open_zarr(session.store, consolidated=False)

    result: dict[str, Any] = {}

    result["dataset_snapshot_id"] = session.snapshot_id
    vc = repo.config.virtual_chunk_containers
    if vc:
        result["virtual_chunk_containers"] = list(vc.keys())
    result["global_attrs"] = dict(ds.attrs)
    result["dims"] = dict(ds.sizes)

    variables = {}
    for name, da in ds.data_vars.items():
        entry: dict[str, Any] = {
            "dtype": str(da.dtype),
            "shape": list(da.shape),
            "dims": list(da.dims),
            "attrs": dict(da.attrs),
        }
        if da.encoding.get("chunks"):
            entry["chunks"] = list(da.encoding["chunks"])
        variables[str(name)] = entry
    result["variables"] = variables

    result["coords"] = {
        str(name): {
            "dtype": str(da.dtype),
            "shape": list(da.shape),
            "attrs": dict(da.attrs),
        }
        for name, da in ds.coords.items()
    }

    if derive_extent:
        result.update(extract_extent(ds))

    return result


def stable_attrs(info: dict[str, Any]) -> dict[str, Any]:
    """Extract the subset of inspect_store output safe to store eagerly in the catalog.

    Excludes dims/shape (mutable on append). Includes CF global attrs and
    per-variable stable attrs (units, long_name, standard_name, cell_methods).
    """
    out: dict[str, Any] = {}

    cf_keys = {
        "title",
        "institution",
        "source",
        "references",
        "history",
        "comment",
        "conventions",
    }
    for k in cf_keys:
        if k in info.get("global_attrs", {}):
            out[k] = info["global_attrs"][k]

    var_summary = {}
    for name, meta in info.get("variables", {}).items():
        var_summary[name] = {
            "dtype": meta["dtype"],
            "dims": meta["dims"],
            "attrs": {
                k: v
                for k, v in meta.get("attrs", {}).items()
                if k in {"units", "long_name", "standard_name", "cell_methods"}
            },
        }
    if var_summary:
        out["variables"] = var_summary

    for key in ("bbox", "start_datetime", "end_datetime"):
        if key in info:
            out[key] = info[key]

    return out
