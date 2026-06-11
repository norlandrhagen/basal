"""Tests for plain Zarr store catalog support (format='zarr')."""

from __future__ import annotations

import icechunk
import numpy as np
import pytest
import xarray as xr
from basal import Catalog


@pytest.fixture
def catalog(tmp_path):
    storage = icechunk.local_filesystem_storage(str(tmp_path / "catalog"))
    return Catalog.open_or_create(storage)


@pytest.fixture
def zarr_store(tmp_path):
    """Local Zarr v3 store with a simple dataset."""
    path = str(tmp_path / "test.zarr")
    ds = xr.Dataset(
        {
            "temperature": xr.DataArray(
                np.random.rand(3, 4).astype("float32"),
                dims=["time", "x"],
                attrs={"units": "K", "long_name": "Air Temperature"},
            )
        },
        attrs={"title": "Test Dataset", "institution": "Test Org"},
    )
    ds.to_zarr(path, mode="w")
    return path


# --- register_zarr ---


def test_register_zarr_basic(catalog, zarr_store):
    catalog.register_zarr("test-ds", location=zarr_store, owner="test-org")
    entry = catalog.get("test-ds")
    assert entry.name == "test-ds"
    assert entry.location == zarr_store
    assert entry.format == "zarr"
    assert entry.owner == "test-org"


def test_register_zarr_derives_cf_attrs(catalog, zarr_store):
    catalog.register_zarr("test-ds", location=zarr_store)
    entry = catalog.get("test-ds")
    assert entry.metadata.get("title") == "Test Dataset"
    assert "variables" in entry.metadata
    assert "temperature" in entry.metadata["variables"]


def test_register_zarr_stores_store_config(catalog, zarr_store, monkeypatch):
    # store_config only valid for remote URI stores; mock inspect so we can
    # verify they round-trip through metadata without needing a real S3 store.
    from basal import inspect as basal_inspect

    monkeypatch.setattr(
        basal_inspect,
        "inspect_zarr_store",
        lambda location, store_config=None, derive_extent=False: {
            "global_attrs": {},
            "dims": {},
            "variables": {},
            "coords": {},
        },
    )
    opts = {"skip_signature": True}
    catalog.register_zarr("test-ds", location=zarr_store, store_config=opts)
    entry = catalog.get("test-ds")
    assert entry.metadata.get("store_config") == opts


def test_register_zarr_no_store_config_not_stored(catalog, zarr_store):
    catalog.register_zarr("test-ds", location=zarr_store)
    entry = catalog.get("test-ds")
    assert "store_config" not in entry.metadata


def test_register_zarr_no_dataset_snapshot_id(catalog, zarr_store):
    catalog.register_zarr("test-ds", location=zarr_store)
    entry = catalog.get("test-ds")
    assert "dataset_snapshot_id" not in entry.metadata


def test_register_zarr_duplicate_raises(catalog, zarr_store):
    catalog.register_zarr("test-ds", location=zarr_store)
    with pytest.raises(ValueError, match="already registered"):
        catalog.register_zarr("test-ds", location=zarr_store)


# --- to_xarray ---


def test_to_xarray_zarr(catalog, zarr_store):
    catalog.register_zarr("test-ds", location=zarr_store)
    entry = catalog.get("test-ds")
    ds = entry.to_xarray()
    assert isinstance(ds, xr.Dataset)
    assert "temperature" in ds


def test_to_xarray_zarr_with_open_kwargs(catalog, zarr_store):
    catalog.register_zarr("test-ds", location=zarr_store)
    entry = catalog.get("test-ds")
    ds = entry.to_xarray(open_kwargs={"chunks": {}})
    assert isinstance(ds, xr.Dataset)


# --- inspect ---


def test_inspect_zarr(catalog, zarr_store):
    catalog.register_zarr("test-ds", location=zarr_store)
    entry = catalog.get("test-ds")
    info = entry.inspect()
    assert "variables" in info
    assert "temperature" in info["variables"]
    assert "global_attrs" in info
    assert "dataset_snapshot_id" not in info


# --- icechunk-only guards ---


def test_open_repo_raises_for_zarr(catalog, zarr_store):
    catalog.register_zarr("test-ds", location=zarr_store)
    entry = catalog.get("test-ds")
    with pytest.raises(NotImplementedError, match="open_repo"):
        entry.open_repo()


def test_open_session_raises_for_zarr(catalog, zarr_store):
    catalog.register_zarr("test-ds", location=zarr_store)
    entry = catalog.get("test-ds")
    with pytest.raises(NotImplementedError, match="open_session"):
        entry.open_session()


def test_is_stale_raises_for_zarr(catalog, zarr_store):
    catalog.register_zarr("test-ds", location=zarr_store)
    entry = catalog.get("test-ds")
    with pytest.raises(NotImplementedError, match="is_stale"):
        entry.is_stale()


def test_last_data_updated_raises_for_zarr(catalog, zarr_store):
    catalog.register_zarr("test-ds", location=zarr_store)
    entry = catalog.get("test-ds")
    with pytest.raises(NotImplementedError, match="last_data_updated"):
        entry.last_data_updated()


# --- schema validation ---


def test_validate_rejects_unknown_format(catalog, zarr_store):
    from basal.schema import validate

    with pytest.raises(ValueError, match="Unsupported format"):
        validate({"location": "s3://bucket/path", "format": "netcdf"})


# --- build_zarr_store cloud URI construction (no credentials needed) ---


@pytest.mark.parametrize(
    "location,config",
    [
        ("s3://my-bucket/path/to/store", {"skip_signature": True}),
        ("gs://my-bucket/path/to/store", {"skip_signature": True}),
        ("gcs://my-bucket/path/to/store", {"skip_signature": True}),
        ("az://my-container/path/to/store", {"account_name": "myaccount"}),
    ],
)
def test_build_zarr_store_cloud_schemes(location, config):
    import zarr
    from basal.zarr_store import build_zarr_store

    store = build_zarr_store(location, config)
    assert isinstance(store, zarr.storage.ObjectStore)
    assert store.read_only


def test_build_zarr_store_local_passthrough(tmp_path):
    from basal.zarr_store import build_zarr_store

    assert build_zarr_store(str(tmp_path)) == str(tmp_path)
    assert build_zarr_store(f"file://{tmp_path}") == str(tmp_path)


def test_build_zarr_store_unsupported_scheme():
    from basal.zarr_store import build_zarr_store

    with pytest.raises(ValueError, match="Unsupported URI scheme"):
        build_zarr_store("ftp://host/store")
