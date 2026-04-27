"""Tests for post-refactor behavior: name validation, event-keyed history,
facet denylist, internal-key stripping, and helper extraction.
"""

import icechunk
import numpy as np
import pytest
import xarray as xr
from basal import Entry, IcechunkCatalog
from basal.catalog import (
    FACET_DENYLIST,
    _derive_metadata_from_store,
    _strip_internal,
    _validate_name,
)
from basal.history import EVENT_KEY
from basal.storage import (
    repo_config_from_virtual_chunks,
    storage_from_config,
    storage_to_config,
    storage_to_location,
    virtual_chunk_credentials_from_config,
)


@pytest.fixture
def catalog(tmp_path):
    storage = icechunk.local_filesystem_storage(str(tmp_path / "catalog"))
    return IcechunkCatalog.create(storage)


@pytest.fixture
def fake_store(tmp_path):
    path = str(tmp_path / "fake_dataset")
    storage = icechunk.local_filesystem_storage(path)
    repo = icechunk.Repository.create(storage)
    session = repo.writable_session("main")
    ds = xr.Dataset({"var": xr.DataArray([1.0], dims=["x"])})
    ds.to_zarr(session.store, consolidated=False)
    session.commit("init")
    return storage


# --- name validation ---


@pytest.mark.parametrize(
    "name",
    ["sst", "air-temperature", "ersstv5", "a.b", "a/b", "a_b", "a1", "1a"],
)
def test_valid_names_accepted(name):
    _validate_name(name)


@pytest.mark.parametrize(
    "name",
    ["", "-leading-dash", ".leading-dot", "_under", "has space", "a\tb", "a\nb", "a#b"],
)
def test_invalid_names_rejected(name):
    with pytest.raises(ValueError, match="Invalid entry name"):
        _validate_name(name)


def test_reserved_name_main_rejected():
    with pytest.raises(ValueError, match="reserved"):
        _validate_name("main")


def test_register_rejects_invalid_name(catalog, fake_store):
    with pytest.raises(ValueError, match="Invalid entry name"):
        catalog.register("has space", storage=fake_store)


def test_register_rejects_main(catalog, fake_store):
    with pytest.raises(ValueError, match="reserved"):
        catalog.register("main", storage=fake_store)


# --- internal key stripping ---


def test_strip_internal_removes_dunder_keys():
    meta = {"owner": "org", "__event__": "registered", "__private__": "x", "title": "T"}
    out = _strip_internal(meta)
    assert out == {"owner": "org", "title": "T"}


def test_get_does_not_expose_event_key(catalog, fake_store):
    catalog.register("sst", storage=fake_store, owner="org")
    entry = catalog.get("sst")
    assert EVENT_KEY not in entry.metadata
    assert "__event__" not in entry.metadata


def test_list_does_not_expose_event_key(catalog, fake_store):
    catalog.register("sst", storage=fake_store, owner="org")
    entries = catalog.list()
    assert all(EVENT_KEY not in e.metadata for e in entries)


def test_update_does_not_leak_event_key(catalog, fake_store):
    catalog.register("sst", storage=fake_store, owner="org")
    catalog.update("sst", title="Updated")
    entry = catalog.get("sst")
    assert EVENT_KEY not in entry.metadata
    assert entry.metadata["title"] == "Updated"


# --- history uses event metadata, not message prefix ---


def test_history_event_read_from_metadata(catalog, fake_store):
    """Event type comes from snapshot metadata, not commit message parsing."""
    catalog.register("sst", storage=fake_store, owner="org")
    catalog.update("sst", title="v2")

    hist = catalog.history(name="sst")
    events = [r["event"] for r in hist]
    assert "registered" in events
    assert "updated" in events

    snap_id = catalog._repo.lookup_branch("sst")
    snap = catalog._repo.lookup_snapshot(snap_id)
    assert snap.metadata.get(EVENT_KEY) == "updated"


def test_history_registered_snapshot_has_registered_event(catalog, fake_store):
    catalog.register("sst", storage=fake_store, owner="org")
    hist = catalog.history(name="sst")
    reg = next(r for r in hist if r["event"] == "registered")
    snap = catalog._repo.lookup_snapshot(reg["snapshot_id"])
    assert snap.metadata.get(EVENT_KEY) == "registered"


# --- facet denylist ---


def test_facets_excludes_denylisted_fields(catalog, fake_store):
    catalog.register(
        "sst",
        storage=fake_store,
        owner="org",
        description="a long free-text blurb about ocean data",
        doi="10.1234/abc",
        title="SST",
    )
    facets = catalog.facets()
    for field in FACET_DENYLIST:
        assert field not in facets
    assert "owner" in facets
    assert "title" in facets


def test_facets_still_counts_regular_fields(catalog, fake_store):
    catalog.register("sst", storage=fake_store, owner="NOAA", title="SST")
    catalog.register("air", storage=fake_store, owner="NOAA", title="Air")
    catalog.register("wind", storage=fake_store, owner="NCEP", title="Wind")
    facets = catalog.facets()
    assert facets["owner"]["NOAA"] == 2
    assert facets["owner"]["NCEP"] == 1


# --- derive helper ---


def test_derive_metadata_from_store(tmp_path):
    data_path = tmp_path / "ds"
    storage = icechunk.local_filesystem_storage(str(data_path))
    repo = icechunk.Repository.create(storage)
    session = repo.writable_session("main")
    ds = xr.Dataset(
        {"x": (("t",), np.arange(3).astype("float32"), {"units": "m"})},
        coords={"t": [0, 1, 2]},
        attrs={"title": "T", "conventions": "CF-1.8"},
    )
    ds.to_zarr(session.store, mode="w")
    session.commit("w")

    derived = _derive_metadata_from_store(
        icechunk.local_filesystem_storage(str(data_path))
    )
    assert derived["title"] == "T"
    assert derived["conventions"] == "CF-1.8"
    assert "dataset_snapshot_id" in derived
    assert derived["variables"]["x"]["attrs"]["units"] == "m"


# --- module layout ---


def test_public_exports():
    import basal as pkg

    assert pkg.IcechunkCatalog is IcechunkCatalog
    assert pkg.Entry is Entry
    assert hasattr(pkg, "search")
    assert hasattr(pkg, "inspect")


def test_core_module_removed():
    with pytest.raises(ImportError):
        import basal.core  # noqa: F401


def test_no_search_sql_method():
    assert not hasattr(IcechunkCatalog, "search_sql")


# --- storage_to_config / storage_to_location / storage_from_config ---


def test_storage_to_config_local(tmp_path):
    path = str(tmp_path / "store")
    s = icechunk.local_filesystem_storage(path)
    config = storage_to_config(s)
    assert config == {"type": "local", "path": path}


def test_storage_to_config_s3_anon():
    s = icechunk.s3_storage(
        bucket="my-bucket", prefix="my/prefix", region="us-west-2", anonymous=True
    )
    config = storage_to_config(s)
    assert config["type"] == "s3"
    assert config["bucket"] == "my-bucket"
    assert config["prefix"] == "my/prefix"
    assert config["region"] == "us-west-2"
    assert config["anonymous"] is True
    assert "from_env" not in config


def test_storage_to_location_local(tmp_path):
    path = str(tmp_path / "store")
    s = icechunk.local_filesystem_storage(path)
    assert storage_to_location(s) == f"file://{path}"


def test_storage_to_location_s3():
    s = icechunk.s3_storage(
        bucket="my-bucket", prefix="my/prefix", region="us-west-2", anonymous=True
    )
    assert storage_to_location(s) == "s3://my-bucket/my/prefix"


def test_storage_from_config_s3_roundtrip():
    config = {
        "type": "s3",
        "bucket": "b",
        "prefix": "p",
        "region": "us-west-2",
        "anonymous": True,
    }
    s = storage_from_config(config)
    assert s is not None


def test_storage_from_config_unknown_raises():
    with pytest.raises(ValueError, match="Unknown storage type"):
        storage_from_config({"type": "ftp", "bucket": "b", "prefix": "p"})


def test_register_derives_location_and_config(tmp_path):
    """register() auto-derives location and storage_config from icechunk.Storage."""
    data_path = tmp_path / "ds"
    storage = icechunk.local_filesystem_storage(str(data_path))
    repo = icechunk.Repository.create(storage)
    session = repo.writable_session("main")
    ds = xr.Dataset({"v": xr.DataArray([1.0, 2.0], dims=["x"])})
    ds.to_zarr(session.store, consolidated=False)
    session.commit("init")

    catalog_storage = icechunk.local_filesystem_storage(str(tmp_path / "catalog"))
    cat = IcechunkCatalog.create(catalog_storage)
    cat.register("v", storage=storage)

    entry = cat.get("v")
    assert entry.location == f"file://{data_path}"
    assert entry.metadata["storage_config"] == {"type": "local", "path": str(data_path)}
    opened = entry.to_xarray()
    xr.testing.assert_equal(ds, opened.compute())


def test_register_location_override(tmp_path, fake_store):
    """location= kwarg overrides auto-derived location."""
    cat = IcechunkCatalog.create(icechunk.local_filesystem_storage(str(tmp_path / "c")))
    cat.register("x", storage=fake_store, location="s3://custom/override")
    entry = cat.get("x")
    assert entry.location == "s3://custom/override"


def test_open_repo_returns_repository(tmp_path, fake_store):
    cat = IcechunkCatalog.create(icechunk.local_filesystem_storage(str(tmp_path / "c")))
    cat.register("x", storage=fake_store, owner="org")
    entry = cat.get("x")
    repo = entry.open_repo()
    assert isinstance(repo, icechunk.Repository)


def test_open_session_returns_session(tmp_path, fake_store):
    cat = IcechunkCatalog.create(icechunk.local_filesystem_storage(str(tmp_path / "c")))
    cat.register("x", storage=fake_store, owner="org")
    entry = cat.get("x")
    session = entry.open_session()
    assert isinstance(session, icechunk.Session)


# --- virtual chunk config utilities ---


def test_virtual_chunk_credentials_anon():
    containers = [{"url_prefix": "s3://bucket/", "anonymous": True}]
    creds = virtual_chunk_credentials_from_config(containers)
    assert creds is not None


def test_virtual_chunk_credentials_empty():
    assert virtual_chunk_credentials_from_config([]) is None


def test_repo_config_from_virtual_chunks_s3():
    containers = [
        {"url_prefix": "s3://bucket/", "region": "us-west-2", "anonymous": True}
    ]
    config = repo_config_from_virtual_chunks(containers)
    assert isinstance(config, icechunk.RepositoryConfig)
    assert "s3://bucket/" in config.virtual_chunk_containers


def test_repo_config_from_virtual_chunks_unsupported_scheme():
    containers = [{"url_prefix": "ftp://bucket/", "anonymous": True}]
    with pytest.raises(NotImplementedError):
        repo_config_from_virtual_chunks(containers)


# --- values() handles unhashable values ---


def test_values_with_unhashable(catalog, fake_store):
    catalog.register("a", storage=fake_store, owner="org", dims={"t": 3})
    catalog.register("b", storage=fake_store, owner="org", dims={"t": 3})
    catalog.register("c", storage=fake_store, owner="org", dims={"t": 5})
    vals = catalog.values("dims")
    assert {"t": 3} in vals
    assert {"t": 5} in vals
    assert len(vals) == 2
