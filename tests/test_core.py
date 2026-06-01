import warnings

import icechunk
import numpy as np
import pytest
from basal import IcechunkCatalog
from basal import storage as st
from basal.search import similar, similar_by_schema


@pytest.fixture
def catalog(tmp_path):
    path = str(tmp_path / "test_catalog")
    storage = icechunk.local_filesystem_storage(path)
    return IcechunkCatalog.open_or_create(storage)


@pytest.fixture
def fake_store(tmp_path):
    """Minimal local icechunk repo with a zarr array — used as storage= in register() calls.

    Returns a StorageSpec (the preferred, serializable form) rather than a raw Storage.
    """
    import xarray as xr

    path = str(tmp_path / "fake_dataset")
    spec = st.local_filesystem_storage(path)
    repo = icechunk.Repository.create(spec.build())
    session = repo.writable_session("main")
    ds = xr.Dataset({"var": xr.DataArray([1.0, 2.0], dims=["x"])})
    ds.to_zarr(session.store, consolidated=False)
    session.commit("init")
    return spec


# --- Core Catalog Tests ---


def test_open_or_create(tmp_path, fake_store):
    path = str(tmp_path / "persistent_catalog")
    storage = icechunk.local_filesystem_storage(path)

    c1 = IcechunkCatalog.open_or_create(storage)
    c1.register(
        "sst", storage=fake_store, location="s3://bucket/sst/", owner="carbonplan"
    )

    c2 = IcechunkCatalog.open_or_create(storage)
    assert len(c2.list()) == 1
    assert c2.get("sst").name == "sst"


def test_register_and_get(catalog, fake_store):
    catalog.register(
        "sst", storage=fake_store, location="s3://bucket/sst/", owner="carbonplan"
    )
    entry = catalog.get("sst")
    assert entry.name == "sst"
    assert entry.location == "s3://bucket/sst/"
    assert entry.owner == "carbonplan"
    assert entry.format == "icechunk"


def test_register_with_optional_metadata(catalog, fake_store):
    catalog.register(
        "precip",
        storage=fake_store,
        location="s3://bucket/precip/",
        owner="carbonplan",
        title="Precipitation",
        keywords=["rain", "climate"],
        license="CC-BY-4.0",
    )
    entry = catalog.get("precip")
    assert entry.metadata["title"] == "Precipitation"
    assert entry.metadata["keywords"] == ["rain", "climate"]


def test_list(catalog, fake_store):
    catalog.register(
        "sst", storage=fake_store, location="s3://bucket/sst/", owner="carbonplan"
    )
    catalog.register(
        "precip", storage=fake_store, location="s3://bucket/precip/", owner="noaa"
    )
    assert {e.name for e in catalog.list()} == {"sst", "precip"}


def test_deregister(catalog, fake_store):
    catalog.register(
        "sst", storage=fake_store, location="s3://bucket/sst/", owner="carbonplan"
    )
    catalog.deregister("sst")
    assert not any(e.name == "sst" for e in catalog.list())


def test_duplicate_register_raises(catalog, fake_store):
    catalog.register(
        "sst", storage=fake_store, location="s3://bucket/sst/", owner="carbonplan"
    )
    with pytest.raises(ValueError, match="already registered"):
        catalog.register(
            "sst", storage=fake_store, location="s3://bucket/sst2/", owner="carbonplan"
        )


def test_missing_required_field_raises():
    from basal.schema import validate

    with pytest.raises(ValueError, match="Missing required fields"):
        validate({"location": "s3://bucket/sst/"})


# --- Update ---


def test_update(catalog, fake_store):
    catalog.register(
        "sst",
        storage=fake_store,
        location="s3://bucket/sst/",
        owner="carbonplan",
        keywords=["ocean", "sst"],
        title="Old Title",
    )
    catalog.update("sst", title="New Title", license="CC-BY-4.0")
    entry = catalog.get("sst")
    assert entry.metadata["title"] == "New Title"
    assert entry.metadata["license"] == "CC-BY-4.0"
    assert entry.owner == "carbonplan"
    assert entry.metadata["keywords"] == ["ocean", "sst"]


# --- Search ---


def test_search_sql(catalog, fake_store):
    catalog.register(
        "sst", storage=fake_store, location="s3://bucket/sst/", owner="carbonplan"
    )
    catalog.register(
        "precip", storage=fake_store, location="s3://bucket/precip/", owner="noaa"
    )
    catalog.register(
        "wind", storage=fake_store, location="s3://bucket/wind/", owner="carbonplan"
    )
    results = catalog.sql(
        "SELECT name FROM entries WHERE metadata->>'owner' = 'carbonplan' ORDER BY name"
    )
    assert {r[0] for r in results} == {"sst", "wind"}


def test_search_no_results(catalog, fake_store):
    catalog.register(
        "sst", storage=fake_store, location="s3://bucket/sst/", owner="carbonplan"
    )
    results = catalog.sql(
        "SELECT name FROM entries WHERE metadata->>'owner' = 'nobody'"
    )
    assert results == []


def test_search_similarity(catalog, fake_store):
    catalog.register(
        "ocean-data",
        storage=fake_store,
        location="s3://b/ocean",
        owner="org",
        description="sea surface temperature",
    )
    catalog.register(
        "land-data",
        storage=fake_store,
        location="s3://b/land",
        owner="org",
        description="soil moisture on land",
    )

    def mock_embed(texts):
        vecs = []
        for t in texts:
            t = t.lower()
            if any(word in t for word in ["ocean", "sea", "temperature"]):
                vecs.append(np.array([1.0, 0.0], dtype=np.float32))
            else:
                vecs.append(np.array([0.0, 1.0], dtype=np.float32))
        return vecs

    results = similar(catalog, "ocean search", embed_fn=mock_embed, top_k=1)
    assert len(results) == 1
    entry, score = results[0]
    assert entry.name == "ocean-data"
    assert score > 0.99


def test_similar_by_schema(catalog, fake_store, monkeypatch):
    catalog.register("precip-data", storage=fake_store, location="s3://b/pr")
    catalog.register("temp-data", storage=fake_store, location="s3://b/tas")

    fake_infos = {
        "precip-data": {
            "global_attrs": {"title": "Precipitation dataset", "institution": "NOAA"},
            "variables": {
                "pr": {
                    "dtype": "float32",
                    "shape": [100, 180, 360],
                    "dims": ["time", "lat", "lon"],
                    "attrs": {"units": "kg m-2 s-1", "long_name": "Precipitation flux"},
                }
            },
            "coords": {},
        },
        "temp-data": {
            "global_attrs": {"title": "Temperature dataset", "institution": "ECMWF"},
            "variables": {
                "tas": {
                    "dtype": "float32",
                    "shape": [100, 180, 360],
                    "dims": ["time", "lat", "lon"],
                    "attrs": {"units": "K", "long_name": "Air temperature"},
                }
            },
            "coords": {},
        },
    }

    import basal.search as search_mod

    def mock_fetch(entry):
        return entry.name, fake_infos[entry.name]

    monkeypatch.setattr(search_mod, "_fetch_store_info", mock_fetch)
    monkeypatch.setattr(search_mod, "_schema_cache", {})

    def mock_embed(texts):
        vecs = []
        for t in texts:
            t = t.lower()
            if any(w in t for w in ["precipitation", "rain", "pr", "kg"]):
                vecs.append(np.array([1.0, 0.0], dtype=np.float32))
            else:
                vecs.append(np.array([0.0, 1.0], dtype=np.float32))
        return vecs

    results = similar_by_schema(
        catalog, "precipitation flux", embed_fn=mock_embed, top_k=1
    )
    assert len(results) == 1
    assert results[0][0].name == "precip-data"
    assert results[0][1] > 0.99

    # pre_filter: only datasets with variable_name = 'tas'
    results_filtered = similar_by_schema(
        catalog,
        "precipitation flux",
        pre_filter="variable_name = 'tas'",
        embed_fn=mock_embed,
        top_k=5,
    )
    assert len(results_filtered) == 1
    assert results_filtered[0][0].name == "temp-data"


# --- filter() ---


@pytest.fixture
def catalog_with_bounds(catalog, fake_store):
    catalog.register(
        "historical",
        storage=fake_store,
        location="s3://b/historical",
        start_datetime="2000-01-01",
        end_datetime="2010-12-31",
        bbox=[-180.0, -90.0, 180.0, 90.0],
    )
    catalog.register(
        "recent",
        storage=fake_store,
        location="s3://b/recent",
        start_datetime="2018-01-01",
        end_datetime="2023-12-31",
        bbox=[-10.0, 30.0, 40.0, 70.0],
    )
    catalog.register(
        "ongoing",
        storage=fake_store,
        location="s3://b/ongoing",
        start_datetime="2020-01-01",
        bbox=[-180.0, -90.0, 0.0, 90.0],
    )
    catalog.register("no-bounds", storage=fake_store, location="s3://b/no-bounds")
    return catalog


def test_filter_temporal(catalog_with_bounds):
    assert {
        e.name for e in catalog_with_bounds.filter(time_start="2019", time_end="2021")
    } == {"recent", "ongoing"}
    assert {e.name for e in catalog_with_bounds.filter(time_start="2015")} == {
        "recent",
        "ongoing",
    }
    assert {e.name for e in catalog_with_bounds.filter(time_end="2005")} == {
        "historical"
    }


def test_filter_spatial_overlap(catalog_with_bounds):
    results = catalog_with_bounds.filter(bbox=(-20.0, 40.0, 50.0, 60.0))
    assert {e.name for e in results} == {"historical", "recent", "ongoing"}


def test_filter_combined(catalog_with_bounds):
    results = catalog_with_bounds.filter(
        time_start="2019", time_end="2022", bbox=(-20.0, 40.0, 50.0, 60.0)
    )
    assert {e.name for e in results} == {"recent", "ongoing"}


def test_filter_warns_missing_temporal(catalog_with_bounds):
    with pytest.warns(UserWarning, match="start_datetime"):
        catalog_with_bounds.filter(time_start="2020")


def test_filter_warns_missing_bbox(catalog_with_bounds):
    with pytest.warns(UserWarning, match="bbox"):
        catalog_with_bounds.filter(bbox=(-10.0, -10.0, 10.0, 10.0))


def test_filter_no_args_returns_all(catalog_with_bounds):
    assert len(catalog_with_bounds.filter()) == 4


# --- refresh() ---


def test_refresh_returns_stale_flags(catalog, fake_store):
    catalog.register("ds", storage=fake_store, location="s3://b/ds")
    result = catalog.refresh()
    assert "ds" in result
    assert result["ds"] is False


def test_refresh_warns_missing_snapshot_id(catalog, fake_store):
    catalog.register("ds", storage=fake_store, location="s3://b/ds")
    catalog.update("ds", dataset_snapshot_id=None)
    with pytest.warns(UserWarning, match="dataset_snapshot_id"):
        result = catalog.refresh()
    assert "ds" not in result


# --- update_all_from_store() ---


def test_update_all_from_store(catalog, fake_store):
    catalog.register("ds", storage=fake_store, location="s3://b/ds")
    before = catalog.get("ds").metadata.get("dataset_snapshot_id")
    catalog.update_all_from_store()
    after = catalog.get("ds").metadata.get("dataset_snapshot_id")
    assert after == before


def test_update_all_from_store_warns_no_storage_config(catalog, fake_store):
    catalog.register("ds", storage=fake_store, location="s3://b/ds")
    catalog.update("ds", storage_config=None)
    with pytest.warns(UserWarning, match="storage_config"):
        catalog.update_all_from_store()


def test_update_from_store_time_only(catalog, geo_store):
    # geo_store has a time coord spanning 2020-2022; time_only refreshes end_datetime
    # and dataset_snapshot_id from it and returns the diff.
    catalog.register("clim", storage=geo_store)
    diff = catalog.update_from_store("clim", time_only=True)
    assert "end_datetime" in diff
    assert catalog.get("clim").metadata["end_datetime"].startswith("2022")


# --- infer_extent / derive_extent ---


@pytest.fixture
def geo_store(tmp_path):
    import pandas as pd
    import xarray as xr

    path = str(tmp_path / "geo_dataset")
    spec = st.local_filesystem_storage(path)
    repo = icechunk.Repository.create(spec.build())
    session = repo.writable_session("main")

    lat = np.array([-10.0, 0.0, 10.0])
    lon = np.array([-20.0, 0.0, 20.0])
    time = pd.date_range("2020-01-01", periods=3, freq="YS")
    ds = xr.Dataset(
        {"var": xr.DataArray(np.ones((3, 3, 3)), dims=["time", "lat", "lon"])},
        coords={"time": time, "lat": lat, "lon": lon},
    )
    ds.to_zarr(session.store, consolidated=False)
    session.commit("init")
    return spec


def test_infer_extent(catalog, geo_store):
    catalog.register("geo", storage=geo_store, location="s3://b/geo")
    entry = catalog.get("geo")
    extent = entry.infer_extent(catalog, update=True)
    assert extent["bbox"] == [-20.0, -10.0, 20.0, 10.0]
    assert "start_datetime" in extent
    updated = catalog.get("geo")
    assert updated.metadata["bbox"] == [-20.0, -10.0, 20.0, 10.0]


def test_register_derive_extent(catalog, geo_store):
    catalog.register(
        "geo", storage=geo_store, location="s3://b/geo", derive_extent=True
    )
    entry = catalog.get("geo")
    assert entry.metadata["bbox"] == [-20.0, -10.0, 20.0, 10.0]
    assert "start_datetime" in entry.metadata


def test_register_explicit_kwargs_win_over_derived(catalog, geo_store):
    catalog.register(
        "geo",
        storage=geo_store,
        location="s3://b/geo",
        derive_extent=True,
        bbox=[-180.0, -90.0, 180.0, 90.0],
    )
    assert catalog.get("geo").metadata["bbox"] == [-180.0, -90.0, 180.0, 90.0]


# --- StorageSpec (WS1) ---


def test_storage_spec_roundtrip():
    from basal.storage import location_from_config, storage_from_config

    spec = st.s3_storage(bucket="b", prefix="p/q", region="us-west-2", anonymous=True)
    cfg = spec.to_config()
    assert cfg == {
        "type": "s3",
        "bucket": "b",
        "prefix": "p/q",
        "region": "us-west-2",
        "anonymous": True,
    }
    # config reconstructs without error and yields the canonical location
    storage_from_config(cfg)
    assert location_from_config(cfg) == "s3://b/p/q"
    assert isinstance(spec.build(), icechunk.Storage)


def test_storage_spec_drops_non_serializable_kwargs():
    # credential objects / unknown kwargs must not leak into to_config()
    spec = st.s3_storage(bucket="b", prefix="p", credentials=object())
    cfg = spec.to_config()
    assert "credentials" not in cfg
    assert cfg["bucket"] == "b"


def test_register_with_storage_spec_no_warning(catalog):
    with warnings.catch_warnings():
        warnings.simplefilter("error")  # any warning fails the test
        catalog.register(
            "viaspec",
            storage=st.s3_storage(bucket="b", prefix="viaspec", region="us-west-2"),
            inspect=False,
            owner="org",
        )
    entry = catalog.get("viaspec")
    assert entry.location == "s3://b/viaspec"
    assert entry.metadata["storage_config"]["type"] == "s3"


def test_register_raw_storage_without_config_omits_storage_config(catalog, fake_store):
    # A raw icechunk.Storage cannot be serialized: the entry is created (location is
    # explicit) but carries no storage_config — to_xarray() then needs storage=.
    catalog.register(
        "raw", storage=fake_store.build(), location="s3://b/raw", inspect=False
    )
    entry = catalog.get("raw")
    assert entry.location == "s3://b/raw"
    assert "storage_config" not in entry.metadata


def test_register_raw_storage_no_location_raises(catalog, fake_store):
    with pytest.raises(ValueError, match="Cannot determine location"):
        catalog.register("raw", storage=fake_store.build(), inspect=False)


def test_register_raw_storage_with_explicit_config(catalog, fake_store):
    catalog.register(
        "rawcfg",
        storage=fake_store.build(),
        location="s3://b/rawcfg",
        storage_config={"type": "s3", "bucket": "b", "prefix": "rawcfg"},
        inspect=False,
    )
    assert catalog.get("rawcfg").metadata["storage_config"]["bucket"] == "b"


# --- soft-delete (WS2) ---


def test_deregister_is_soft_by_default(catalog, fake_store):
    catalog.register("sst", storage=fake_store, location="s3://b/sst")
    catalog.deregister("sst")
    # excluded from list
    assert not any(e.name == "sst" for e in catalog.list())
    # get raises unless include_deleted
    with pytest.raises(KeyError):
        catalog.get("sst")
    assert catalog.get("sst", include_deleted=True).name == "sst"
    # branch retained (history preserved)
    assert "sst" in catalog._repo.list_branches()


def test_deregister_history_shows_event(catalog, fake_store):
    catalog.register("sst", storage=fake_store, location="s3://b/sst")
    catalog.deregister("sst")
    events = [(h["event"], h["name"]) for h in catalog.history(name="sst")]
    assert ("deregistered", "sst") in events
    assert ("registered", "sst") in events


def test_restore(catalog, fake_store):
    catalog.register("sst", storage=fake_store, location="s3://b/sst")
    catalog.deregister("sst")
    catalog.restore("sst")
    assert catalog.get("sst").name == "sst"
    assert any(e.name == "sst" for e in catalog.list())


def test_reregister_over_tombstone(catalog, fake_store):
    catalog.register("sst", storage=fake_store, location="s3://b/sst")
    catalog.deregister("sst")
    # re-register the same name succeeds (does not raise "already registered")
    catalog.register("sst", storage=fake_store, location="s3://b/sst-v2")
    assert catalog.get("sst").location == "s3://b/sst-v2"
    assert any(e.name == "sst" for e in catalog.list())


def test_register_live_entry_still_raises(catalog, fake_store):
    catalog.register("sst", storage=fake_store, location="s3://b/sst")
    with pytest.raises(ValueError, match="already registered"):
        catalog.register("sst", storage=fake_store, location="s3://b/sst-dup")


def test_deregister_purge_removes_branch(catalog, fake_store):
    catalog.register("sst", storage=fake_store, location="s3://b/sst")
    catalog.deregister("sst", purge=True)
    assert "sst" not in catalog._repo.list_branches()


# --- expire (WS3) ---


def test_expire_collapses_history_keeps_entries(catalog, fake_store):
    import datetime

    catalog.register("sst", storage=fake_store, location="s3://b/sst")
    for i in range(8):
        catalog.update("sst", rev=i)

    n_before = len(catalog._repo.inspect_repo_info()["snapshots"])
    catalog.expire(datetime.datetime.now(datetime.UTC))
    n_after = len(catalog._repo.inspect_repo_info()["snapshots"])

    assert n_after < n_before  # history collapsed
    # entry survives with its latest metadata intact
    entry = catalog.get("sst")
    assert entry.name == "sst"
    assert entry.metadata["rev"] == 7
    assert {e.name for e in catalog.list()} == {"sst"}
