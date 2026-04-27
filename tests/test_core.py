import icechunk
import numpy as np
import pytest
from basal import IcechunkCatalog
from basal.search import similar

# --- Fixtures ---


@pytest.fixture
def catalog(tmp_path):
    path = str(tmp_path / "test_catalog")
    storage = icechunk.local_filesystem_storage(path)
    return IcechunkCatalog.open_or_create(storage)


@pytest.fixture
def fake_store(tmp_path):
    """Minimal local icechunk repo with a zarr array — used as storage= in register() calls."""
    import xarray as xr

    path = str(tmp_path / "fake_dataset")
    storage = icechunk.local_filesystem_storage(path)
    repo = icechunk.Repository.create(storage)
    session = repo.writable_session("main")
    ds = xr.Dataset({"var": xr.DataArray([1.0, 2.0], dims=["x"])})
    ds.to_zarr(session.store, consolidated=False)
    session.commit("init")
    return storage


# --- Core Catalog Tests ---


def test_open_or_create(tmp_path, fake_store):
    """Verify that we can re-open an existing catalog without losing data."""
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
    """Basic registration and retrieval flow."""
    catalog.register(
        "sst", storage=fake_store, location="s3://bucket/sst/", owner="carbonplan"
    )
    entry = catalog.get("sst")
    assert entry.name == "sst"
    assert entry.location == "s3://bucket/sst/"
    assert entry.owner == "carbonplan"
    assert entry.format == "icechunk"


def test_register_with_optional_metadata(catalog, fake_store):
    """Ensure arbitrary metadata fields are preserved."""
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
    """Verify that list() returns all registered branches."""
    catalog.register(
        "sst", storage=fake_store, location="s3://bucket/sst/", owner="carbonplan"
    )
    catalog.register(
        "precip", storage=fake_store, location="s3://bucket/precip/", owner="noaa"
    )
    entries = catalog.list()
    names = {e.name for e in entries}
    assert names == {"sst", "precip"}


def test_deregister(catalog, fake_store):
    """Test branch deletion."""
    catalog.register(
        "sst", storage=fake_store, location="s3://bucket/sst/", owner="carbonplan"
    )
    catalog.deregister("sst")
    assert not any(e.name == "sst" for e in catalog.list())


def test_duplicate_register_raises(catalog, fake_store):
    """Ensure we don't accidentally overwrite a branch."""
    catalog.register(
        "sst", storage=fake_store, location="s3://bucket/sst/", owner="carbonplan"
    )
    with pytest.raises(ValueError, match="already registered"):
        catalog.register(
            "sst", storage=fake_store, location="s3://bucket/sst2/", owner="carbonplan"
        )


def test_missing_required_field_raises():
    """Verify schema enforcement."""
    from basal.schema import validate

    with pytest.raises(ValueError, match="Missing required fields"):
        # Missing 'format'
        validate({"location": "s3://bucket/sst/"})


# --- Update Logic Tests ---


def test_update_merges_fields(catalog, fake_store):
    """Verify that update adds new fields without losing old ones."""
    catalog.register(
        "sst", storage=fake_store, location="s3://bucket/sst/", owner="carbonplan"
    )
    catalog.update("sst", title="Sea Surface Temp", license="CC-BY-4.0")

    entry = catalog.get("sst")
    assert entry.metadata["title"] == "Sea Surface Temp"
    assert entry.metadata["license"] == "CC-BY-4.0"
    assert entry.owner == "carbonplan"
    assert entry.location == "s3://bucket/sst/"


def test_update_overwrites_field(catalog, fake_store):
    catalog.register(
        "sst",
        storage=fake_store,
        location="s3://bucket/sst/",
        owner="carbonplan",
        title="Old Title",
    )
    catalog.update("sst", title="New Title")

    entry = catalog.get("sst")
    assert entry.metadata["title"] == "New Title"


def test_update_preserves_other_fields(catalog, fake_store):
    catalog.register(
        "sst",
        storage=fake_store,
        location="s3://bucket/sst/",
        owner="carbonplan",
        keywords=["ocean", "sst"],
        license="CC-BY-4.0",
    )
    catalog.update("sst", title="Updated")

    entry = catalog.get("sst")
    assert entry.metadata["keywords"] == ["ocean", "sst"]
    assert entry.metadata["license"] == "CC-BY-4.0"


def test_update_entry_still_in_list(catalog, fake_store):
    catalog.register(
        "sst", storage=fake_store, location="s3://bucket/sst/", owner="carbonplan"
    )
    catalog.register(
        "precip", storage=fake_store, location="s3://bucket/precip/", owner="noaa"
    )
    catalog.update("sst", title="Updated SST")

    names = {e.name for e in catalog.list()}
    assert names == {"sst", "precip"}


def test_update_nonexistent_raises(catalog):
    with pytest.raises(icechunk.IcechunkError, match="ref not found"):
        catalog.update("ghost", title="nothing")


# --- Search Tests ---


def test_search_sql(catalog, fake_store):
    """Standard metadata filtering using DuckDB SQL (via search.sql)."""
    catalog.register(
        "sst", storage=fake_store, location="s3://bucket/sst/", owner="carbonplan"
    )
    catalog.register(
        "precip", storage=fake_store, location="s3://bucket/precip/", owner="noaa"
    )
    catalog.register(
        "wind", storage=fake_store, location="s3://bucket/wind/", owner="carbonplan"
    )

    results = catalog.search(
        "SELECT name FROM entries WHERE metadata->>'owner' = 'carbonplan' ORDER BY name"
    )
    names = {r[0] for r in results}
    assert names == {"sst", "wind"}


def test_search_similarity(catalog, fake_store):
    """Similarity search logic using a mock embedding function."""
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


def test_search_no_results(catalog, fake_store):
    """Verify SQL filtering returns empty list correctly."""
    catalog.register(
        "sst", storage=fake_store, location="s3://bucket/sst/", owner="carbonplan"
    )
    results = catalog.search(
        "SELECT name FROM entries WHERE metadata->>'owner' = 'nobody'"
    )
    assert results == []


# --- filter() Tests ---


@pytest.fixture
def catalog_with_bounds(catalog, fake_store):
    """Catalog with entries covering various time/space ranges."""
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
        # no end_datetime — open-ended
        bbox=[-180.0, -90.0, 0.0, 90.0],
    )
    catalog.register(
        "no-bounds",
        storage=fake_store,
        location="s3://b/no-bounds",
        # no temporal or spatial fields
    )
    return catalog


def test_filter_temporal_overlap(catalog_with_bounds):
    results = catalog_with_bounds.filter(time_start="2019", time_end="2021")
    names = {e.name for e in results}
    assert names == {"recent", "ongoing"}


def test_filter_temporal_start_only(catalog_with_bounds):
    results = catalog_with_bounds.filter(time_start="2015")
    names = {e.name for e in results}
    assert names == {"recent", "ongoing"}


def test_filter_temporal_end_only(catalog_with_bounds):
    results = catalog_with_bounds.filter(time_end="2005")
    names = {e.name for e in results}
    assert names == {"historical"}


def test_filter_spatial_overlap(catalog_with_bounds):
    results = catalog_with_bounds.filter(bbox=(-20.0, 40.0, 50.0, 60.0))
    names = {e.name for e in results}
    assert names == {"historical", "recent", "ongoing"}


def test_filter_combined(catalog_with_bounds):
    results = catalog_with_bounds.filter(
        time_start="2019", time_end="2022", bbox=(-20.0, 40.0, 50.0, 60.0)
    )
    names = {e.name for e in results}
    assert names == {"recent", "ongoing"}


def test_filter_warns_missing_temporal(catalog_with_bounds):
    with pytest.warns(UserWarning, match="start_datetime"):
        catalog_with_bounds.filter(time_start="2020")


def test_filter_warns_missing_bbox(catalog_with_bounds):
    with pytest.warns(UserWarning, match="bbox"):
        catalog_with_bounds.filter(bbox=(-10.0, -10.0, 10.0, 10.0))


def test_filter_no_args_returns_all(catalog_with_bounds):
    results = catalog_with_bounds.filter()
    assert len(results) == 4


def test_filter_year_string_parsed(catalog_with_bounds):
    # Ensure short ISO strings like "2020" are accepted
    results = catalog_with_bounds.filter(time_start="2020", time_end="2020")
    names = {e.name for e in results}
    assert "recent" in names
    assert "ongoing" in names


# --- refresh() Tests ---


def test_refresh_returns_stale_flags(catalog, fake_store):
    catalog.register("ds", storage=fake_store, location="s3://b/ds")
    result = catalog.refresh()
    assert "ds" in result
    assert result["ds"] is False


def test_refresh_warns_missing_snapshot_id(catalog, fake_store):
    # Register without dataset_snapshot_id by stripping it after registration
    catalog.register("ds", storage=fake_store, location="s3://b/ds")
    # Overwrite with no snapshot id
    catalog.update("ds", dataset_snapshot_id=None)
    # is_stale raises ValueError for None snapshot id; refresh should warn+skip
    with pytest.warns(UserWarning, match="dataset_snapshot_id"):
        result = catalog.refresh()
    assert "ds" not in result


# --- update_all_from_store() Tests ---


def test_update_all_from_store(catalog, fake_store):
    catalog.register("ds", storage=fake_store, location="s3://b/ds")
    before = catalog.get("ds").metadata.get("dataset_snapshot_id")
    catalog.update_all_from_store()
    after = catalog.get("ds").metadata.get("dataset_snapshot_id")
    # snapshot id should still be present (unchanged since no new commits)
    assert after == before


def test_update_all_from_store_warns_no_storage_config(catalog, fake_store):
    catalog.register("ds", storage=fake_store, location="s3://b/ds")
    # Remove storage_config so auto-resolution fails
    catalog.update("ds", storage_config=None)
    with pytest.warns(UserWarning, match="storage_config"):
        catalog.update_all_from_store()


# --- infer_extent / derive_extent Tests ---


@pytest.fixture
def geo_store(tmp_path):
    """Icechunk store with lat/lon/time coordinates for extent inference."""
    import pandas as pd
    import xarray as xr

    path = str(tmp_path / "geo_dataset")
    storage = icechunk.local_filesystem_storage(path)
    repo = icechunk.Repository.create(storage)
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
    return storage


def test_infer_extent_returns_fields(catalog, geo_store):
    catalog.register("geo", storage=geo_store, location="s3://b/geo")
    entry = catalog.get("geo")
    extent = entry.infer_extent(catalog, update=False)
    assert extent["bbox"] == [-20.0, -10.0, 20.0, 10.0]
    assert "start_datetime" in extent
    assert "end_datetime" in extent


def test_infer_extent_updates_catalog(catalog, geo_store):
    catalog.register("geo", storage=geo_store, location="s3://b/geo")
    entry = catalog.get("geo")
    entry.infer_extent(catalog, update=True)
    updated = catalog.get("geo")
    assert updated.metadata["bbox"] == [-20.0, -10.0, 20.0, 10.0]
    assert "start_datetime" in updated.metadata


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
        bbox=[-180.0, -90.0, 180.0, 90.0],  # explicit override
    )
    entry = catalog.get("geo")
    assert entry.metadata["bbox"] == [-180.0, -90.0, 180.0, 90.0]
