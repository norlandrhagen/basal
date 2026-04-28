import icechunk
import numpy as np
import pytest
import xarray as xr
from basal import IcechunkCatalog
from basal.inspect import inspect_store, stable_attrs


@pytest.fixture
def dataset_store(tmp_path):
    data_path = tmp_path / "dataset"
    storage = icechunk.local_filesystem_storage(str(data_path))
    repo = icechunk.Repository.create(storage)
    session = repo.writable_session("main")

    lat = np.array([-90.0, 0.0, 90.0])
    lon = np.array([-180.0, 0.0, 180.0])
    time = np.array([0, 1, 2], dtype="datetime64[D]")

    ds = xr.Dataset(
        {
            "sst": (
                ("time", "lat", "lon"),
                np.random.rand(3, 3, 3).astype("float32"),
                {
                    "units": "degC",
                    "long_name": "Sea Surface Temperature",
                    "standard_name": "sea_surface_temperature",
                },
            )
        },
        coords={"lat": lat, "lon": lon, "time": time},
        attrs={
            "title": "Test SST Dataset",
            "institution": "Test Org",
            "conventions": "CF-1.8",
            "source": "synthetic",
        },
    )
    ds.to_zarr(session.store, mode="w")
    session.commit("write dataset")
    return str(data_path)


def _storage(path: str) -> icechunk.Storage:
    return icechunk.local_filesystem_storage(path)


def test_inspect_store(dataset_store):
    info = inspect_store(_storage(dataset_store))
    assert "sst" in info["variables"]
    assert info["variables"]["sst"]["dtype"] == "float32"
    assert info["dims"] == {"time": 3, "lat": 3, "lon": 3}
    assert info["global_attrs"]["title"] == "Test SST Dataset"
    assert info["global_attrs"]["conventions"] == "CF-1.8"
    assert info["variables"]["sst"]["attrs"]["units"] == "degC"
    assert (
        info["variables"]["sst"]["attrs"]["standard_name"] == "sea_surface_temperature"
    )
    assert "dataset_snapshot_id" in info
    assert isinstance(info["dataset_snapshot_id"], str)


def test_stable_attrs(dataset_store):
    info = inspect_store(_storage(dataset_store))
    attrs = stable_attrs(info)
    assert "dims" not in attrs
    assert attrs.get("title") == "Test SST Dataset"
    assert attrs.get("conventions") == "CF-1.8"
    assert "sst" in attrs["variables"]
    assert (
        attrs["variables"]["sst"]["attrs"]["standard_name"] == "sea_surface_temperature"
    )
    assert "shape" not in attrs["variables"]["sst"]


def test_register_derives_cf_attrs(tmp_path, dataset_store):
    catalog_storage = icechunk.local_filesystem_storage(str(tmp_path / "catalog"))
    catalog = IcechunkCatalog.create(catalog_storage)
    catalog.register("sst", storage=_storage(dataset_store), owner="test-org")

    entry = catalog.get("sst")
    assert entry.metadata["title"] == "Test SST Dataset"
    assert entry.metadata["conventions"] == "CF-1.8"
    assert "sst" in entry.metadata["variables"]
    assert "dataset_snapshot_id" in entry.metadata


def test_register_explicit_wins(tmp_path, dataset_store):
    catalog_storage = icechunk.local_filesystem_storage(str(tmp_path / "catalog"))
    catalog = IcechunkCatalog.create(catalog_storage)
    catalog.register(
        "sst",
        storage=_storage(dataset_store),
        owner="test-org",
        title="My Override Title",
    )
    assert catalog.get("sst").metadata["title"] == "My Override Title"


def test_is_stale(tmp_path, dataset_store):
    catalog_storage = icechunk.local_filesystem_storage(str(tmp_path / "catalog"))
    catalog = IcechunkCatalog.create(catalog_storage)
    catalog.register("sst", storage=_storage(dataset_store), owner="test-org")
    entry = catalog.get("sst")

    assert entry.is_stale() is False

    repo = icechunk.Repository.open(_storage(dataset_store))
    session = repo.writable_session("main")
    session.commit("new data", allow_empty=True)

    assert catalog.get("sst").is_stale() is True
