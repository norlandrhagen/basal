"""Tests for basal-specific logic: name validation, event-keyed history,
facet denylist, internal-key stripping, storage serialization, and virtual chunk roundtrip.
"""

import icechunk
import numpy as np
import pytest
import xarray as xr
from basal import Catalog, Entry
from basal.catalog import (
    FACET_DENYLIST,
    _derive_metadata_from_store,
    _strip_internal,
    _validate_name,
)
from basal.history import EVENT_KEY
from basal.storage import (
    _repo_config_from_virtual_chunks,
    _virtual_chunk_container_to_config,
    _virtual_chunk_credentials_from_config,
    http_storage,
    local_filesystem_storage,
    location_from_config,
    redirect_storage,
    s3_storage,
    storage_from_config,
)


@pytest.fixture
def catalog(tmp_path):
    storage = icechunk.local_filesystem_storage(str(tmp_path / "catalog"))
    return Catalog.create(storage)


@pytest.fixture
def fake_store(tmp_path):
    path = str(tmp_path / "fake_dataset")
    spec = local_filesystem_storage(path)
    repo = icechunk.Repository.create(spec.build())
    session = repo.writable_session("main")
    ds = xr.Dataset({"var": xr.DataArray([1.0], dims=["x"])})
    ds.to_zarr(session.store, consolidated=False)
    session.commit("init")
    return spec


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


# --- internal key stripping ---


def test_strip_internal_removes_dunder_keys():
    meta = {"owner": "org", "__event__": "registered", "__private__": "x", "title": "T"}
    assert _strip_internal(meta) == {"owner": "org", "title": "T"}


def test_internal_keys_not_exposed(catalog, fake_store):
    catalog.register("sst", storage=fake_store, owner="org")
    catalog.update("sst", title="Updated")
    for entry in [catalog.get("sst"), *catalog.list()]:
        assert EVENT_KEY not in entry.metadata
        assert entry.metadata.get("title") == "Updated"


# --- history uses event metadata ---


def test_history_event_read_from_metadata(catalog, fake_store):
    catalog.register("sst", storage=fake_store, owner="org")
    catalog.update("sst", title="v2")

    hist = catalog.history(name="sst")
    events = [r["event"] for r in hist]
    assert "registered" in events
    assert "updated" in events

    snap_id = catalog._repo.lookup_branch("sst")
    snap = catalog._repo.lookup_snapshot(snap_id)
    assert snap.metadata.get(EVENT_KEY) == "updated"


# --- facet denylist ---


def test_facets_excludes_denylisted_fields(catalog, fake_store):
    catalog.register(
        "sst",
        storage=fake_store,
        owner="org",
        description="a long free-text blurb",
        doi="10.1234/abc",
        title="SST",
    )
    facets = catalog.facets()
    for field in FACET_DENYLIST:
        assert field not in facets
    assert "owner" in facets
    assert "title" in facets


def test_facets_counts(catalog, fake_store):
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


# --- public API ---


def test_public_exports():
    import basal as pkg

    assert pkg.Catalog is Catalog
    assert pkg.Entry is Entry
    assert hasattr(pkg, "search")
    assert hasattr(pkg, "inspect")


# --- storage serialization roundtrip ---


def test_storage_spec_to_config_local(tmp_path):
    path = str(tmp_path / "store")
    assert local_filesystem_storage(path).to_config() == {"type": "local", "path": path}


def test_storage_spec_to_config_s3():
    spec = s3_storage(
        bucket="my-bucket", prefix="my/prefix", region="us-west-2", anonymous=True
    )
    assert spec.to_config() == {
        "type": "s3",
        "bucket": "my-bucket",
        "prefix": "my/prefix",
        "region": "us-west-2",
        "anonymous": True,
    }


def test_storage_spec_to_config_s3_force_path_style():
    spec = s3_storage(
        bucket="b",
        prefix="p",
        endpoint_url="https://nyu1.osn.mghpcc.org",
        force_path_style=True,
    )
    cfg = spec.to_config()
    assert cfg["force_path_style"] is True
    assert cfg["endpoint_url"] == "https://nyu1.osn.mghpcc.org"
    assert isinstance(storage_from_config(cfg), icechunk.Storage)


def test_storage_spec_roundtrip_build(tmp_path):
    path = str(tmp_path / "store")
    spec = local_filesystem_storage(path)
    rebuilt = storage_from_config(spec.to_config())
    assert rebuilt is not None


def test_storage_from_config_unknown_raises():
    with pytest.raises(ValueError, match="Unknown storage type"):
        storage_from_config({"type": "ftp", "bucket": "b", "prefix": "p"})


def test_register_derives_location_and_config(tmp_path):
    data_path = tmp_path / "ds"
    spec = local_filesystem_storage(str(data_path))
    repo = icechunk.Repository.create(spec.build())
    session = repo.writable_session("main")
    xr.Dataset({"v": xr.DataArray([1.0, 2.0], dims=["x"])}).to_zarr(
        session.store, consolidated=False
    )
    session.commit("init")

    cat = Catalog.create(icechunk.local_filesystem_storage(str(tmp_path / "catalog")))
    cat.register("v", storage=spec)

    entry = cat.get("v")
    assert entry.location == f"file://{data_path}"
    assert entry.metadata["storage_config"] == {"type": "local", "path": str(data_path)}


def test_register_location_override(tmp_path, fake_store):
    cat = Catalog.create(icechunk.local_filesystem_storage(str(tmp_path / "c")))
    cat.register("x", storage=fake_store, location="s3://custom/override")
    assert cat.get("x").location == "s3://custom/override"


# --- virtual chunk config roundtrip ---


def test_virtual_chunk_container_roundtrip():
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(
            "s3://my-bucket/",
            store=icechunk.ObjectStoreConfig.S3(
                icechunk.S3Options(region="us-east-1", anonymous=True)
            ),
        )
    )
    serialized = [
        _virtual_chunk_container_to_config(vc)
        for vc in config.virtual_chunk_containers.values()
    ]
    reconstructed = _repo_config_from_virtual_chunks(serialized)
    assert "s3://my-bucket/" in reconstructed.virtual_chunk_containers


@pytest.mark.parametrize(
    "url_prefix,store_cfg",
    [
        ("https://example.com/data/", icechunk.ObjectStoreConfig.Http(None)),
        ("http://example.com/data/", icechunk.ObjectStoreConfig.Http(None)),
        ("gs://my-bucket/data/", icechunk.ObjectStoreConfig.Gcs(None)),
    ],
)
def test_virtual_chunk_container_roundtrip_http_gcs(url_prefix, store_cfg):
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(url_prefix, store=store_cfg)
    )
    serialized = [
        _virtual_chunk_container_to_config(vc)
        for vc in config.virtual_chunk_containers.values()
    ]
    assert serialized == [{"url_prefix": url_prefix}]
    reconstructed = _repo_config_from_virtual_chunks(serialized)
    assert url_prefix in reconstructed.virtual_chunk_containers


@pytest.mark.parametrize(
    "containers",
    [
        [{"url_prefix": "https://example.com/data/"}],
        [{"url_prefix": "gs://my-bucket/data/"}],
        [{"url_prefix": "gs://my-bucket/data/", "anonymous": True}],
    ],
)
def test_virtual_chunk_credentials_http_gcs(containers):
    creds = _virtual_chunk_credentials_from_config(containers)
    assert creds is not None


# --- storage_from_config http / redirect ---


def test_storage_from_config_http():
    storage = storage_from_config(
        {"type": "http", "base_url": "https://example.com/catalog"}
    )
    assert isinstance(storage, icechunk.Storage)


def test_storage_from_config_redirect():
    storage = storage_from_config(
        {"type": "redirect", "base_url": "https://example.com/catalog"}
    )
    assert isinstance(storage, icechunk.Storage)


def test_http_redirect_spec_roundtrip():
    for spec, expected_type in [
        (http_storage(base_url="https://example.com/c"), "http"),
        (redirect_storage(base_url="https://example.com/c"), "redirect"),
    ]:
        config = spec.to_config()
        assert config == {"type": expected_type, "base_url": "https://example.com/c"}
        assert location_from_config(config) == "https://example.com/c"
        assert isinstance(storage_from_config(config), icechunk.Storage)


# --- values() handles unhashable values ---


def test_values_with_unhashable(catalog, fake_store):
    catalog.register("a", storage=fake_store, owner="org", dims={"t": 3})
    catalog.register("b", storage=fake_store, owner="org", dims={"t": 3})
    catalog.register("c", storage=fake_store, owner="org", dims={"t": 5})
    vals = catalog.values("dims")
    assert {"t": 3} in vals
    assert {"t": 5} in vals
    assert len(vals) == 2


# --- update remove_fields ---


def test_update_remove_fields(catalog, fake_store):
    catalog.register("sst", storage=fake_store, owner="org", extra="scratch")
    catalog.update("sst", remove_fields=["extra"])
    assert "extra" not in catalog.get("sst").metadata


def test_update_cannot_remove_required(catalog, fake_store):
    catalog.register("sst", storage=fake_store, owner="org")
    with pytest.raises(ValueError, match="Missing required fields"):
        catalog.update("sst", remove_fields=["location"])


# --- readonly catalog ---


def test_readonly_catalog_blocks_mutations(tmp_path, fake_store):
    storage_path = str(tmp_path / "ro-catalog")
    Catalog.create(icechunk.local_filesystem_storage(storage_path)).register(
        "sst", storage=fake_store, owner="org"
    )
    ro = Catalog.open(icechunk.local_filesystem_storage(storage_path), readonly=True)
    assert ro.get("sst").owner == "org"
    with pytest.raises(PermissionError, match="readonly"):
        ro.register("new", storage=fake_store)
    with pytest.raises(PermissionError, match="readonly"):
        ro.update("sst", title="x")
    with pytest.raises(PermissionError, match="readonly"):
        ro.deregister("sst")


# --- get missing entry ---


def test_get_missing_entry_raises_keyerror(catalog):
    with pytest.raises(KeyError, match="No entry named"):
        catalog.get("does-not-exist")


# --- validate does not mutate ---


def test_validate_does_not_mutate_input():
    from basal.schema import finalize, validate

    meta = {"location": "s3://b/p", "format": "zarr", "bbox": [0.0, 0.0, 1.0, 1.0]}
    before = dict(meta)
    validate(meta)
    assert meta == before
    finalized = finalize(meta)
    assert meta == before
    assert "geometry" in finalized


# --- to_stac matches API server conversion ---


def test_to_stac_includes_entries_without_bbox(catalog, fake_store):
    catalog.register("no-bbox", storage=fake_store, owner="org")
    catalog.register(
        "with-bbox", storage=fake_store, owner="org", bbox=[0.0, 0.0, 10.0, 10.0]
    )
    result = catalog.to_stac()
    by_id = {i["id"]: i for i in result["items"]}
    assert by_id["no-bbox"]["geometry"] is None
    assert by_id["no-bbox"]["bbox"] is None
    assert by_id["with-bbox"]["bbox"] == [0.0, 0.0, 10.0, 10.0]
    assert result["collection"]["extent"]["spatial"]["bbox"] == [[0.0, 0.0, 10.0, 10.0]]


# --- filter: touching bboxes count as overlap ---


def test_filter_bbox_touching_edge_included(catalog, fake_store):
    catalog.register(
        "touch", storage=fake_store, owner="org", bbox=[0.0, 0.0, 10.0, 10.0]
    )
    names = {e.name for e in catalog.filter(bbox=(10.0, 0.0, 20.0, 10.0))}
    assert "touch" in names
