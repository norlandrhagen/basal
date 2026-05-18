"""Tests for the basal STAC API server."""

from __future__ import annotations

import pytest

icechunk = pytest.importorskip("icechunk")
fastapi = pytest.importorskip("fastapi")
httpx = pytest.importorskip("httpx")

import xarray as xr  # noqa: E402
from basal import IcechunkCatalog  # noqa: E402
from basal.stac_api import create_app  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402


@pytest.fixture
def catalog(tmp_path):
    storage = icechunk.local_filesystem_storage(str(tmp_path / "catalog"))
    return IcechunkCatalog.open_or_create(storage)


@pytest.fixture
def fake_store(tmp_path):
    path = str(tmp_path / "ds.icechunk")
    storage = icechunk.local_filesystem_storage(path)
    repo = icechunk.Repository.create(storage)
    session = repo.writable_session("main")
    ds = xr.Dataset({"var": xr.DataArray([1.0, 2.0], dims=["x"])})
    ds.to_zarr(session.store, consolidated=False)
    session.commit("init")
    return storage


@pytest.fixture
def populated_catalog(catalog, fake_store):
    catalog.register(
        "ds-with-bbox",
        storage=fake_store,
        owner="test-org",
        title="Dataset With Bbox",
        bbox=[-180.0, -90.0, 180.0, 90.0],
        start_datetime="2020-01-01T00:00:00Z",
        end_datetime="2021-01-01T00:00:00Z",
        tags=["global", "test"],
        license="CC-BY-4.0",
    )
    catalog.register(
        "ds-no-bbox",
        storage=fake_store,
        owner="test-org",
        title="Dataset Without Bbox",
    )
    return catalog


@pytest.fixture
def client(populated_catalog):
    app = create_app(populated_catalog)
    return TestClient(app)


# --- landing / conformance ---


def test_landing_page(client):
    r = client.get("/")
    assert r.status_code == 200
    data = r.json()
    assert data["type"] == "Catalog"
    assert data["stac_version"] == "1.0.0"
    assert "conformsTo" in data
    assert any("core" in c for c in data["conformsTo"])


def test_conformance(client):
    r = client.get("/conformance")
    assert r.status_code == 200
    assert "conformsTo" in r.json()


# --- collections ---


def test_collections(client):
    r = client.get("/collections")
    assert r.status_code == 200
    data = r.json()
    assert len(data["collections"]) == 1
    assert data["collections"][0]["id"] == "basal-catalog"


def test_collection_by_id(client):
    r = client.get("/collections/basal-catalog")
    assert r.status_code == 200
    assert r.json()["id"] == "basal-catalog"


def test_collection_not_found(client):
    r = client.get("/collections/nonexistent")
    assert r.status_code == 404


# --- items ---


def test_items_returns_all(client):
    r = client.get("/collections/basal-catalog/items?limit=100")
    assert r.status_code == 200
    data = r.json()
    assert data["type"] == "FeatureCollection"
    assert len(data["features"]) == 2


def test_items_pagination(client):
    r1 = client.get("/collections/basal-catalog/items?limit=1")
    assert r1.status_code == 200
    d1 = r1.json()
    assert len(d1["features"]) == 1
    assert d1["numberMatched"] == 2
    links = {link["rel"]: link for link in d1["links"]}
    assert "next" in links

    next_href = links["next"]["href"]
    r2 = client.get(next_href)
    assert r2.status_code == 200
    d2 = r2.json()
    assert len(d2["features"]) == 1
    assert d2["features"][0]["id"] != d1["features"][0]["id"]


def test_item_by_id(client):
    r = client.get("/collections/basal-catalog/items/ds-with-bbox")
    assert r.status_code == 200
    data = r.json()
    assert data["id"] == "ds-with-bbox"
    assert data["type"] == "Feature"
    assert data["bbox"] is not None
    assert data["geometry"] is not None


def test_item_no_bbox_has_null_geometry(client):
    r = client.get("/collections/basal-catalog/items/ds-no-bbox")
    assert r.status_code == 200
    data = r.json()
    assert data["geometry"] is None
    assert data["bbox"] is None


def test_item_not_found(client):
    r = client.get("/collections/basal-catalog/items/missing")
    assert r.status_code == 404


def test_item_stac_fields(client):
    r = client.get("/collections/basal-catalog/items/ds-with-bbox")
    data = r.json()
    props = data["properties"]
    assert props["title"] == "Dataset With Bbox"
    assert props["license"] == "CC-BY-4.0"
    assert "global" in props["keywords"]
    assert props["start_datetime"] == "2020-01-01T00:00:00Z"
    assert "data" in data["assets"]


# --- search ---


def test_search_get_no_filters(client):
    r = client.get("/search")
    assert r.status_code == 200
    data = r.json()
    assert data["type"] == "FeatureCollection"
    assert len(data["features"]) == 2


def test_search_get_bbox(client):
    r = client.get("/search?bbox=-180,-90,180,90")
    assert r.status_code == 200
    data = r.json()
    # only ds-with-bbox has a bbox
    assert all(f["bbox"] is not None for f in data["features"])


def test_search_get_ids(client):
    r = client.get("/search?ids=ds-with-bbox")
    assert r.status_code == 200
    data = r.json()
    assert len(data["features"]) == 1
    assert data["features"][0]["id"] == "ds-with-bbox"


def test_search_post(client):
    r = client.post("/search", json={"ids": ["ds-with-bbox"], "limit": 5})
    assert r.status_code == 200
    data = r.json()
    assert len(data["features"]) == 1
    assert data["features"][0]["id"] == "ds-with-bbox"


def test_search_post_bbox(client):
    r = client.post("/search", json={"bbox": [-180.0, -90.0, 180.0, 90.0]})
    assert r.status_code == 200
    data = r.json()
    assert len(data["features"]) >= 1


def test_item_root_endpoint(client):
    """Root /items/{id} endpoint — STAC browsers navigate here from item links."""
    r = client.get("/items/ds-with-bbox")
    assert r.status_code == 200
    data = r.json()
    assert data["id"] == "ds-with-bbox"


def test_item_root_not_found(client):
    r = client.get("/items/nonexistent")
    assert r.status_code == 404


def test_item_has_self_link(client):
    r = client.get("/collections/basal-catalog/items/ds-with-bbox")
    data = r.json()
    links = {link["rel"]: link for link in data["links"]}
    assert "self" in links
    assert "ds-with-bbox" in links["self"]["href"]
    assert "collection" in links


def test_item_has_collection_field(client):
    r = client.get("/collections/basal-catalog/items/ds-with-bbox")
    data = r.json()
    assert data.get("collection") == "basal-catalog"


def test_collection_temporal_extent(client):
    r = client.get("/collections/basal-catalog")
    data = r.json()
    interval = data["extent"]["temporal"]["interval"][0]
    # ds-with-bbox has start_datetime and end_datetime
    assert interval[0] is not None  # t_start derived from entries
