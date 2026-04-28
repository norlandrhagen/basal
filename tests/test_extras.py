import icechunk
import pytest
import xarray as xr
from basal import IcechunkCatalog
from basal.search import similar, sql, sql_df
from basal.storage import storage_from_location


def _make_dataset_store(path: str) -> icechunk.Storage:
    storage = icechunk.local_filesystem_storage(path)
    repo = icechunk.Repository.create(storage)
    session = repo.writable_session("main")
    ds = xr.Dataset({"var": xr.DataArray([1.0], dims=["x"])})
    ds.to_zarr(session.store, consolidated=False)
    session.commit("init")
    return storage


@pytest.fixture
def fake_store(tmp_path):
    return _make_dataset_store(str(tmp_path / "fake_dataset"))


@pytest.fixture
def catalog(tmp_path):
    storage = icechunk.local_filesystem_storage(str(tmp_path / "catalog"))
    c = IcechunkCatalog.create(storage)
    sst = _make_dataset_store(str(tmp_path / "data-sst"))
    air = _make_dataset_store(str(tmp_path / "data-air"))
    wind = _make_dataset_store(str(tmp_path / "data-wind"))
    c.register(
        "sst",
        storage=sst,
        location=str(tmp_path / "data-sst"),
        owner="NOAA",
        title="Sea Surface Temp",
        keywords=["sst", "ocean"],
        license="public-domain",
    )
    c.register(
        "air",
        storage=air,
        location=str(tmp_path / "data-air"),
        owner="NCEP",
        title="Air Temp",
        keywords=["air", "atmosphere"],
        license="public-domain",
    )
    c.register(
        "wind",
        storage=wind,
        location=str(tmp_path / "data-wind"),
        owner="NOAA",
        title="Wind",
        keywords=["wind", "atmosphere"],
        license="CC-BY-4.0",
    )
    return c


def test_fields(catalog):
    fields = catalog.fields()
    assert {"location", "owner", "format", "title", "keywords", "license"} <= fields


def test_values(catalog):
    assert set(catalog.values("owner")) == {"NOAA", "NCEP"}
    assert {"sst", "air", "wind", "ocean", "atmosphere"} <= set(
        catalog.values("keywords")
    )


def test_facets(catalog):
    facets = catalog.facets()
    assert facets["owner"]["NOAA"] == 2
    assert facets["owner"]["NCEP"] == 1
    assert facets["keywords"]["atmosphere"] == 2
    assert facets["license"]["public-domain"] == 2


def test_sql_search(catalog):
    rows = sql(
        catalog,
        "SELECT name FROM entries WHERE metadata->>'owner' = 'NOAA' ORDER BY name",
    )
    assert [r[0] for r in rows] == ["sst", "wind"]


def test_sql_df(catalog):
    df = sql_df(
        catalog, "SELECT name, metadata->>'owner' AS owner FROM entries ORDER BY name"
    )
    assert list(df["name"]) == ["air", "sst", "wind"]
    assert list(df["owner"]) == ["NCEP", "NOAA", "NOAA"]


def test_storage_from_location_local(tmp_path):
    assert storage_from_location(str(tmp_path / "foo")) is not None
    assert storage_from_location(f"file://{tmp_path}/bar") is not None


def test_storage_from_location_unknown():
    with pytest.raises(ValueError, match="Unsupported location scheme"):
        storage_from_location("ftp://foo/bar")


def _make_embed_fn(vocab):
    def embed_fn(texts):
        vecs = []
        for text in texts:
            words = set(text.lower().split())
            vec = [1.0 if w in words else 0.0 for w in vocab]
            norm = sum(x**2 for x in vec) ** 0.5
            vecs.append([x / norm if norm > 0 else 0.0 for x in vec])
        return vecs

    return embed_fn


def test_similar_ranks_correctly(catalog):
    embed_fn = _make_embed_fn(["sst", "ocean", "air", "atmosphere", "wind"])
    results = similar(catalog, "sst ocean", embed_fn=embed_fn, top_k=3)
    assert len(results) == 3
    assert results[0][0].name == "sst"


def test_similar_empty_catalog(tmp_path):
    storage = icechunk.local_filesystem_storage(str(tmp_path / "empty"))
    cat = IcechunkCatalog.create(storage)
    results = similar(cat, "ocean", embed_fn=lambda texts: [[1.0] * 4] * len(texts))
    assert results == []


def test_history_all(catalog):
    hist = catalog.history()
    events = [r["event"] for r in hist]
    names = [r["name"] for r in hist]
    assert events.count("registered") == 3
    assert set(names) == {"sst", "air", "wind"}
    assert hist == sorted(hist, key=lambda r: r["timestamp"], reverse=True)


def test_history_name_filter(catalog):
    hist = catalog.history(name="sst")
    assert all(r["name"] == "sst" for r in hist)
    assert any(r["event"] == "registered" for r in hist)


def test_history_events(tmp_path):
    store = _make_dataset_store(str(tmp_path / "sst"))
    cat = IcechunkCatalog.create(
        icechunk.local_filesystem_storage(str(tmp_path / "catalog"))
    )
    cat.register("sst", storage=store, location="s3://b/sst", owner="noaa")
    cat.update("sst", title="SST v2")
    cat.deregister("sst")

    hist = cat.history()
    events = [r["event"] for r in hist]
    assert "registered" in events
    assert "updated" in events
    assert "deregistered" in events


def test_history_limit(tmp_path):
    storage = icechunk.local_filesystem_storage(str(tmp_path / "catalog"))
    cat = IcechunkCatalog.create(storage)
    for i in range(5):
        store = _make_dataset_store(str(tmp_path / f"ds{i}"))
        cat.register(f"ds{i}", storage=store, location=f"s3://b/ds{i}", owner="org")
    assert len(cat.history(limit=3)) == 3
