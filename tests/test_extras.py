import icechunk
import numpy as np
import pytest
import xarray as xr
from basal import IcechunkCatalog
from basal.search import similar, sql, sql_df
from basal.storage import storage_from_location


def _make_dataset_store(path: str) -> icechunk.Storage:
    """Create a minimal icechunk store with a zarr array at path."""
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
    owners = set(catalog.values("owner"))
    assert owners == {"NOAA", "NCEP"}

    kw = set(catalog.values("keywords"))
    assert {"sst", "air", "wind", "ocean", "atmosphere"} <= kw


def test_facets(catalog):
    facets = catalog.facets()
    assert facets["owner"]["NOAA"] == 2
    assert facets["owner"]["NCEP"] == 1
    assert facets["keywords"]["atmosphere"] == 2
    assert facets["license"]["public-domain"] == 2


def test_repr(catalog):
    r = repr(catalog)
    assert "3 entries" in r


def test_entry_repr(catalog):
    r = repr(catalog.get("sst"))
    assert "sst" in r and "NOAA" in r


def test_html_repr(catalog):
    html = catalog._repr_html_()
    assert "<table>" in html
    assert "sst" in html


def test_describe_and_print(catalog, capsys):
    catalog.describe("sst")
    out = capsys.readouterr().out
    assert "sst" in out
    assert "NOAA" in out

    catalog.print()
    out = capsys.readouterr().out
    assert "IcechunkCatalog" in out


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


def test_sql_json_array(catalog):
    rows = sql(
        catalog,
        "SELECT name FROM entries WHERE list_contains(CAST(metadata->'keywords' AS VARCHAR[]), 'atmosphere') ORDER BY name",
    )
    assert [r[0] for r in rows] == ["air", "wind"]


def test_storage_from_location_local(tmp_path):
    s = storage_from_location(str(tmp_path / "foo"))
    assert s is not None

    s = storage_from_location(f"file://{tmp_path}/bar")
    assert s is not None


def test_storage_from_location_unknown():
    with pytest.raises(ValueError, match="Unsupported location scheme"):
        storage_from_location("ftp://foo/bar")


def test_to_xarray_roundtrip(tmp_path):
    data_path = tmp_path / "dataset"
    storage = icechunk.local_filesystem_storage(str(data_path))
    repo = icechunk.Repository.create(storage)
    session = repo.writable_session("main")

    ds = xr.Dataset(
        {"temp": (("time", "x"), np.arange(12).reshape(3, 4).astype("float32"))},
        coords={"time": [0, 1, 2], "x": [10, 20, 30, 40]},
    )
    ds.to_zarr(session.store, mode="w")
    session.commit("write dataset")

    catalog_storage = icechunk.local_filesystem_storage(str(tmp_path / "catalog"))
    catalog = IcechunkCatalog.create(catalog_storage)
    catalog.register("temp", storage=storage, owner="test")

    entry = catalog.get("temp")
    opened = entry.to_xarray()
    xr.testing.assert_equal(ds, opened.compute())


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
    names = [e.name for e, _ in results]
    assert names[0] == "sst"


def test_similar_scores_in_range(catalog):
    embed_fn = _make_embed_fn(["sst", "ocean", "air", "atmosphere", "wind"])
    results = similar(catalog, "ocean", embed_fn=embed_fn, top_k=3)
    for _, score in results:
        assert 0.0 <= score <= 1.0


def test_similar_top_k(catalog):
    embed_fn = _make_embed_fn(["sst", "ocean", "air", "atmosphere", "wind"])
    results = similar(catalog, "anything", embed_fn=embed_fn, top_k=2)
    assert len(results) == 2


def test_similar_matches_arbitrary_field(tmp_path):
    store_a = _make_dataset_store(str(tmp_path / "a"))
    store_b = _make_dataset_store(str(tmp_path / "b"))
    storage = icechunk.local_filesystem_storage(str(tmp_path / "catalog"))
    cat = IcechunkCatalog.create(storage)
    cat.register(
        "a", storage=store_a, location="/tmp/a", owner="org", doi="10.1234/climate"
    )
    cat.register(
        "b", storage=store_b, location="/tmp/b", owner="org", doi="10.9999/other"
    )

    vocab = ["10.1234", "climate", "10.9999", "other"]
    embed_fn = _make_embed_fn(vocab)

    results = similar(cat, "10.1234 climate", embed_fn=embed_fn, top_k=2)
    assert results[0][0].name == "a"


def test_similar_matches_list_field(tmp_path):
    store_ice = _make_dataset_store(str(tmp_path / "ice"))
    store_rain = _make_dataset_store(str(tmp_path / "rain"))
    storage = icechunk.local_filesystem_storage(str(tmp_path / "catalog"))
    cat = IcechunkCatalog.create(storage)
    cat.register(
        "ice",
        storage=store_ice,
        location="/tmp/ice",
        owner="org",
        keywords=["cryosphere", "sea-ice"],
    )
    cat.register(
        "rain",
        storage=store_rain,
        location="/tmp/rain",
        owner="org",
        keywords=["precipitation", "hydrology"],
    )

    vocab = ["cryosphere", "sea-ice", "precipitation", "hydrology"]
    embed_fn = _make_embed_fn(vocab)

    results = similar(cat, "cryosphere sea-ice", embed_fn=embed_fn, top_k=2)
    assert results[0][0].name == "ice"


def test_similar_matches_dict_variable_keys(tmp_path):
    store_ocean = _make_dataset_store(str(tmp_path / "ocean"))
    store_atmos = _make_dataset_store(str(tmp_path / "atmos"))
    storage = icechunk.local_filesystem_storage(str(tmp_path / "catalog"))
    cat = IcechunkCatalog.create(storage)
    cat.register(
        "ocean",
        storage=store_ocean,
        location="/tmp/o",
        owner="org",
        variables={"sst": {}, "salinity": {}},
    )
    cat.register(
        "atmos",
        storage=store_atmos,
        location="/tmp/a",
        owner="org",
        variables={"tas": {}, "pr": {}},
    )

    vocab = ["sst", "salinity", "tas", "pr"]
    embed_fn = _make_embed_fn(vocab)

    results = similar(cat, "sst salinity", embed_fn=embed_fn, top_k=2)
    assert results[0][0].name == "ocean"


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


def test_history_update_appears(tmp_path):
    store = _make_dataset_store(str(tmp_path / "sst"))
    storage = icechunk.local_filesystem_storage(str(tmp_path / "catalog"))
    cat = IcechunkCatalog.create(storage)
    cat.register("sst", storage=store, location="s3://b/sst", owner="noaa")
    cat.update("sst", title="SST v2")

    hist = cat.history(name="sst")
    events = [r["event"] for r in hist]
    assert "registered" in events
    assert "updated" in events


def test_history_deregister_appears(tmp_path):
    store = _make_dataset_store(str(tmp_path / "sst"))
    storage = icechunk.local_filesystem_storage(str(tmp_path / "catalog"))
    cat = IcechunkCatalog.create(storage)
    cat.register("sst", storage=store, location="s3://b/sst", owner="noaa")
    cat.deregister("sst")

    hist = cat.history()
    events = [r["event"] for r in hist]
    assert "deregistered" in events
    assert any(r["name"] == "sst" and r["event"] == "deregistered" for r in hist)


def test_history_limit(tmp_path):
    storage = icechunk.local_filesystem_storage(str(tmp_path / "catalog"))
    cat = IcechunkCatalog.create(storage)
    for i in range(5):
        store = _make_dataset_store(str(tmp_path / f"ds{i}"))
        cat.register(f"ds{i}", storage=store, location=f"s3://b/ds{i}", owner="org")

    hist = cat.history(limit=3)
    assert len(hist) == 3


def test_history_snapshot_id_on_commits(tmp_path):
    store = _make_dataset_store(str(tmp_path / "sst"))
    storage = icechunk.local_filesystem_storage(str(tmp_path / "catalog"))
    cat = IcechunkCatalog.create(storage)
    cat.register("sst", storage=store, location="s3://b/sst", owner="noaa")

    hist = cat.history(name="sst")
    reg = next(r for r in hist if r["event"] == "registered")
    assert reg["snapshot_id"] is not None
    assert isinstance(reg["snapshot_id"], str)
