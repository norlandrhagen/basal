import icechunk
import numpy as np
import pytest
from basal import Catalog, FederatedCatalog
from basal import storage as st


@pytest.fixture
def ds_spec(tmp_path):
    """A real, openable local icechunk dataset store (StorageSpec)."""
    import xarray as xr

    path = str(tmp_path / "dataset")
    spec = st.local_filesystem_storage(path)
    repo = icechunk.Repository.create(spec.build())
    session = repo.writable_session("main")
    xr.Dataset({"var": xr.DataArray([1.0, 2.0], dims=["x"])}).to_zarr(
        session.store, consolidated=False
    )
    session.commit("init")
    return spec


@pytest.fixture
def cat_a(tmp_path, ds_spec):
    cat = Catalog.create(
        icechunk.local_filesystem_storage(str(tmp_path / "cat_a")),
        name="alpha",
        owner="carbonplan",
        description="catalog alpha",
    )
    cat.register("sst", storage=ds_spec, description="sea surface temperature")
    cat.register(
        "wind",
        storage=ds_spec,
        start_datetime="2000-01-01",
        end_datetime="2010-12-31",
    )
    return cat


@pytest.fixture
def cat_b(tmp_path, ds_spec):
    cat = Catalog.create(
        icechunk.local_filesystem_storage(str(tmp_path / "cat_b")),
        name="beta",
        owner="noaa",
    )
    # 'sst' collides with cat_a on purpose to exercise namespacing.
    cat.register("sst", storage=ds_spec, description="soil moisture on land")
    cat.register(
        "precip",
        storage=ds_spec,
        start_datetime="2018-01-01",
        end_datetime="2023-12-31",
    )
    return cat


# --- catalog-level identity ---


def test_catalog_identity(tmp_path):
    cat = Catalog.create(
        icechunk.local_filesystem_storage(str(tmp_path / "c")),
        name="alpha",
        owner="carbonplan",
        description="hello",
    )
    assert cat.name == "alpha"
    assert cat.owner == "carbonplan"
    assert cat.info == {
        "name": "alpha",
        "owner": "carbonplan",
        "description": "hello",
    }


def test_catalog_identity_unset(tmp_path):
    cat = Catalog.create(icechunk.local_filesystem_storage(str(tmp_path / "c")))
    assert cat.name is None
    assert cat.info == {}


def test_set_info(tmp_path):
    cat = Catalog.create(icechunk.local_filesystem_storage(str(tmp_path / "c")))
    cat.set_info(name="late", owner="org")
    assert cat.name == "late"
    assert cat.owner == "org"


def test_identity_does_not_leak_into_entries(cat_a):
    # catalog-level metadata lives on main and must not appear as an entry.
    assert {e.name for e in cat_a.list()} == {"sst", "wind"}


# --- union / namespacing ---


def test_list_union_namespaced(cat_a, cat_b):
    fed = FederatedCatalog({"a": cat_a, "b": cat_b})
    names = {e.name for e in fed.list()}
    assert names == {"a/sst", "a/wind", "b/sst", "b/precip"}
    assert len(fed.list()) == len(cat_a.list()) + len(cat_b.list())
    by_name = {e.name: e for e in fed.list()}
    assert by_name["a/sst"].source == "a"
    assert by_name["b/precip"].source == "b"


def test_get_dispatch(cat_a, cat_b):
    fed = FederatedCatalog({"a": cat_a, "b": cat_b})
    e = fed.get("b/precip")
    assert e.name == "b/precip"
    assert e.source == "b"
    assert e.metadata["start_datetime"] == "2018-01-01"


def test_get_unknown_alias_raises(cat_a):
    fed = FederatedCatalog({"a": cat_a})
    with pytest.raises(KeyError, match="known"):
        fed.get("zzz/sst")
    with pytest.raises(KeyError):
        fed.get("noslash")


def test_default_alias_from_name(cat_a, cat_b):
    fed = FederatedCatalog([cat_a, cat_b])
    assert sorted(fed.members) == ["alpha", "beta"]
    assert {e.name for e in fed.list()} == {
        "alpha/sst",
        "alpha/wind",
        "beta/sst",
        "beta/precip",
    }


def test_unnamed_member_without_alias_raises(tmp_path, ds_spec):
    cat = Catalog.create(icechunk.local_filesystem_storage(str(tmp_path / "anon")))
    cat.register("x", storage=ds_spec)
    with pytest.raises(ValueError, match="no name"):
        FederatedCatalog([cat])


def test_duplicate_alias_raises(cat_a, cat_b):
    with pytest.raises(ValueError, match="Duplicate"):
        FederatedCatalog([("dup", cat_a), ("dup", cat_b)])


# --- list-derived ops span members ---


def test_filter_across_members(cat_a, cat_b):
    fed = FederatedCatalog({"a": cat_a, "b": cat_b})
    names = {e.name for e in fed.filter(time_start="2019", time_end="2021")}
    assert names == {"b/precip"}


def test_facets_values_fields(cat_a, cat_b):
    fed = FederatedCatalog({"a": cat_a, "b": cat_b})
    assert "location" in fed.fields()
    facets = fed.facets()
    assert facets["format"]["icechunk"] == 4


def test_sql_source_filter(cat_a, cat_b):
    fed = FederatedCatalog({"a": cat_a, "b": cat_b})
    rows = fed.sql("SELECT name FROM entries WHERE source = 'b' ORDER BY name")
    assert {r[0] for r in rows} == {"b/precip", "b/sst"}


def test_plain_catalog_source_is_empty(cat_a):
    rows = cat_a.sql("SELECT name FROM entries WHERE source = ''")
    assert {r[0] for r in rows} == {"sst", "wind"}


def test_search_across_members(cat_a, cat_b):
    from basal.search import similar

    fed = FederatedCatalog({"a": cat_a, "b": cat_b})

    def mock_embed(texts):
        vecs = []
        for t in texts:
            t = t.lower()
            if any(w in t for w in ["ocean", "sea", "surface", "temperature"]):
                vecs.append(np.array([1.0, 0.0], dtype=np.float32))
            else:
                vecs.append(np.array([0.0, 1.0], dtype=np.float32))
        return vecs

    results = similar(fed, "ocean temperature", embed_fn=mock_embed, top_k=1)
    assert results[0][0].name == "a/sst"


# --- membership ---


def test_add_remove(cat_a, cat_b):
    fed = FederatedCatalog({"a": cat_a})
    fed.add("b", cat_b)
    assert sorted(fed.members) == ["a", "b"]
    fed.remove("a")
    assert sorted(fed.members) == ["b"]


def test_add_duplicate_alias_raises(cat_a):
    fed = FederatedCatalog({"a": cat_a})
    with pytest.raises(ValueError, match="already present"):
        fed.add("a", cat_a)


# --- to_xarray through federation ---


def test_to_xarray_live(cat_a, cat_b):
    fed = FederatedCatalog({"a": cat_a, "b": cat_b})
    ds = fed.get("a/sst").to_xarray()
    assert "var" in ds


# --- materialize (persistent merge) ---


def test_materialize(tmp_path, cat_a, cat_b):
    fed = FederatedCatalog({"a": cat_a, "b": cat_b})
    merged = fed.materialize(
        icechunk.local_filesystem_storage(str(tmp_path / "merged"))
    )
    assert {e.name for e in merged.list()} == {
        "a/sst",
        "a/wind",
        "b/sst",
        "b/precip",
    }
    # merged is standalone and openable
    ds = merged.get("b/precip").to_xarray()
    assert "var" in ds
    # metadata preserved
    assert merged.get("b/precip").metadata["start_datetime"] == "2018-01-01"


def test_materialize_records_source_identity(tmp_path, cat_a, cat_b):
    fed = FederatedCatalog({"a": cat_a, "b": cat_b})
    merged = fed.materialize(
        icechunk.local_filesystem_storage(str(tmp_path / "merged_src"))
    )
    sources = merged.info["sources"]
    assert sources["a"] == {
        "name": "alpha",
        "owner": "carbonplan",
        "description": "catalog alpha",
    }
    assert sources["b"] == {"name": "beta", "owner": "noaa"}


def test_catalog_merge_sugar(tmp_path, cat_a, cat_b):
    merged = Catalog.merge(
        [cat_a, cat_b],
        icechunk.local_filesystem_storage(str(tmp_path / "merged2")),
    )
    assert isinstance(merged, Catalog)
    assert {e.name for e in merged.list()} == {
        "alpha/sst",
        "alpha/wind",
        "beta/sst",
        "beta/precip",
    }
    assert "var" in merged.get("alpha/wind").to_xarray()


def test_repr(cat_a, cat_b):
    fed = FederatedCatalog({"a": cat_a, "b": cat_b})
    assert "FederatedCatalog" in repr(fed)
    assert "4 entries" in repr(fed)


# --- alias validation ---


def test_alias_with_slash_raises(cat_a, cat_b):
    with pytest.raises(ValueError, match="may not contain"):
        FederatedCatalog({"a/b": cat_a})


def test_add_alias_with_slash_raises(cat_a):
    fed = FederatedCatalog({"a": cat_a})
    with pytest.raises(ValueError, match="may not contain"):
        fed.add("x/y", cat_a)


# --- member-failure handling ---


class _BrokenCatalog:
    name = "broken"

    def list(self):
        raise RuntimeError("store unreachable")


def test_list_strict_propagates_member_failure(cat_a):
    fed = FederatedCatalog({"a": cat_a, "bad": _BrokenCatalog()})
    with pytest.raises(RuntimeError, match="member 'bad' failed"):
        fed.list()


def test_list_non_strict_skips_failed_member(cat_a):
    fed = FederatedCatalog({"a": cat_a, "bad": _BrokenCatalog()}, strict=False)
    with pytest.warns(UserWarning, match="member 'bad' skipped"):
        names = {e.name for e in fed.list()}
    assert names == {"a/sst", "a/wind"}
