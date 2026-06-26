import icechunk
import numpy as np
import pytest
import xarray as xr
from basal import Catalog
from basal import storage as st


@pytest.fixture
def catalog(tmp_path):
    storage = icechunk.local_filesystem_storage(str(tmp_path / "catalog"))
    return Catalog.open_or_create(storage)


@pytest.fixture
def tree_store(tmp_path):
    """Local icechunk repo holding a DataTree with two leaf nodes under empty parents."""
    spec = st.local_filesystem_storage(str(tmp_path / "tree"))
    repo = icechunk.Repository.create(spec.build())
    session = repo.writable_session("main")

    tree = xr.DataTree.from_dict(
        {
            "ACCESS-CM2/ssp245": xr.Dataset(
                {"tas": xr.DataArray(np.arange(3.0), dims=["time"])},
                attrs={"title": "ACCESS ssp245"},
            ),
            "ACCESS-CM2/ssp585": xr.Dataset(
                {"tas": xr.DataArray(np.arange(3.0) + 10, dims=["time"])},
                attrs={"title": "ACCESS ssp585"},
            ),
        }
    )
    tree.to_zarr(session.store, consolidated=False)
    session.commit("write tree")
    return spec


def test_register_datatree_fans_out_leaves(catalog, tree_store):
    names = catalog.register_datatree("cmip6", storage=tree_store)
    assert set(names) == {"cmip6/ACCESS-CM2/ssp245", "cmip6/ACCESS-CM2/ssp585"}
    # empty parent node "ACCESS-CM2" dropped by leaves_only
    assert "cmip6/ACCESS-CM2" not in names


def test_datatree_entry_records_group(catalog, tree_store):
    catalog.register_datatree("cmip6", storage=tree_store)
    entry = catalog.get("cmip6/ACCESS-CM2/ssp245")
    assert entry.group == "ACCESS-CM2/ssp245"
    assert entry.metadata["var_names"] == ["tas"]


def test_register_datatree_name_and_metadata_fn(catalog, tree_store):
    def name_fn(path):
        model, scenario = path.split("/")
        return f"{model}_{scenario}"

    def metadata_fn(path, info):
        model, scenario = path.split("/")
        return {
            "model": model,
            "scenario": scenario,
            "title": info["global_attrs"]["title"],
        }

    names = catalog.register_datatree(
        "cmip6",
        storage=tree_store,
        name_fn=name_fn,
        metadata_fn=metadata_fn,
        owner="NASA",
    )
    assert set(names) == {"ACCESS-CM2_ssp245", "ACCESS-CM2_ssp585"}
    e = catalog.get("ACCESS-CM2_ssp245")
    assert e.metadata["model"] == "ACCESS-CM2"
    assert e.metadata["scenario"] == "ssp245"
    assert e.metadata["title"] == "ACCESS ssp245"
    assert e.owner == "NASA"  # shared field


def test_register_datatree_idempotent_rerun(catalog, tree_store):
    catalog.register_datatree("cmip6", storage=tree_store)
    # second run must not raise (update=True default)
    names = catalog.register_datatree("cmip6", storage=tree_store, owner="NASA")
    assert len(names) == 2
    assert catalog.get("cmip6/ACCESS-CM2/ssp245").owner == "NASA"


def test_datatree_entry_to_xarray_opens_group(catalog, tree_store):
    catalog.register_datatree("cmip6", storage=tree_store)
    ds = catalog.get("cmip6/ACCESS-CM2/ssp585").to_xarray()
    assert "tas" in ds.data_vars
    assert float(ds["tas"][0]) == 10.0


def test_register_group_single_node(catalog, tree_store):
    catalog.register("just245", storage=tree_store, group="ACCESS-CM2/ssp245")
    entry = catalog.get("just245")
    assert entry.group == "ACCESS-CM2/ssp245"
    # derived attrs come from the node, not the empty root
    assert entry.metadata["var_names"] == ["tas"]
    assert float(entry.to_xarray()["tas"][0]) == 0.0


def test_to_datatree_opens_subtree(catalog, tree_store):
    catalog.register("wholetree", storage=tree_store)
    dt = catalog.get("wholetree").to_datatree()
    assert float(dt["ACCESS-CM2/ssp245"]["tas"][0]) == 0.0
    assert float(dt["ACCESS-CM2/ssp585"]["tas"][0]) == 10.0
