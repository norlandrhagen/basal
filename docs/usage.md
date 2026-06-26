# Getting started

## Create a catalog

`open_or_create()` initializes a new catalog if none exists, or opens the existing one:

```python
from basal import Catalog
import icechunk

catalog_storage = icechunk.s3_storage(
    bucket="my-bucket", prefix="my-catalog", region="us-west-2", from_env=True
)
catalog = Catalog.open_or_create(catalog_storage)
```

For consumers of a shared catalog, open read-only — any mutating method then
raises `PermissionError`:

```python
catalog = Catalog.open(catalog_storage, readonly=True)
```

See [Opening modes](reference.md#opening-modes) for HTTP / no-credential access.

## Register entries

A `basal.storage` spec (like `s3_storage`) holds the store location and its config.
basal records it on the entry, so `entry.to_xarray()` reopens the store with no
arguments — pass one of these instead of a raw `icechunk.Storage`.

### Register an Icechunk store

```python
from basal import storage as bstorage

dataset_storage = bstorage.s3_storage(
    bucket="my-data-bucket",
    prefix="my-dataset.icechunk",
    region="us-west-2",
    anonymous=True,
)

catalog.register(
    "my-dataset",
    storage=dataset_storage,
    owner="my-org",
    title="My Dataset",
    license="CC-BY-4.0",
    start_datetime="2015-01-15",
    bbox=[-180.0, -90.0, 180.0, 90.0],
)
```

`register()` auto-extracts CF global attrs, per-variable semantic attrs, and records `dataset_snapshot_id`. Pass `derive_extent=True` to also read coordinate arrays and fill `bbox`, `start_datetime`, and `end_datetime`:

```python
catalog.register("my-dataset", storage=dataset_storage, owner="my-org", derive_extent=True)
```

Pass `inspect=False` to skip store IO and supply metadata entirely as kwargs.

### Register a plain Zarr store

```python
catalog.register_zarr(
    "arco-era5-full",
    location="gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3",
    store_config={"skip_signature": True},  # anonymous GCS — obstore config
    owner="google-research",
    title="ARCO-ERA5 Full (37-level hourly)",
    bbox=[-180.0, -90.0, 180.0, 90.0],
    start_datetime="1940-01-01",
    license="CC-BY-4.0",
)
```

### Register a single DataTree node

Pass `group=` to target one node in a multi-group store. The group path is recorded
in the entry so `to_xarray()` opens exactly that node — no extra args needed at read
time:

```python
catalog.register(
    "cmip6/ACCESS-CM2/ssp245",
    storage=dataset_storage,
    group="ACCESS-CM2/ssp245",
    owner="csiro",
    title="ACCESS-CM2 SSP2-4.5",
)
```

`register_zarr()` accepts `group=` the same way.

### Register a DataTree store (fan-out)

`register_datatree()` walks every group in a DataTree-style store and registers each
leaf as its own entry in one call. All entries share the same underlying store; each
gets its own `group` path, derived CF attrs, and `dataset_snapshot_id`:

```python
names = catalog.register_datatree(
    "cmip6",
    storage=dataset_storage,
    owner="cmip6-project",
    license="CC-BY-4.0",
    derive_extent=True,
)
# → ["cmip6/ACCESS-CM2/historical", "cmip6/ACCESS-CM2/ssp245", ...]
```

`leaves_only=True` (default) skips empty parent nodes that hold no data variables.
`update=True` (default) makes the call idempotent — re-running updates existing
entries rather than raising.

Use `name_fn` or `metadata_fn` to customise names and per-node metadata:

```python
def my_meta(group_path: str, info: dict) -> dict:
    # info["global_attrs"] has the node's zarr attrs
    parts = group_path.split("/")
    return {"model": parts[0], "scenario": parts[1] if len(parts) > 1 else None}

catalog.register_datatree(
    "cmip6",
    storage=dataset_storage,
    metadata_fn=my_meta,
    owner="cmip6-project",
)
```

### Register a virtual Icechunk store (VirtualiZarr)

Pass `config=` with virtual chunk container settings. basal serializes them so `to_xarray()` can reconstruct credentials automatically:

```python
repo_config = icechunk.RepositoryConfig.default()
repo_config.set_virtual_chunk_container(
    icechunk.VirtualChunkContainer(
        "s3://carbonplan-share/",
        store=icechunk.ObjectStoreConfig.S3(
            icechunk.S3Options(region="us-west-2", anonymous=True)
        ),
    )
)

catalog.register(
    "my-virtual-dataset",
    storage=dataset_storage,
    config=repo_config,
    owner="carbonplan",
    title="My Virtual Dataset",
)
```

Container prefixes with `s3://`, `gs://`, `http://`, and `https://` schemes all
serialize and reconstruct automatically.

For non-anonymous credentials at read time:

```python
credentials = icechunk.containers_credentials(
    {"s3://my-private-bucket/": icechunk.s3_from_env_credentials()}
)
ds = entry.to_xarray(authorize_virtual_chunk_access=credentials)
```

!!! note
    `gs://` containers reconstruct with from-env credentials by default — the
    anonymous flag is not captured in the serialized config. For anonymous GCS
    reads, pass credentials explicitly:
    `authorize_virtual_chunk_access=icechunk.containers_credentials({"gs://bucket/": icechunk.gcs_credentials(anonymous=True)})`

### Register or update in bulk

`register_or_update()` registers on the first call and updates after, returning
`"registered"` or `"updated"` — use it when the dataset may already exist:

```python
action = catalog.register_or_update("my-dataset", storage=dataset_storage, owner="my-org")
```

Each `register()` is one commit, so building a catalog is a batch/cron job, not an
interactive call (see [Scaling](manage.md#scaling)). Loop with `register_or_update()`
(idempotent, safe to re-run), then `expire()` if the run updated anything:

```python
for name, storage, meta in sources:
    catalog.register_or_update(name, storage=storage, **meta)
catalog.expire(datetime.datetime.now(datetime.UTC))
```
