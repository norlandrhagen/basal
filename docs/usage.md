# Usage

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

## List entries

```python
entries = catalog.list()   # list[Entry], excludes deregistered
entry = catalog.get("noaa-gfs-analysis")
```

For terminal inspection:

```python
catalog.print()              # rich-formatted table of all entries
catalog.describe("my-dataset")  # rich-formatted detail panel for one entry
```

## Register entries

Prefer `basal.storage.*` (StorageSpec) over a raw `icechunk.Storage` — it captures construction kwargs so `storage_config` is recorded and `entry.to_xarray()` works with no arguments.

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

`register()` auto-extracts CF global attrs, per-variable semantic attrs, and records `dataset_snapshot_id`. Pass `derive_extent=True` to also read coordinate arrays and auto-populate `bbox`, `start_datetime`, and `end_datetime`:

```python
catalog.register("my-dataset", storage=dataset_storage, owner="my-org", derive_extent=True)
```

Pass `inspect=False` to skip all store IO and supply metadata entirely as kwargs.

Use `register_or_update()` when the dataset may already exist — it registers on first call and updates on subsequent calls, returning `"registered"` or `"updated"`:

```python
action = catalog.register_or_update("my-dataset", storage=dataset_storage, owner="my-org")
```

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

For non-anonymous credentials at read time:

```python
credentials = icechunk.containers_credentials(
    {"s3://my-private-bucket/": icechunk.s3_from_env_credentials()}
)
ds = entry.to_xarray(authorize_virtual_chunk_access=credentials)
```

## Deregister and restore

```python
catalog.deregister("my-dataset")           # reversible: commits a deregistered marker
catalog.restore("my-dataset")              # undo
catalog.get("my-dataset", include_deleted=True)

catalog.deregister("my-dataset", purge=True)  # irreversible: drops the branch
```

Default deregister keeps the branch and commit history. Use `purge=True` only for legal/compliance erasure or throwaway test entries.

## Update metadata

```python
catalog.update("noaa-gfs-analysis", doi="10.5281/zenodo.12345")

# Re-inspect the live store, refresh CF attrs + snapshot anchor
catalog.update_from_store("noaa-gfs-analysis")
catalog.update_from_store("noaa-gfs-analysis", title="My dataset, v2")

# Auto-derive bbox + temporal bounds for an existing entry
entry = catalog.get("noaa-gfs-analysis")
entry.infer_extent(catalog, update=True)

# For time-appending datasets: refresh only end_datetime + snapshot_id
diff = catalog.update_from_store("noaa-hrrr-forecast", time_only=True)
# {'dataset_snapshot_id': ('abc123', 'xyz789'), 'end_datetime': ('2026-04-01', '2026-04-28')}

# Resync all entries
catalog.update_all_from_store()
```

## Inspect store metadata

```python
entry = catalog.get("noaa-gfs-analysis")
info = entry.inspect()
# {'dataset_snapshot_id': '...', 'global_attrs': {...}, 'dims': {...}, 'variables': {...}}
```

## Filter by time and space

```python exec="true" html="true" session="usage"
import warnings
import icechunk
from basal import Catalog

storage = icechunk.s3_storage(
    bucket="carbonplan-share",
    prefix="basal/public_icechunk_stores",
    region="us-west-2",
    anonymous=True,
)
catalog = Catalog.open(storage)
warnings.filterwarnings("ignore")

def _entries_html(entries, title=""):
    rows = "".join(
        f"<tr><td><b>{e.name}</b></td><td>{e.metadata.get('owner','')}</td>"
        f"<td>{e.metadata.get('title','')}</td></tr>"
        for e in sorted(entries, key=lambda e: e.name)
    )
    header = f"<tr><th colspan=3>{title} ({len(entries)} entries)</th></tr>" if title else ""
    return (
        f'<div style="overflow-x:auto"><table><thead>{header}'
        f"<tr><th>name</th><th>owner</th><th>title</th></tr>"
        f"</thead><tbody>{rows}</tbody></table></div>"
    )
```

Filter by temporal coverage:

```python exec="true" html="true" session="usage"
entries = catalog.filter(time_start="2021", time_end="*")   # open-ended from 2021
print(_entries_html(entries, "time_start='2021', time_end='*'"))
```

Filter by bounding box (Europe):

```python exec="true" html="true" session="usage"
entries = catalog.filter(bbox=(-10.0, 35.0, 40.0, 70.0))   # [W, S, E, N]
print(_entries_html(entries, "bbox=(-10, 35, 40, 70)"))
```

Combined:

```python
entries = catalog.filter(time_start="2018", time_end="2023", bbox=(-10.0, 30.0, 40.0, 70.0))
```

## Data freshness

```python
entry.is_stale()          # True if store has new commits since last register/update
entry.last_data_updated() # datetime of current HEAD snapshot

stale = catalog.refresh() # {name: bool} across all entries
```

## History

```python
catalog.history()                         # 10 most recent ops across all entries
catalog.history(name="noaa-gfs-analysis") # filter to one entry
catalog.history(limit=50)
# [{'event': 'registered', 'name': '...', 'timestamp': datetime(...), 'snapshot_id': '...'}, ...]
```

Events: `registered`, `updated`, `deregistered`.

## Search and discovery

Requires `basal[search]`.

### Field discovery

`fields()`, `values()`, and `facets()` work purely from cached catalog metadata — no store IO.

```python
catalog.fields()         # union of all metadata keys across entries
catalog.values("owner")  # distinct values for a field; list-valued fields flattened
catalog.facets()         # {field: Counter(value -> count)} for scalar + list fields
catalog.summary()        # field coverage table with recommended fields flagged
```

`facets()` is the closest basal equivalent to intake's driver/source listing — it tells you what values exist and how common they are across the catalog:

```python
facets = catalog.facets()
# {'owner': Counter({'NOAA': 5, 'ECMWF': 3}),
#  'license': Counter({'CC-BY-4.0': 6, 'public-domain': 2}),
#  'tags': Counter({'reanalysis': 4, 'forecast': 3, 'ensemble': 1}), ...}
```

High-cardinality and free-text fields (`title`, `description`, `doi`, timestamps, etc.) are excluded automatically so the output stays useful.

**vs. intake**: intake catalogs are tree-structured — you browse a hierarchy of named sources with fixed drivers. basal is flat with rich metadata: you discover via facets/SQL/similarity instead of navigating a tree. No driver concept; all entries open via `to_xarray()`.

### SQL search (DuckDB)

All catalog metadata is queryable as a DuckDB in-memory table — no server, no index to maintain.

```python
catalog.sql("SELECT name FROM entries WHERE metadata->>'owner' = 'dynamical.org'")
catalog.sql(
    "SELECT name FROM entries "
    "WHERE list_contains(CAST(metadata->'tags' AS VARCHAR[]), 'ensemble')"
)
```

Table schema: `(name VARCHAR, snapshot_id VARCHAR, metadata JSON)`.

`sql_df()` returns a pandas DataFrame instead of raw tuples:

```python
from basal.search import sql_df
df = sql_df(catalog, "SELECT name, metadata->>'owner' AS owner FROM entries ORDER BY name")
```

### Similarity search

```python
catalog.search("high resolution precipitation radar CONUS", top_k=3)
# [(Entry('noaa-mrms-hourly'), 0.77), (Entry('noaa-hrrr-analysis'), 0.73), ...]

catalog.similar_to("ecmwf-aifs-single", n=4)
```

All use DuckDB `array_cosine_similarity` — no external vector DB. Pass `use_schema=True` for richer embeddings from the full zarr schema.

## Scaling

`list()` reads every snapshot in the repo in a single call — cost grows with total commit history (entries × updates), not entry count alone. Bound it with `expire()`:

```python
import datetime
catalog.expire(datetime.datetime.now(datetime.UTC))  # keep only current HEADs
catalog.expire(older_than, garbage_collect=True)      # also reclaim object storage
```

Benchmark (local FS):

| N entries | updates/entry | `list()` | after `expire()` |
|---|---|---|---|
| 100 | 0  | 1.1 ms  | 1.1 ms |
| 100 | 10 | 9.2 ms  | 2.6 ms |
| 500 | 0  | 5.5 ms  | 5.5 ms |
| 500 | 10 | 40.2 ms | 5.6 ms |

## STAC export

!!! warning
    Experimental — may change at any time.

```python
stac = catalog.to_stac(collection_id="my-catalog")
stac["collection"]  # STAC Collection dict
stac["items"]       # list of STAC Item dicts
```

Entries without `bbox` are skipped with a warning. `geometry` (GeoJSON Polygon) is auto-derived from `bbox`.

### STAC API server

Requires `basal[stac]`.

```python
from basal.stac_api import create_app
app = create_app(catalog)
# uvicorn mymodule:app --host 0.0.0.0 --port 8000
```

CLI via env vars:

```bash
BASAL_BUCKET=carbonplan-share \
BASAL_PREFIX=basal/public_icechunk_stores \
BASAL_REGION=us-west-2 \
BASAL_ANONYMOUS=true \
uvicorn basal.stac_api:app --host 0.0.0.0 --port 8000
```

Endpoints: `GET /collections`, `GET /collections/{id}/items`, `GET /search`, `POST /search`. CORS enabled.
