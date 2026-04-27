# basal: an icechunk native catalog

A small, serverless dataset catalog built on [Icechunk 2](https://icechunk.io) with no external databases. 


`*basal` as in, the bottom layer of an icesheet, not the herb.
** Warning, super experimental** may change at an time. 


## Concept

Earth science catalogs seem to fall into two categories: a managed centralized database or local some collection of local files (STAC json or intake.yaml/json). The idea here is to get a bit of the shared catalog and tracking from a managed database, but without the overhead of running that. Icechunk provides really nice git-like transaction history and gives you things like optimistic concurrency in cloud storage. 

You start by creating a *_catalog_* Icechunk storage that acts as a centralized dataset catalog. Each dataset is registered as a branch whose HEAD snapshot carries that entry's metadata. `inspect_repo_info()` is a single read that returns all entries in your catalog — no external database, no coordinator.

```
s3://carbonplan-share/basal/public_icechunk_stores/  ← (the catalog)
s3://dynamical-noaa-gfs/noaa-gfs-analysis/v0.1.0.icechunk/ ←  Icechunk dataset entry
s3://dynamical-noaa-gfs/noaa-gfs-forecast/v0.2.7.icechunk/ ← Icechunk dataset entry
...                                                         ← more entries
```


## Public catalog entries

14 public Icechunk datasets at `s3://carbonplan-share/basal/public_icechunk_stores`:

| Entry | Owner | Notes |
|---|---|---|
| `carbonplan-nohrsc-snowfall` | carbonplan | Virtual Zarr store |
| `carbonplan-ocr-fire-risk` | carbonplan | |
| `dwd-icon-eu` | dynamical.org | |
| `ecmwf-aifs-single` | dynamical.org | |
| `ecmwf-ifs-ens` | dynamical.org | |
| `era5-weatherbench2` | google-research | |
| `gefs-forecast-35d` | dynamical.org | |
| `glad-land-cover` | glad | |
| `noaa-gefs-analysis` | dynamical.org | |
| `noaa-gfs-analysis` | dynamical.org | |
| `noaa-gfs-forecast` | dynamical.org | |
| `noaa-hrrr-analysis` | dynamical.org | |
| `noaa-hrrr-forecast` | dynamical.org | |
| `noaa-mrms-hourly` | dynamical.org | |

Locations are stored in catalog metadata — use `catalog.get(name).location` to retrieve.

## Usage

All examples use the public catalog at `s3://carbonplan-share/basal/public_icechunk_stores`,
with 14 public Icechunk datasets from [dynamical.org](https://dynamical.org) and others.
No credentials required — the catalog is publicly readable.

### Open an existing catalog

```python
import icechunk
from basal import IcechunkCatalog

storage = icechunk.s3_storage(
    bucket="carbonplan-share",
    prefix="basal/public_icechunk_stores",
    region="us-west-2",
)
catalog = IcechunkCatalog.open(storage)
```

### Browse entries

```python
# Jupyter: renders as HTML table. REPL: terse repr.
catalog
# <IcechunkCatalog with 14 entries>

# Rich-formatted terminal table
catalog.print()
# ┌──────────────────────────────┬──────────────────┬─────────────────────────────────┬─────────────────┐
# │ name                         │ owner            │ title                           │ location        │
# ├──────────────────────────────┼──────────────────┼─────────────────────────────────┼─────────────────┤
# │ carbonplan-nohrsc-snowfall   │ carbonplan       │ NOHRSC National Snowfall …      │ s3://carbonpla… │
# │ carbonplan-ocr-fire-risk     │ carbonplan       │ Open Climate Risk: Wildfire …   │ s3://us-west-2… │
# │ dwd-icon-eu                  │ dynamical.org    │ DWD ICON-EU 5-Day Forecast      │ s3://dynamica…  │
# │ ecmwf-aifs-single            │ dynamical.org    │ ECMWF AIFS Single Forecast      │ s3://dynamica…  │
# │ ecmwf-ifs-ens                │ dynamical.org    │ ECMWF IFS Ensemble 15-Day …     │ s3://dynamica…  │
# │ era5-weatherbench2           │ google-research  │ WeatherBench2 ERA5 (subset)     │ s3://icechunk…  │
# │ gefs-forecast-35d            │ dynamical.org    │ NOAA GEFS 35-Day Extended …     │ s3://dynamica…  │
# │ glad-land-cover              │ glad             │ GLAD Land Cover and Land Use    │ s3://icechunk…  │
# │ noaa-gefs-analysis           │ dynamical.org    │ NOAA GEFS Analysis              │ s3://dynamica…  │
# │ noaa-gfs-analysis            │ dynamical.org    │ NOAA GFS Analysis               │ s3://dynamica…  │
# │ noaa-gfs-forecast            │ dynamical.org    │ NOAA GFS 16-Day Forecast        │ s3://dynamica…  │
# │ noaa-hrrr-analysis           │ dynamical.org    │ NOAA HRRR Analysis              │ s3://dynamica…  │
# │ noaa-hrrr-forecast           │ dynamical.org    │ NOAA HRRR 48-Hour Forecast      │ s3://dynamica…  │
# │ noaa-mrms-hourly             │ dynamical.org    │ NOAA MRMS CONUS Hourly …        │ s3://dynamica…  │
# └──────────────────────────────┴──────────────────┴─────────────────────────────────┴─────────────────┘

# All entries as list
entries = catalog.list()
entries

# Single entry
entry = catalog.get("noaa-gfs-analysis")
# Entry(name='noaa-gfs-analysis', owner='dynamical.org', location='s3://dynamical-noaa-gfs/...')

# Full metadata panel
catalog.describe("ecmwf-aifs-single")
# ╭─────────────────────── ecmwf-aifs-single ────────────────────────╮
# │ title             ECMWF AIFS Single Forecast                     │
# │ owner             dynamical.org                                  │
# │ format            icechunk                                       │
# │ location          s3://dynamical-ecmwf-aifs-single/…            │
# │ license           CC-BY-4.0                                      │
# │ domain            Global                                         │
# │ spatial_resolution 0.25 degrees (~20km)                         │
# │ temporal_coverage 2024-06-01 to present                         │
# │ update_frequency  Twice daily (00Z, 12Z), 10-day horizon         │
# │ variables         ['t2m', 'u10', 'v10', 'tp', 'msl', …]        │
# │ tags              ['global', 'ai', 'ecmwf', 'machine-learning'…] │
# │ snapshot_id       STSEGF7J69C1J30FRBJG                          │
# ╰──────────────────────────────────────────────────────────────────╯
```

### Open a dataset

```python
entry = catalog.get("noaa-gfs-analysis")
ds = entry.to_xarray()
# <xarray.Dataset>
# Dimensions: (time: ..., latitude: 721, longitude: 1440)
# Data variables:
#     temperature_2m  (time, latitude, longitude) float32 ...

# Pin to a specific branch, tag, or snapshot
ds = entry.to_xarray(branch="main")
ds = entry.to_xarray(tag="v1.0")
ds = entry.to_xarray(snapshot_id="...")

# Extra kwargs forwarded to xarray.open_zarr
ds = entry.to_xarray(open_kwargs={"chunks": {}})

# Low-level: icechunk.Repository — for tags, branches, ancestry, writing
repo = entry.open_repo()

# Low-level: icechunk.Session
session = entry.open_session()
ds = xr.open_zarr(session.store, group="0", consolidated=False)
```

`entry.to_xarray()` works with no arguments when the entry was registered with `storage_config=`
(stored in catalog metadata). If `storage_config` is absent, pass  Icechunk `storage=` explicitly.

For virtual-chunk stores, see [Virtual datasets (VirtualiZarr)](#virtual-datasets-virtualizarr) below.

### Create your own catalog

`open_or_create()` initializes a new catalog if none exists at the storage location, or opens the existing one. The typical workflow is create → build dataset Storage → register:

```python
from basal import IcechunkCatalog
import icechunk

# 1. Create (or open) the Icechunk catalog repo
catalog_storage = icechunk.s3_storage(
    bucket="my-bucket", prefix="my-catalog", region="us-west-2", from_env=True
)
catalog = IcechunkCatalog.open_or_create(catalog_storage)

# 2. Build storage for a dataset — use Icechunk directly so we can pass on all that hard work to Icechunk.
dataset_storage = icechunk.s3_storage(
    bucket="my-data-bucket", prefix="my-dataset.icechunk",
    region="us-west-2",
    anonymous=True,   # public data — or from_env=True for credentialed stores. Check the icechunk docs for more examples.
)

# 3. Register — location and storage_config are derived from Icechunk Storage.
catalog.register(
    "my-dataset",
    storage=dataset_storage,
    owner="my-org",
    title="My Dataset",
    license="CC-BY-4.0",
)
```

`register()` accepts an `icechunk.Storage` object directly — no URL parsing. It opens the repo via `icechunk.Repository.open()` so Icechunk handles all validation: auth errors, missing stores, version mismatches. `location` and `storage_config` are auto-derived from the storage object and stored in catalog metadata so consumers can call `entry.to_xarray()` with no storage arguments!


### Register and deregister

```python
catalog.register(
    "my-dataset",
    storage=storage,
    owner="my-org",
    title="My Dataset",
    license="CC-BY-4.0",
    variables=["temperature"],
)

catalog.deregister("my-dataset")
```

`register()` tries to auto extract CF global attrs (`title`, `institution`, `conventions`, `source`),
per-variable `units`/`long_name`/`standard_name`, and records `dataset_snapshot_id` (the snapshot
at registration time). Explicit kwargs are proritized higher than anything derived from the store.

### Virtual datasets (VirtualiZarr)

Icechunk stores created with [VirtualiZarr](https://virtualizarr.readthedocs.io) reference external
chunks in object storage rather than storing data directly. basal handles these with a bit of minimal extra
configuration.

**Registration** — pass `config` with a `VirtualChunkContainer` so basal can open the repo and
detect virtual chunk containers. `virtual_chunk_containers_config` is then auto-built (region
inferred from auto-derived `storage_config`, anonymous read assumed):

```python
import icechunk
from basal.storage import repo_config_from_virtual_chunks

dataset_storage = icechunk.s3_storage(
    bucket="carbonplan-share",
    prefix="basal/examples/virtual_icechunk",
    region="us-west-2",
    from_env=True,
)

# RepositoryConfig needed at registration so basal can open the repo and detect VC containers
repo_config = repo_config_from_virtual_chunks(
    [{"url_prefix": "s3://carbonplan-share/", "region": "us-west-2", "anonymous": True}]
)

catalog.register(
    "my-virtual-zarr-datacube",
    storage=dataset_storage,
    config=repo_config,
    # virtual_chunk_containers_config auto-built from detected prefixes + storage region. May be fragile?
    owner="carbonplan",
    title="virtual zarr store of... ",
)
```

**Reading** — `entry.to_xarray()` can work with no extra arguments. Virtual chunk container config
and anonymous credentials are reconstructed automatically from stored metadata:

```python
entry = catalog.get("my-virtual-dataset")
ds = entry.to_xarray()
```

Pass `authorize_virtual_chunk_access` explicitly when chunks require non-anonymous credentials
(e.g. credentialed S3 access):

```python
import icechunk

credentials = icechunk.containers_credentials(
    {"s3://my-private-bucket/": icechunk.s3_from_env_credentials()}
)
ds = entry.to_xarray(authorize_virtual_chunk_access=credentials)
```

> **Detection note:** Icechunk does not persist `RepositoryConfig` inside the store. Virtual chunk
> containers are only detected at registration time if `config=` is passed with the containers
> configured. Without it, `virtual_chunk_containers_config` is not stored and callers must pass
> `config=` and `authorize_virtual_chunk_access=` explicitly at read time.

### Update metadata

Patch individual fields without re-registering:

```python
catalog.update("noaa-gfs-analysis", doi="10.5281/zenodo.12345")
# Merges into existing metadata. Required fields (location, owner, format) cannot be dropped.
```

Refresh CF attrs and staleness anchor from the live store:

```python
catalog.update_from_store("noaa-gfs-analysis")
# Re-inspects the store, updates derived attrs + dataset_snapshot_id.
# Pass kwargs to override specific fields on top of the fresh derived attrs.
catalog.update_from_store("noaa-gfs-analysis", title="My dataset, version 2")

# Also refresh bbox and temporal bounds from coordinate arrays:
# Warning, coordinate arrays for Zarr can grow large, this could cause issues!
catalog.update_from_store("noaa-gfs-analysis", derive_extent=True)
```

### Data freshness

```python
entry = catalog.get("noaa-gfs-analysis")

# True if the dataset store has new commits since registration/last update
entry.is_stale()
# False

# After data is appended to the dataset store:
entry.is_stale()
# True

# Resync:
catalog.update_from_store("noaa-gfs-analysis")
catalog.get("noaa-gfs-analysis").is_stale()
# False
```

`is_stale()` requires `dataset_snapshot_id` to be recorded (set by `register()` and `update_from_store()`).
Raises `ValueError` with a clear message if the field is missing.

Bulk staleness check and resync across all entries:

```python
# {name: bool} for all entries with dataset_snapshot_id + storage_config
# Entries missing either field are skipped with a warning
stale = catalog.refresh()
# {'noaa-gfs-analysis': False, 'noaa-hrrr-analysis': True, ...}

# Resync all entries from their live stores
catalog.update_all_from_store()
# Entries without stored storage_config are skipped with a warning
```

Read the timestamp of the current HEAD snapshot (no chunk IO):

```python
entry = catalog.get("noaa-hrrr-analysis")
entry.last_data_updated()
# datetime(2026, 4, 24, 16, 14, tzinfo=timezone.utc)
```

Unlike `is_stale()`, `last_data_updated()` works for any entry — it opens the dataset repo and reads the branch HEAD directly.

### Inspect store metadata

Read live zarr metadata from the dataset store (no chunk IO):

```python
entry = catalog.get("noaa-gfs-analysis")
info = entry.inspect()
# {
#   'dataset_snapshot_id': '...',
#   'global_attrs': {'title': 'NOAA GFS Analysis', ...},
#   'dims': {'time': ..., 'latitude': 721, 'longitude': 1440},
#   'variables': {'temperature_2m': {'dtype': 'float32', ...}},
#   'coords': {'time': {...}, 'latitude': {...}, 'longitude': {...}},
# }
```

### History

Catalog operations log, newest first:

```python
# All ops across all entries
catalog.history()
# [{'event': 'registered', 'name': 'noaa-gfs-analysis',  'timestamp': datetime(...), 'snapshot_id': '...'},
#  {'event': 'registered', 'name': 'noaa-gfs-forecast',  'timestamp': datetime(...), 'snapshot_id': '...'},
#  ...]

# Filter to one entry
catalog.history(name="noaa-gfs-analysis")

# Limit results
catalog.history(limit=20)
```

Events: `registered`, `updated`, `deregistered`. `snapshot_id` is present on `registered`/`updated` events — use it to pin `entry.to_xarray(snapshot_id=...)` to that exact metadata state.


### Filter by time and space

```python
# Temporal filter — entries whose coverage overlaps [2018, 2023]
# Fields: start_datetime / end_datetime (ISO 8601, STAC convention)
entries = catalog.filter(time_start="2018", time_end="2023")

# Open-ended bounds — use "*" for no lower or upper limit
entries = catalog.filter(time_start="*", time_end="2020")   # anything up to 2020
entries = catalog.filter(time_start="2025", time_end="*")   # 2025 onward
entries = catalog.filter(time_start="*", time_end="*")      # all (warns on missing fields)

# Spatial filter — entries intersecting a bounding box [west, south, east, north]
# Field: bbox (WGS84 decimal degrees, STAC convention)
entries = catalog.filter(bbox=(-10.0, 30.0, 40.0, 70.0))

# Combined
entries = catalog.filter(time_start="2018", time_end="2023", bbox=(-10.0, 30.0, 40.0, 70.0))
```

Entries missing the queried field are excluded and a `UserWarning` is issued listing their names with a hint to add the field:

```python
# UserWarning: 3 entries skipped — no start_datetime/end_datetime: ['glad-land-cover', ...].
# Add with: catalog.update(name, start_datetime='2020-01-01', end_datetime='2023-12-31')
```

Register entries with STAC-compatible temporal and spatial metadata. For large datasets pass values explicitly — avoids reading coordinate arrays:

```python
catalog.register(
    "noaa-gfs-analysis",
    storage=storage,
    start_datetime="2015-01-15",          # ISO 8601, no end = open-ended (ongoing)
    bbox=[-180.0, -90.0, 180.0, 90.0],   # [west, south, east, north] WGS84
)

# For smaller datasets: derive bbox and temporal bounds automatically from
# coordinate arrays (lat/lon/time). Explicit kwargs still win if both are given.
catalog.register("my-dataset", storage=storage, derive_extent=True)
```

Infer and write bounds for an existing entry:

```python
entry = catalog.get("noaa-gfs-analysis")

# Opens the dataset, reads coord arrays, updates catalog (update=True by default)
extent = entry.infer_extent(catalog)
# {'bbox': [-180.0, -90.0, 180.0, 90.0], 'start_datetime': '...', 'end_datetime': '...'}

# Inspect only — no catalog write
extent = entry.infer_extent(catalog, update=False)
```

### Search & Discovery

Three layers of search — each opt-in, each building on the last:

- **Python filter** (`catalog.filter()`) — structured time/space queries, no extras needed
- **DuckDB SQL** — flexible predicates over any metadata field
- **Semantic similarity** — ONNX embeddings + DuckDB `array_cosine_similarity`, no external vector DB

SQL search and similarity search both require `basal[search]`:

```
uv add "basal[search]"
```

#### Field discovery

```python
# All metadata keys across all entries
catalog.fields()
# {'location', 'owner', 'format', 'title', 'license', 'variables', 'tags', 'domain',
#  'spatial_resolution', 'temporal_coverage', 'update_frequency', 'storage_config'}

# Distinct values for a field (list-valued fields are flattened)
catalog.values("owner")
# ['carbonplan', 'dynamical.org', 'glad', 'google-research']

catalog.values("tags")
# ['analysis', 'atmosphere', 'ensemble', 'forecast', 'global', 'machine-learning', 'reanalysis', ...]

# Frequency count per field/value
catalog.facets()
# {
#   'owner': Counter({'dynamical.org': 10, 'carbonplan': 1, 'glad': 1, 'google-research': 1}),
#   'tags': Counter({'forecast': 6, 'analysis': 5, 'global': 4, 'ensemble': 2, ...}),
#   'license': Counter({'CC-BY-4.0': 13}),
# }
```

#### SQL search (DuckDB)

```python
from basal.search import sql, sql_df, sql_arrow

# Via catalog directly
catalog.search("SELECT name FROM entries WHERE metadata->>'owner' = 'dynamical.org'")
# [('dwd-icon-eu',), ('ecmwf-aifs-single',), ('ecmwf-ifs-ens',), ...]

# Or use the standalone functions for more return-type control:

# Raw tuples
sql(catalog, "SELECT name FROM entries WHERE metadata->>'license' = 'CC-BY-4.0' ORDER BY name")
# [('carbonplan-ocr-fire-risk',), ('dwd-icon-eu',), ('ecmwf-aifs-single',), ...]

# Pandas DataFrame
sql_df(catalog, "SELECT name, metadata->>'owner' AS owner FROM entries ORDER BY name")
#                         name             owner
# 0   carbonplan-ocr-fire-risk        carbonplan
# 1                dwd-icon-eu     dynamical.org
# 2          ecmwf-aifs-single     dynamical.org
# ...

# Filter on list-valued field (tags)
sql(
    catalog,
    "SELECT name FROM entries "
    "WHERE list_contains(CAST(metadata->'tags' AS VARCHAR[]), 'ensemble')",
)
# [('ecmwf-ifs-ens',), ('gefs-forecast-35d',), ('noaa-gefs-analysis',)]

# arrow3 Table (zero-copy from DuckDB, no pyarrow required)
tbl = sql_arrow(catalog, "SELECT * FROM entries")
```

DuckDB table schema: `(name VARCHAR, snapshot_id VARCHAR, metadata JSON)`.
Use `metadata->>'field'` for scalar extraction, `CAST(metadata->'field' AS VARCHAR[])` for array fields.

> **Alternative backend ideas:** [Apache DataFusion](https://datafusion.apache.org/) 
> [zarr-datafusion-search-examples](https://github.com/developmentseed/zarr-datafusion-search-examples).

#### Similarity search

When you know what you're looking for but can't express it as a structured query, similarity search lets you describe it in plain text — or use a dataset you already know as the starting point.

```python
from basal.search import similar

# Default: fastembed TextEmbedding runs locally, no API key
results = similar(catalog, "high resolution precipitation radar CONUS", top_k=3)
# [(Entry(name='noaa-mrms-hourly', ...),   0.77),
#  (Entry(name='noaa-hrrr-analysis', ...), 0.73),
#  (Entry(name='noaa-hrrr-forecast', ...), 0.70)]

# Or pass any embed_fn: list[str] -> list[list[float]]
from fastembed import TextEmbedding
model = TextEmbedding("BAAI/bge-small-en-v1.5")
results = similar(
    catalog,
    "sea surface temperature",
    embed_fn=lambda texts: list(model.embed(texts)),
    top_k=3,
)

# Shortcut via catalog
results = catalog.search_similar("ocean reanalysis", top_k=3)
```

`similar()` uses DuckDB `array_cosine_similarity` — no external vector DB. All metadata fields (including arbitrary kwargs) are automatically included in the embedding text.

If you already know a useful dataset, find what else in the catalog resembles it — same variables, similar domain, related coverage:

```python
# Via catalog
results = catalog.similar_to("ecmwf-aifs-single", n=4)
for entry, score in results:
    print(f"{entry.name}: {score:.2f}")
# ecmwf-ifs-ens: 0.87
# noaa-gfs-forecast: 0.80
# era5-weatherbench2: 0.79
# noaa-gefs-analysis: 0.78

# Via entry (shorthand)
entry = catalog.get("noaa-hrrr-forecast")
for neighbor, score in entry.similar(catalog, n=3):
    print(f"{neighbor.name}: {score:.2f}")
# noaa-hrrr-analysis: 0.92
# noaa-mrms-hourly: 0.83
# noaa-gfs-analysis: 0.79
```


## Metadata schema

Two fields are required:

```python
{
    "location": "s3://bucket/path/to/icechunk-store/",  # auto-derived from storage
    "format": "icechunk",                                # default - Note: currently only icechunk is supported
}
```

`name` is a positional argument to `register()`. `owner` is strongly recommended but optional. Both `location` and `format` are always present — `location` is auto-derived from the `storage` object, and `format` defaults to `"icechunk"`.

Everything else is optional and unconstrained — pass any additional kwargs to `register()`. The protocol doesn't own your schema; domain-specific fields live in the free-form blob. See [Register and deregister](#register-and-deregister) for a full example.

## Design principles

- **Enforce nothing beyond what's needed to function.** Only `location` and `format` are required; both auto-derived. 
- **Domain-specific metadata lives in the free-form blob.** The protocol doesn't own your schema.
- **Search is a layer above, not baked in.** Core is a Python filter; DuckDB adds SQL; similarity search adds vectors — each opt-in!
- **Storage reads are explicit and bounded.** `register()` reads the dataset store once at registration time to extract CF attrs and snapshot anchor. `update_from_store()` and `entry.inspect()` are explicit re-inspection opt-ins; all other catalog operations are metadata-only.
- **`derived_from` enables lineage without requiring it.** Pass a list of upstream entry IDs to track provenance.
- **No server, no database.** The catalog is just an Icechunk repo in object storage. Gives you a non-local catalog that doesn't require running a bunch of infrastructure.

## Open questions

- How will this scale? 
    - No idea, this would be great to figure out the limits. Maybe this is where you could federate catalogs. 
