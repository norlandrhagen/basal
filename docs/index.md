# basal

A small, serverless data catalog **prototype** built on [Icechunk](https://icechunk.io).

*basal* as in the bottom layer of an ice sheet, not the herb.

> **Warning: super experimental — may change at any time.**

## Concept

Earth science catalogs tend to be one of two things: a managed central database, or a local collection of files (STAC json, intake yaml, etc.). `basal` sits in the middle — shared catalog tracking without the overhead of running a database. Icechunk provides git-like transaction history and optimistic concurrency in cloud storage, which the catalog inherits by being itself an Icechunk store.

Each dataset is registered as a branch whose HEAD snapshot carries that entry's metadata. A single read returns all entries in your catalog.

```
s3://carbonplan-share/basal/public_icechunk_stores/  ← catalog
s3://dynamical-noaa-gfs/noaa-gfs-analysis/v0.1.0.icechunk/  ← dataset entry
s3://dynamical-noaa-gfs/noaa-gfs-forecast/v0.2.7.icechunk/  ← dataset entry
```

## Design principles

- **Minimal catalog field hard requirements.** Only `location` and `format` are required.
- **Domain-specific metadata lives in a free-form blob.** i.e., shove dataset metadata in or leave it very plain.
- **Search is a layer above, not baked in.** We can (optionally) add on DuckDB to search dataset similarity. 
- **No server, no database.** The catalog is an Icechunk repo in object storage.
- **One branch per entry.** Branches give independent commit histories and concurrent `register()` calls shouldn't conflict.

## Install

```
uv add "basal @ git+https://github.com/norlandrhagen/basal"
```

Optional extras (basal is not on PyPI, so extras need the git URL too):

```
uv add "basal[search] @ git+https://github.com/norlandrhagen/basal"   # DuckDB SQL + similarity search
uv add "basal[stac] @ git+https://github.com/norlandrhagen/basal"     # STAC API server
```

## Public catalog

Here is an example catalog built from public Icechunk and Zarr stores.

```python exec="true" html="true" session="index"
import icechunk
from basal import Catalog

storage = icechunk.s3_storage(
    bucket="carbonplan-share",
    prefix="basal/public_icechunk_stores",
    region="us-west-2",
    anonymous=True,
)
catalog = Catalog.open(storage)
print(f'<div style="overflow-x:auto">{catalog._repr_html_()}</div>')
```

## Quickstart

```python
entry = catalog.get("noaa-gfs-analysis")
```

```python exec="true" html="true" session="index"
entry = catalog.get("noaa-gfs-analysis")
print(f'<div style="overflow-x:auto">{entry._repr_html_()}</div>')
```

```python
ds = entry.to_xarray()
ds
```

```python exec="true" html="true" session="index"
ds = entry.to_xarray()
print(f'<div style="overflow-x:auto">{ds._repr_html_()}</div>')
```

See [Getting started](usage.md), [Discover](discover.md), and [Managing entries](manage.md) for the full API.
