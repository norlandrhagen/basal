# basal

A small, serverless dataset catalog built on [Icechunk 2](https://icechunk.io) with no external database.

*basal* as in the bottom layer of an ice sheet, not the herb.

> **Warning: super experimental — may change at any time.**

## Concept

Earth science catalogs fall into two categories: a managed centralized database or a local collection of files (STAC json, intake yaml). basal aims for the middle — shared catalog tracking without the overhead of running a database. Icechunk provides git-like transaction history and optimistic concurrency in cloud storage.

Each dataset is registered as a branch whose HEAD snapshot carries that entry's metadata. A single read returns all entries in your catalog.

```
s3://carbonplan-share/basal/public_icechunk_stores/  ← catalog
s3://dynamical-noaa-gfs/noaa-gfs-analysis/v0.1.0.icechunk/  ← dataset entry
s3://dynamical-noaa-gfs/noaa-gfs-forecast/v0.2.7.icechunk/  ← dataset entry
```

## Design principles

- **Enforce nothing beyond what's needed to function.** Only `location` and `format` are required; both auto-derived.
- **Domain-specific metadata lives in a free-form blob.** The protocol doesn't own your schema.
- **Search is a layer above, not baked in.** Core is a Python filter; DuckDB adds SQL; similarity search adds vectors — each opt-in.
- **Storage reads are explicit and bounded.** `register()` reads the store once at registration. `update_from_store()` and `entry.inspect()` are explicit opt-ins; all other operations are metadata-only.
- **No server, no database.** The catalog is an Icechunk repo in object storage.
- **One branch per entry.** Branches give independent commit histories and concurrent `register()` calls never conflict — Icechunk transactions are branch-scoped. All branch HEADs are fetched in a single call.

## Install

```
uv add basal
```

Optional extras:

```
uv add "basal[search]"   # DuckDB SQL + similarity search
uv add "basal[stac]"   # STAC API server
```

## Public catalog

No credentials required.

```python exec="true" html="true" session="index"
import icechunk
from basal import Catalog

storage = icechunk.s3_storage(
    bucket="carbonplan-share",
    prefix="basal/public_icechunk_stores",
    region="us-west-2",
)
catalog = Catalog.open(storage)
print(f'<div style="overflow-x:auto">{catalog._repr_html_()}</div>')
```

## Quickstart

```python exec="true" html="true" session="index"
entry = catalog.get("noaa-gfs-analysis")
print(f'<div style="overflow-x:auto">{entry._repr_html_()}</div>')
```

```python
ds = entry.to_xarray()
```

See [Usage](usage.md) for the full API.
