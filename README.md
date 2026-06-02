# basal: an icechunk native catalog

A small, serverless data catalog built on [Icechunk](https://icechunk.io).

*basal* as in the bottom layer of an ice sheet, not the herb.

> **Warning: super experimental — may change at any time.**

## Install

```
uv add basal
```

Optional extras:

```
uv add "basal[search]"   # DuckDB SQL + similarity search
uv add "basal[stac]"   # STAC API server
```

## Docs

[carbonplan.github.io/basal](https://carbonplan.github.io/basal/)

## Public catalog

15 public datasets at `s3://carbonplan-share/basal/public_icechunk_stores` — no credentials required.

```python
import icechunk
from basal import Catalog

storage = icechunk.s3_storage(
    bucket="carbonplan-share",
    prefix="basal/public_icechunk_stores",
    region="us-west-2",
)
catalog = Catalog.open(storage)
ds = catalog.get("noaa-gfs-analysis").to_xarray()
```
