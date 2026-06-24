# Reference

## Metadata schema

Two fields are required (both auto-derived):

| Field | Description |
|---|---|
| `location` | URI of the dataset store — auto-derived from `storage` |
| `format` | `"icechunk"` or `"zarr"` |

Everything else is optional and unconstrained — pass any additional kwargs to `register()`.

### Recommended fields

These map to [STAC Item spec](https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md) fields and enable filtering, STAC export, and discoverability:

| Field | Description | STAC equivalent |
|---|---|---|
| `title` | Human-readable dataset name | `properties.title` |
| `owner` | Producing organization | `properties.providers[].name` |
| `bbox` | `[west, south, east, north]` WGS84 | `bbox` |
| `start_datetime` | Coverage start, ISO 8601 | `properties.start_datetime` |
| `end_datetime` | Coverage end; omit if ongoing | `properties.end_datetime` |
| `license` | SPDX identifier e.g. `CC-BY-4.0` | `properties.license` |
| `tags` | List of keyword strings | `properties.keywords` |
| `doi` | Dataset DOI | `sci:doi` |

Auto-derived at registration — no need to set manually:

- `geometry` (GeoJSON Polygon) — from `bbox`
- `var_names`, `coord_names`, `dim_names` — from the store

### Icechunk vs Zarr entries

| Feature | `format="icechunk"` | `format="zarr"` |
|---|---|---|
| `to_xarray()` | pin to branch / tag / snapshot | always reads latest |
| `inspect()` | ✓ | ✓ |
| `infer_extent()` | ✓ | ✓ |
| `is_stale()` | ✓ snapshot comparison | ✗ |
| `last_data_updated()` | ✓ HEAD snapshot timestamp | ✗ |
| `open_repo()` / `open_session()` | ✓ | ✗ |

## Opening modes

`Catalog.open(storage, ...)` reaches the catalog repo; the `storage` argument
selects how, and with what credentials. Add `readonly=True` for any consumer of a
shared catalog (see [Getting started](usage.md#create-a-catalog)).

**Default** — credentialed object storage via `icechunk.s3_storage(...)` /
`icechunk.gcs_storage(...)`.

**Over HTTP (no cloud credentials).** A catalog served over plain HTTP — e.g. a
public S3 bucket or a CDN — can be opened read-only with no AWS credentials at all:

```python
catalog = Catalog.open(
    icechunk.http_storage(
        base_url="https://carbonplan-share.s3.us-west-2.amazonaws.com/basal/public_icechunk_stores"
    ),
    readonly=True,
)
```

`icechunk.redirect_storage(base_url=...)` works the same way for endpoints that
302-redirect to object storage.

