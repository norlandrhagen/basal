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

