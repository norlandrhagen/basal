# TODO

## High priority

### HTTP/HTTPS virtual chunk container support
`_object_store_config_from_virtual_chunk_dict` currently raises `NotImplementedError` for
`http://` and `https://` URL prefixes. Need to wire up `icechunk.ObjectStoreConfig.Http()`
(or equivalent) so virtual chunk stores referencing HTTP sources can round-trip through
`repo_config_from_virtual_chunks()` and `virtual_chunk_credentials_from_config()`.

Also: add `http` and `redirect` storage support in `storage_from_config()` — the
`{"type": "http", "base_url": "..."}` and `{"type": "redirect", "base_url": "..."}` paths
exist but are untested and uncovered.

Related: document the HTTP catalog pattern — open a catalog via
`icechunk.http_storage(base_url="https://...")` for CDN-served read-only access (no AWS
credentials). README's "Why Icechunk 2" table mentions it but no usage example exists.

### STAC export
`catalog.to_stac()` — emit a static STAC catalog (root `catalog.json` + per-entry
`collection.json`). Reference: https://stac.dynamical.org/catalog.json and
https://stac.dynamical.org/noaa-gfs-analysis/collection.json.

Needs conventions for mapping basal metadata fields → STAC fields:
- `location` → `links[].href`
- `bbox` → `extent.spatial.bbox`
- `temporal_coverage_start` / `temporal_coverage_end` → `extent.temporal.interval`
- `license` → `license`
- `title`, `description` → `title`, `description`

Also consider `register_from_stac(stac_url)` — parse a STAC Item/Collection and
auto-populate `location`, `title`, `bbox`, `temporal_coverage`, etc.

## Medium priority

### GCS virtual chunk container support
`_object_store_config_from_virtual_chunk_dict` raises `NotImplementedError` for `gs://`
prefixes. Need to wire up `icechunk.ObjectStoreConfig.Gcs(GcsOptions(...))`.
`virtual_chunk_credentials_from_config` already handles `gs://` for credentials — the gap
is the write-time `RepositoryConfig` reconstruction.

### Temporal range queries
`catalog.filter(time_start="2020", time_end="2023")` — filter entries by time coverage.
Requires a field name convention (`temporal_coverage_start` / `temporal_coverage_end` as
ISO strings). Could be implemented as a DuckDB SQL helper on top of the existing `sql()`
layer, or as a Python filter on `catalog.list()`.

### Spatial bbox filter
`catalog.filter(bbox=(-180, -90, 180, 90))` — standard in STAC. Requires `bbox` field
convention `[west, south, east, north]`. Intersection test can run in DuckDB or Python.

### `catalog.refresh()` / bulk staleness
Re-run `is_stale()` across all entries, return `{name: bool}`. Also
`catalog.update_all_from_store()` — refresh `dataset_snapshot_id` for all entries. Useful
for catalog maintenance cron.

## Lower priority

### Intake / Intake-ESM driver
`catalog.to_intake()` → intake catalog object. `catalog.to_esm_datastore()` → ESM-style
collection JSON. Common in Pangeo workflows.

### Export to JSON / Parquet
`catalog.to_json()` / `catalog.to_parquet()` — flat dump of metadata for non-Python
consumers and dashboard tooling.

### Metadata validation schemas
Opt-in JSON Schema (CF, STAC, or custom) at `register()` time via a `schema=` param.
Current validation enforces only `location`, `owner`, `format`.

### `entry.to_xarray(as_of=datetime)`
Open dataset at snapshot closest to a given timestamp. Requires icechunk ancestry walk to
find the nearest snapshot by `written_at`.

### Catalog merge / union
`IcechunkCatalog.merge(cat_a, cat_b)` — combine two catalogs. Likely copies entries
branch-by-branch (icechunk has no native branch merge across repos).
