# Discover

## List entries

```python
entries = catalog.list()   # list[Entry], excludes deregistered
entry = catalog.get("noaa-gfs-analysis")
```

Pretty-print to a terminal:

```python
catalog.print()              # rich-formatted table of all entries
catalog.describe("my-dataset")  # rich-formatted detail panel for one entry
```

### Inspect store metadata

```python
entry = catalog.get("noaa-gfs-analysis")
info = entry.inspect()
# {'dataset_snapshot_id': '...', 'global_attrs': {...}, 'dims': {...}, 'variables': {...}}
```

## Open data

### Single dataset

```python
entry = catalog.get("noaa-gfs-analysis")
ds = entry.to_xarray()
```

For entries with a stored `group` (e.g. registered with `group=` or via
`register_datatree()`), `to_xarray()` opens exactly that node automatically —
no `group=` needed at call time.

### DataTree

`to_datatree()` opens the entry's subtree as an `xr.DataTree`. If the entry has a
stored `group`, that node becomes the root:

```python
entry = catalog.get("cmip6/ACCESS-CM2/ssp245")
dt = entry.to_datatree()
```

To open the entire store as a DataTree from a top-level entry:

```python
entry = catalog.get("cmip6")
dt = entry.to_datatree()
```

!!! note
    `to_datatree()` requires `engine="zarr"` under the hood — it is applied
    automatically. This differs from `to_xarray()` which auto-detects the engine.

## Filter by time and space

```python exec="true" html="true" session="discover"
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

```python
entries = catalog.filter(time_start="2021", time_end="*")   # open-ended from 2021
```

```python exec="true" html="true" session="discover"
entries = catalog.filter(time_start="2021", time_end="*")
print(_entries_html(entries, "time_start='2021', time_end='*'"))
```

Filter by bounding box (Europe):

```python
entries = catalog.filter(bbox=(-10.0, 35.0, 40.0, 70.0))   # [W, S, E, N]
```

```python exec="true" html="true" session="discover"
entries = catalog.filter(bbox=(-10.0, 35.0, 40.0, 70.0))
print(_entries_html(entries, "bbox=(-10, 35, 40, 70)"))
```

Combined:

```python
entries = catalog.filter(time_start="2018", time_end="2023", bbox=(-10.0, 30.0, 40.0, 70.0))
```

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
