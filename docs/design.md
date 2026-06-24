# Design

How basal maps a catalog onto a single Icechunk repository, and what can go
wrong.

## Data model

The catalog is one Icechunk repo. Each registered dataset is a branch; the
entry's metadata lives in the commit metadata of that branch's HEAD snapshot.
No chunk data is ever written to the catalog repo — every commit is empty
(`allow_empty=True`), only the metadata payload matters.

```
catalog repo (s3://bucket/prefix)
│
├── branch "main"                  ← reserved; init commit only, never an entry
│
├── branch "noaa-gfs-analysis"     ← one branch per entry
│     └── HEAD snapshot
│           └── commit metadata    ← the entry
│                 ├── location: "s3://dynamical-noaa-gfs/...icechunk"
│                 ├── format: "icechunk"
│                 ├── storage_config: {...}     ← rebuilds icechunk.Storage
│                 ├── dataset_snapshot_id       ← staleness anchor
│                 ├── title, owner, bbox, ...   ← free-form metadata
│                 └── __event__: "registered"   ← internal, stripped on read
│
└── branch "era5-sfc"
      └── HEAD snapshot ── commit metadata ── {...}
```

Earlier commits on an entry branch are its history: every `update()` /
`update_from_store()` adds a snapshot, so `history()` is a free audit log.

### Why this layout

- **One read for the whole catalog.** `list()` calls
  `inspect_repo_info()`, which fetches all branches plus snapshot metadata in
  a single atomic call — no per-entry round trips.
- **Optimistic concurrency for free.** Branches have independent commit
  histories, so concurrent `register()` calls for *different* names never
  conflict. Icechunk's commit machinery handles write races.
- **Reversible deletes.** `deregister()` commits a `__deleted__` marker
  instead of deleting the branch; `restore()` clears it. `purge=True` is the
  only irreversible path.

## Security properties

- **Credentials never enter catalog metadata.** `StorageSpec.to_config()`
  serializes only scalar config keys (bucket, prefix, region, anonymous,
  from_env, endpoint_url) and drops credential objects and unknown kwargs.
  For private stores, record `from_env: true` — readers resolve credentials
  from their own environment at open time.
- A catalog opened with `Catalog.open(storage, readonly=True)` raises
  `PermissionError` on any mutating method — use it for shared/public
  catalog consumers.

## Failure modes

| Situation | Behavior |
|---|---|
| Two writers `register()` the same name concurrently | One wins; the other gets `ValueError` ("already registered" or "created concurrently"). |
| Two writers `open_or_create()` a new catalog concurrently | One creates; the other detects the existing repo and opens it. |
| `update()` race on the same entry | Last commit wins. History keeps both snapshots until `expire()`. |
| Entry's dataset store moved or deleted | Catalog entry is unaffected (it stores only metadata). `to_xarray()` / `is_stale()` fail at open time. |
| `get()` on unknown name | `KeyError`. |
| `get()` on deregistered name | `KeyError` with restore hint; pass `include_deleted=True` to read it. |

## Scaling notes

Entry metadata lives in snapshot commit metadata, and `list()` fetches every
entry's blob — so basal stores only `units`/`long_name`/`standard_name` per
variable to keep that blob small. Update history accumulates snapshots that
`list()` must traverse, which `expire()` collapses.

See the [Scaling section](manage.md#scaling) for the read/write cost model.
