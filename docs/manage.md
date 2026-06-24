# Managing entries

## Update metadata

```python
catalog.update("noaa-gfs-analysis", doi="10.5281/zenodo.12345")

# Delete metadata keys outright — merging alone can never remove one.
# Required fields (name, location, ...) cannot be removed — raises ValueError.
catalog.update("noaa-gfs-analysis", remove_fields=["doi"])

# Re-inspect the live store, refresh CF attrs + snapshot anchor
catalog.update_from_store("noaa-gfs-analysis")
catalog.update_from_store("noaa-gfs-analysis", title="My dataset, v2")

# Auto-derive bbox + temporal bounds for an existing entry
entry = catalog.get("noaa-gfs-analysis")
entry.infer_extent(catalog, update=True)

# For time-appending datasets: refresh only end_datetime + snapshot_id
diff = catalog.update_from_store("noaa-hrrr-forecast", time_only=True)
# {'dataset_snapshot_id': ('abc123', 'xyz789'), 'end_datetime': ('2026-04-01', '2026-04-28')}

# Resync all entries — one commit per entry, each adding a snapshot to its history.
# Run on a schedule, then expire() afterward. See Scaling below for the cost model.
catalog.update_all_from_store()
catalog.expire(datetime.datetime.now(datetime.UTC))  # collapse the new snapshots
```

## Deregister and restore

```python
catalog.deregister("my-dataset")           # reversible: commits a deregistered marker
catalog.restore("my-dataset")              # undo
catalog.get("my-dataset", include_deleted=True)

catalog.deregister("my-dataset", purge=True)  # irreversible: drops the branch
```

Default deregister keeps the branch and commit history. Use `purge=True` only for legal/compliance erasure or throwaway test entries.

## Data freshness

```python
entry.is_stale()          # True if store has new commits since last register/update
entry.last_data_updated() # datetime of current HEAD snapshot

stale = catalog.refresh() # {name: bool} across all entries
```

!!! warning
    `is_stale()` raises `NotImplementedError` for virtual stores (entries with
    virtual chunk containers). It compares icechunk snapshots, but a virtual
    store's source files (e.g. NetCDF on S3) can change without producing a new
    snapshot, so the check would silently miss those changes. Check source file
    modification times manually instead.

## History

```python
catalog.history()                         # 10 most recent ops across all entries
catalog.history(name="noaa-gfs-analysis") # filter to one entry (cheap)
catalog.history(limit=50)
# [{'event': 'registered', 'name': '...', 'timestamp': datetime(...), 'snapshot_id': '...'}, ...]
```

Events: `registered`, `updated`, `deregistered`.

!!! warning
    History costs one snapshot lookup per record returned — a round-trip each on
    object storage. The default `limit=10` is bounded; a large catalog-wide `limit`
    over a deep history is slow (seconds on S3). Pass `name=` for per-entry history —
    it skips non-matching branches and only touches that entry's snapshots.

## Scaling

basal runs on cloud object storage, so every commit and metadata read is a network
round-trip.

**Reads are cheap.** `list`, `get`, `filter`, `facets`, and `sql` are one S3 GET of
the metadata graph — tens of ms, roughly flat in the number of entries.

**Writes are the cost.** Each `register()`/`update()` is one commit (~500 ms on S3),
so bulk registration is best run as a batch/cron job.

Three things grow that cost:

- **Many variables/keys per entry** — bigger metadata blob, slower reads. Keep `variables` lean.
- **Many entries** — reads stay flat; only total write time adds up.
- **Many commits per entry** — `list()` and `history()` scale with snapshots, and this one degrades silently. Run `expire()`.

`expire()` collapses old snapshots to current HEADs, returning reads to baseline. Run
it on a schedule for any update-heavy catalog:

```python
import datetime
catalog.expire(datetime.datetime.now(datetime.UTC))  # keep only current HEADs
catalog.expire(older_than, garbage_collect=True)      # also reclaim object storage
```
