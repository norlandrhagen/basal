"""Benchmark catalog list() performance at various scales.

Creates a local catalog with N fake entries and measures:
- current list() approach (1 + 2N calls: list_tags + N lookup_tag + N lookup_snapshot)
- fast list() using inspect_repo_info (1 call, all data in one read)

Run: uv run python scripts/benchmark_catalog.py
"""

import statistics
import tempfile
import time

import icechunk
from basal import IcechunkCatalog
from basal.core import Entry

SIZES = [10, 100, 500, 1000]
REPEATS = 3


def make_catalog(path: str, n: int) -> IcechunkCatalog:
    storage = icechunk.local_filesystem_storage(path)
    catalog = IcechunkCatalog.create(storage)
    for i in range(n):
        catalog.register(
            f"dataset-{i:05d}",
            location=f"s3://fake-bucket/dataset-{i:05d}",
            owner=f"owner-{i % 10}",
            title=f"Fake Dataset {i}",
            keywords=["fake", "benchmark", f"group-{i % 5}"],
            variables=[f"var_{j}" for j in range(4)],
            dims={"time": 100 * i, "lat": 90, "lon": 180},
        )
    return catalog


def list_current(catalog: IcechunkCatalog) -> list[Entry]:
    """Current implementation: list_tags + N*(lookup_tag + lookup_snapshot)."""
    return [catalog.get(name) for name in catalog._repo.list_tags()]


def list_fast(catalog: IcechunkCatalog) -> list[Entry]:
    """Single inspect_repo_info call — all tags + snapshot metadata at once."""
    info = catalog._repo.inspect_repo_info()
    snaps_by_id = {s["id"]: s for s in info["snapshots"]}
    return [
        Entry(
            name=name,
            snapshot_id=snap_id,
            metadata=snaps_by_id[snap_id].get("metadata", {}),
            written_at=snaps_by_id[snap_id].get("flushed_at"),
        )
        for name, snap_id in info["tags"].items()
        if snap_id in snaps_by_id
        and snaps_by_id[snap_id].get("metadata", {}).get("location")
    ]


def bench(fn, catalog, repeats):
    times = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        result = fn(catalog)
        times.append(time.perf_counter() - t0)
    return statistics.median(times), result


print(
    f"{'N':>6}  {'current (ms)':>14}  {'fast (ms)':>12}  {'speedup':>8}  {'entries match':>14}"
)
print("-" * 65)

for n in SIZES:
    with tempfile.TemporaryDirectory() as tmp:
        catalog = make_catalog(tmp, n)

        t_current, r_current = bench(list_current, catalog, REPEATS)
        t_fast, r_fast = bench(list_fast, catalog, REPEATS)

        match = len(r_current) == len(r_fast) == n
        speedup = t_current / t_fast if t_fast > 0 else float("inf")

        print(
            f"{n:>6}  {t_current*1000:>14.1f}  {t_fast*1000:>12.1f}  {speedup:>8.1f}x  {'✓' if match else '✗':>14}"
        )
