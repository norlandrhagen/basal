"""Benchmark catalog list() performance at various scales.

Creates a local catalog with N fake entries (each updated M times) and measures:
- naive list() approach (1 + 2N calls: list_branches + N lookup_branch + N lookup_snapshot)
- fast list() using inspect_repo_info (1 call, all data in one read)

The M dimension is the key test: every update adds a snapshot, and inspect_repo_info
reads ALL snapshots, so fast list() cost should grow with N*M, not N. The bench also
runs catalog.expire() to show it collapses history back to one snapshot per entry,
restoring list() to scale with N alone.

Run: uv run python scripts/benchmark_catalog.py
"""

import datetime
import statistics
import tempfile
import time

import icechunk
from basal import Catalog
from basal.entry import Entry

SIZES = [10, 100, 500, 1000]
UPDATES = [0, 10]  # M: updates applied per entry after registration
REPEATS = 3


def make_catalog(path: str, n: int, m: int = 0) -> Catalog:
    storage = icechunk.local_filesystem_storage(path)
    catalog = Catalog.create(storage)
    main_snap = catalog._repo.lookup_branch("main")
    for i in range(n):
        name = f"dataset-{i:05d}"
        meta = {
            "location": f"s3://fake-bucket/{name}",
            "owner": f"owner-{i % 10}",
            "format": "icechunk",
            "title": f"Fake Dataset {i}",
            "keywords": ["fake", "benchmark", f"group-{i % 5}"],
            "variables": [f"var_{j}" for j in range(4)],
            "dims": {"time": 100 * i, "lat": 90, "lon": 180},
        }
        catalog._repo.create_branch(name, main_snap)
        session = catalog._repo.writable_session(name)
        session.commit(f"register {name}", metadata=meta, allow_empty=True)
        for u in range(m):
            session = catalog._repo.writable_session(name)
            session.commit(
                f"update {name}", metadata={**meta, "rev": u}, allow_empty=True
            )
    return catalog


def n_snapshots(catalog: Catalog) -> int:
    return len(catalog._repo.inspect_repo_info()["snapshots"])


def list_current(catalog: Catalog) -> list[Entry]:
    """Naive O(N) implementation: list_branches + N*(lookup_branch + lookup_snapshot)."""
    return [
        catalog.get(name) for name in catalog._repo.list_branches() if name != "main"
    ]


def list_fast(catalog: Catalog) -> list[Entry]:
    """Single inspect_repo_info call — all branches + snapshot metadata at once."""
    info = catalog._repo.inspect_repo_info()
    snaps_by_id = {s["id"]: s for s in info["snapshots"]}
    return [
        Entry(
            name=name,
            snapshot_id=snap_id,
            metadata=snaps_by_id[snap_id].get("metadata", {}),
            written_at=snaps_by_id[snap_id].get("flushed_at"),
        )
        for name, snap_id in info["branches"].items()
        if name != "main"
        and snap_id in snaps_by_id
        and snaps_by_id[snap_id].get("metadata", {}).get("location")
    ]


def bench(fn, catalog, repeats):
    times = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        result = fn(catalog)
        times.append(time.perf_counter() - t0)
    return statistics.median(times), result


def main(sizes=SIZES, updates=UPDATES) -> None:
    hdr = (
        f"{'N':>6}  {'M':>3}  {'snaps':>7}  {'current (ms)':>13}  {'fast (ms)':>11}  "
        f"{'speedup':>8}  {'fast post-expire (ms)':>21}  {'snaps post':>11}"
    )
    print(hdr)
    print("-" * len(hdr))

    for n in sizes:
        for m in updates:
            with tempfile.TemporaryDirectory() as tmp:
                catalog = make_catalog(tmp, n, m)
                snaps = n_snapshots(catalog)

                t_current, r_current = bench(list_current, catalog, REPEATS)
                t_fast, r_fast = bench(list_fast, catalog, REPEATS)

                # expire history, then re-measure fast list()
                cutoff = datetime.datetime.now(datetime.UTC)
                catalog.expire(cutoff)
                snaps_post = n_snapshots(catalog)
                t_fast_post, r_post = bench(list_fast, catalog, REPEATS)

                match = len(r_current) == len(r_fast) == len(r_post) == n
                speedup = t_current / t_fast if t_fast > 0 else float("inf")
                flag = "" if match else "  MISMATCH"

                print(
                    f"{n:>6}  {m:>3}  {snaps:>7}  {t_current*1000:>13.1f}  "
                    f"{t_fast*1000:>11.1f}  {speedup:>7.1f}x  {t_fast_post*1000:>21.1f}  "
                    f"{snaps_post:>11}{flag}"
                )


if __name__ == "__main__":
    main()
