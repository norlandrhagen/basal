"""SQL and similarity search over catalog metadata using DuckDB and arro3.

DuckDB table schema: (name VARCHAR, snapshot_id VARCHAR, metadata JSON).
Use metadata->>'field' for scalar extraction, DuckDB JSON functions for arrays.

An alternative backend could use Apache DataFusion — it would integrate more
tightly with the Arrow/Zarr/Parquet ecosystem. DuckDB is chosen here because
it is single-dep, zero-config, and gives us SQL over arbitrary JSON metadata
and native array_cosine_similarity for vector search.
"""

from __future__ import annotations

import json
import warnings
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

import numpy as np

if TYPE_CHECKING:
    import duckdb

    from .catalog import IcechunkCatalog
    from .entry import Entry


def _build_connection(catalog: IcechunkCatalog) -> duckdb.DuckDBPyConnection:
    import arro3.core as ac
    import duckdb

    entries = catalog.list()
    con = duckdb.connect()

    if not entries:
        con.execute(
            "CREATE TABLE entries (name VARCHAR, snapshot_id VARCHAR, metadata JSON)"
        )
        return con

    names = ac.Array.from_numpy(np.array([e.name for e in entries]))
    snaps = ac.Array.from_numpy(np.array([e.snapshot_id for e in entries]))
    metas = ac.Array.from_numpy(np.array([json.dumps(e.metadata) for e in entries]))

    tbl = ac.Table.from_pydict({"name": names, "snapshot_id": snaps, "metadata": metas})
    con.register("entries", tbl)
    return con


def sql(catalog: IcechunkCatalog, query: str) -> list[tuple]:
    """Run SQL over entries(name VARCHAR, snapshot_id VARCHAR, metadata JSON).

    Use metadata->>'field' for scalar extraction.
    Use list_contains(CAST(metadata->'field' AS VARCHAR[]), 'value') for arrays.

    Examples
    --------
    >>> sql(catalog, "SELECT name FROM entries WHERE metadata->>'owner' = 'NOAA'")
    >>> sql(catalog, "SELECT name, metadata->>'title' AS title FROM entries ORDER BY name")
    >>> sql(catalog, "SELECT name FROM entries WHERE list_contains(CAST(metadata->'keywords' AS VARCHAR[]), 'ocean')")
    """
    return _build_connection(catalog).execute(query).fetchall()


def sql_df(catalog: IcechunkCatalog, query: str):
    """Same as sql() but returns a pandas DataFrame."""
    return _build_connection(catalog).execute(query).df()


# --- similarity search ---


_TEXT_DENYLIST = frozenset(
    {
        "storage_config",
        "virtual_chunk_containers_config",
        "virtual_chunk_containers",
        "dataset_snapshot_id",
    }
)
"""Operational fields excluded from similarity-search text — not semantic content."""


def _entry_text(entry: Entry) -> str:
    """Flatten all string-valued metadata into one text blob for embedding.

    Covers all fields dynamically — any new metadata field is automatically
    included without code changes. Operational fields (storage_config, etc.)
    are excluded since they contain paths/IDs, not semantic content.
    """
    parts = [entry.name]
    for k, v in entry.metadata.items():
        if k in _TEXT_DENYLIST:
            continue
        if isinstance(v, str):
            parts.append(v)
        elif isinstance(v, list):
            parts.extend(x for x in v if isinstance(x, str))
        elif isinstance(v, dict):
            for dk, dv in v.items():
                parts.append(str(dk))
                if isinstance(dv, str):
                    parts.append(dv)
    return " ".join(p for p in parts if p).strip()


def similar(
    catalog: IcechunkCatalog,
    query: str,
    embed_fn: Callable[[list[str]], list[list[float]]] | None = None,
    top_k: int = 5,
) -> list[tuple[Entry, float]]:
    """Find catalog entries most similar to a text query using vector cosine similarity.

    Uses DuckDB's built-in array_cosine_similarity — no external vector DB needed.
    Brute-force over all entries: fast up to ~10k entries.

    Parameters
    ----------
    catalog
        The catalog to search.
    query
        Free-text query, e.g. "sea surface temperature ocean reanalysis".
    embed_fn
        Callable that takes list[str] and returns list of float vectors.
        Defaults to fastembed TextEmbedding (install: pip install fastembed).

        Example with a custom model::

            from fastembed import TextEmbedding
            model = TextEmbedding("BAAI/bge-small-en-v1.5")
            results = similar(catalog, "ocean temperature", embed_fn=lambda t: list(model.embed(t)))

    top_k
        Number of results to return.

    Returns
    -------
    list of (Entry, score) sorted by descending similarity.
    """
    import arro3.core as ac
    import duckdb

    if embed_fn is None:
        try:
            from fastembed import TextEmbedding
        except ImportError as exc:
            raise ImportError(
                "fastembed is required for default embeddings. "
                "Install with: pip install fastembed"
            ) from exc
        _model = TextEmbedding()

        def embed_fn(texts: list[str]) -> list[list[float]]:
            return list(_model.embed(texts))

    entries = catalog.list()
    if not entries:
        return []

    texts = [_entry_text(e) for e in entries]
    all_vecs = embed_fn(texts + [query])

    entry_vecs = np.array(all_vecs[:-1], dtype=np.float32)
    query_vec = np.array(all_vecs[-1], dtype=np.float32)
    dim = len(query_vec)

    tbl = ac.Table.from_pydict(
        {
            "name": ac.Array.from_numpy(np.array([e.name for e in entries])),
            "embedding": ac.Array.from_numpy(entry_vecs),
        }
    )

    con = duckdb.connect()
    con.register("emb", tbl)

    query_vec_sql = f"[{', '.join(str(float(x)) for x in query_vec)}]::FLOAT[{dim}]"
    rows = con.execute(f"""
        SELECT name,
               array_cosine_similarity(embedding::FLOAT[{dim}], {query_vec_sql}) AS score
        FROM emb
        ORDER BY score DESC
        LIMIT {top_k}
    """).fetchall()

    entry_by_name = {e.name: e for e in entries}
    return [(entry_by_name[name], float(score)) for name, score in rows]


# --- schema-based similarity search ---

# In-memory cache keyed by (entry.name, snapshot_id). Automatically valid as long as
# the snapshot hasn't changed — icechunk snapshot_id is content-addressed.
_schema_cache: dict[tuple[str, str], dict[str, Any]] = {}


def _fetch_store_info(entry: Entry) -> tuple[str, dict[str, Any]] | None:
    from .inspect import inspect_store

    cache_key = (entry.name, entry.snapshot_id)
    if cache_key in _schema_cache:
        return entry.name, _schema_cache[cache_key]

    try:
        storage = entry._resolve_storage()
        config = entry._resolve_repo_config()
        info = inspect_store(storage, config=config)
        _schema_cache[cache_key] = info
        return entry.name, info
    except Exception as exc:
        warnings.warn(f"Could not inspect {entry.name!r}: {exc}", stacklevel=4)
        return None


def _populate_schema_table(
    con: duckdb.DuckDBPyConnection, names: list[str], infos: list[dict[str, Any]]
) -> None:
    con.execute("""
        CREATE TABLE schema_tbl (
            dataset_name VARCHAR,
            variable_name VARCHAR,
            dtype         VARCHAR,
            dims          VARCHAR[],
            shape         BIGINT[],
            chunks        BIGINT[],
            attrs         JSON,
            global_attrs  JSON
        )
    """)
    rows = []
    for name, info in zip(names, infos, strict=False):
        global_attrs_json = json.dumps(info.get("global_attrs", {}))
        for var_name, var_meta in info.get("variables", {}).items():
            rows.append(
                (
                    name,
                    var_name,
                    var_meta.get("dtype", ""),
                    var_meta.get("dims", []),
                    var_meta.get("shape", []),
                    var_meta.get("chunks"),
                    json.dumps(var_meta.get("attrs", {})),
                    global_attrs_json,
                )
            )
    con.executemany("INSERT INTO schema_tbl VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)


def _info_to_text(name: str, info: dict[str, Any]) -> str:
    parts = [name]
    for v in info.get("global_attrs", {}).values():
        parts.append(str(v))
    for var_name, var_meta in info.get("variables", {}).items():
        parts.append(var_name)
        parts.extend(str(d) for d in var_meta.get("dims", []))
        for v in var_meta.get("attrs", {}).values():
            parts.append(str(v))
    for coord_name, coord_meta in info.get("coords", {}).items():
        parts.append(coord_name)
        for v in coord_meta.get("attrs", {}).values():
            parts.append(str(v))
    return " ".join(p for p in parts if p).strip()


def similar_by_schema(
    catalog: IcechunkCatalog,
    query: str,
    pre_filter: str | None = None,
    embed_fn: Callable[[list[str]], list[list[float]]] | None = None,
    top_k: int = 5,
    max_workers: int = 8,
) -> list[tuple[Entry, float]]:
    """Find catalog entries most similar to a query using full zarr schema metadata.

    Lazily fetches complete zarr attrs from each registered store at search time —
    all da.attrs, coord attrs, global_attrs, dtype, dims, shape. Richer than
    similar(), which only uses catalog-cached CF attrs.

    Parameters
    ----------
    catalog
        The catalog to search.
    query
        Free-text query, e.g. "daily precipitation over land on a 0.25 degree grid".
    pre_filter
        Optional DuckDB SQL WHERE clause applied to a variable-level schema table
        before embedding. Reduces N before the expensive embedding step.
        Available columns: dataset_name VARCHAR, variable_name VARCHAR, dtype VARCHAR,
        dims list<utf8>, shape list<int64>, chunks list<int64>,
        attrs JSON (variable attrs), global_attrs JSON.
        Example: ``"list_contains(dims, 'lat') AND attrs->>'units' = 'K'"``
    embed_fn
        Callable taking list[str] → list of float vectors.
        Defaults to fastembed TextEmbedding (install: pip install fastembed).
    top_k
        Number of results to return.
    max_workers
        Number of threads for parallel store fetches.

    Returns
    -------
    list of (Entry, score) sorted by descending similarity.
    """
    import duckdb

    if embed_fn is None:
        try:
            from fastembed import TextEmbedding
        except ImportError as exc:
            raise ImportError(
                "fastembed is required for default embeddings. "
                "Install with: pip install fastembed"
            ) from exc
        _model = TextEmbedding()

        def embed_fn(texts: list[str]) -> list[list[float]]:
            return list(_model.embed(texts))

    entries = catalog.list()
    if not entries:
        return []

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        raw = list(pool.map(_fetch_store_info, entries))

    name_info_pairs = [r for r in raw if r is not None]
    if not name_info_pairs:
        return []

    names, infos = zip(*name_info_pairs, strict=False)
    names, infos = list(names), list(infos)

    if pre_filter:
        con = duckdb.connect()
        _populate_schema_table(con, names, infos)
        allowed = {
            row[0]
            for row in con.execute(
                f"SELECT DISTINCT dataset_name FROM schema_tbl WHERE {pre_filter}"
            ).fetchall()
        }
        pairs = [(n, i) for n, i in zip(names, infos, strict=False) if n in allowed]
        if not pairs:
            return []
        names, infos = zip(*pairs, strict=False)
        names, infos = list(names), list(infos)

    texts = [_info_to_text(n, i) for n, i in zip(names, infos, strict=False)]
    all_vecs = embed_fn(texts + [query])

    entry_vecs = np.array(all_vecs[:-1], dtype=np.float32)
    query_vec = np.array(all_vecs[-1], dtype=np.float32)
    dim = len(query_vec)

    import arro3.core as ac

    emb_tbl = ac.Table.from_pydict(
        {
            "name": ac.Array.from_numpy(np.array(names)),
            "embedding": ac.Array.from_numpy(entry_vecs),
        }
    )
    con2 = duckdb.connect()
    con2.register("emb", emb_tbl)
    query_vec_sql = f"[{', '.join(str(float(x)) for x in query_vec)}]::FLOAT[{dim}]"
    rows = con2.execute(f"""
        SELECT name,
               array_cosine_similarity(embedding::FLOAT[{dim}], {query_vec_sql}) AS score
        FROM emb
        ORDER BY score DESC
        LIMIT {top_k}
    """).fetchall()

    entry_by_name = {e.name: e for e in entries}
    return [(entry_by_name[name], float(score)) for name, score in rows]
