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
from collections.abc import Callable
from typing import TYPE_CHECKING

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
