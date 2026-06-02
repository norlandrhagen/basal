"""Thin STAC API server backed by a basal Catalog.

Usage — programmatic:
    from basal.stac_api import create_app
    app = create_app(catalog)
    # then: uvicorn mymodule:app

Usage — env-var-configured default app:
    BASAL_BUCKET=my-bucket BASAL_PREFIX=my-catalog BASAL_REGION=us-west-2 \\
        uvicorn basal.stac_api:app

Env vars:
    BASAL_BUCKET      S3 bucket name (required for default app)
    BASAL_PREFIX      S3 key prefix (default: "")
    BASAL_REGION      AWS region (default: "us-east-1")
    BASAL_ANONYMOUS   "true" / "1" for anonymous access (default: false)
    BASAL_COLLECTION  STAC collection id (default: "basal-catalog")

Requires basal[stac]: uv add "basal[stac]"
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .catalog import Catalog
    from .entry import Entry

STAC_VERSION = "1.0.0"

CONFORMANCE_CLASSES = [
    "https://api.stacspec.org/v1.0.0/core",
    "https://api.stacspec.org/v1.0.0/item-search",
    "https://api.stacspec.org/v1.0.0/ogcapi-features",
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/oas30",
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson",
]


def _entry_to_stac_item(
    entry: Entry,
    collection_id: str | None = None,
) -> dict[str, Any]:
    """Convert any catalog Entry to a STAC Item dict.

    Entries without bbox get null geometry, which is valid per STAC spec
    for non-spatial datasets. Pass ``collection_id`` to include self/collection
    links so STAC browsers can navigate correctly.
    """
    from .schema import _bbox_to_geometry

    bbox = entry.metadata.get("bbox")
    geometry = entry.metadata.get("geometry") or (
        _bbox_to_geometry(bbox) if bbox else None
    )

    start_dt = entry.metadata.get("start_datetime")
    end_dt = entry.metadata.get("end_datetime")
    stac_datetime = None if (start_dt and end_dt) else (start_dt or None)

    properties: dict[str, Any] = {"datetime": stac_datetime}
    if start_dt:
        properties["start_datetime"] = start_dt
    if end_dt:
        properties["end_datetime"] = end_dt
    for field in ("title", "license"):
        if field in entry.metadata:
            properties[field] = entry.metadata[field]
    if "tags" in entry.metadata:
        properties["keywords"] = entry.metadata["tags"]
    if "owner" in entry.metadata:
        properties["providers"] = [{"name": entry.owner, "roles": ["producer"]}]
    if "doi" in entry.metadata:
        properties["sci:doi"] = entry.metadata["doi"]

    links: list[dict] = [{"rel": "root", "href": "/", "type": "application/json"}]
    if collection_id:
        links += [
            {
                "rel": "self",
                "href": f"/collections/{collection_id}/items/{entry.name}",
                "type": "application/geo+json",
            },
            {
                "rel": "collection",
                "href": f"/collections/{collection_id}",
                "type": "application/json",
            },
        ]
    if "doi" in entry.metadata:
        links.append(
            {"rel": "cite-as", "href": f"https://doi.org/{entry.metadata['doi']}"}
        )

    return {
        "type": "Feature",
        "stac_version": STAC_VERSION,
        "stac_extensions": [],
        "id": entry.name,
        "collection": collection_id,
        "geometry": geometry,
        "bbox": list(bbox) if bbox else None,
        "properties": properties,
        "links": links,
        "assets": {
            "data": {
                "href": entry.location,
                "type": "application/vnd+zarr",
                "roles": ["data"],
                "title": entry.metadata.get("title", entry.name),
            }
        },
    }


def _bbox_overlaps(entry_bbox: list[float], query_bbox: list[float]) -> bool:
    ew, es, ee, en = entry_bbox
    w, s, e, n = query_bbox
    return ee >= w and ew <= e and en >= s and es <= n


def create_app(catalog: Catalog, collection_id: str = "basal-catalog"):
    """Create a STAC API FastAPI app backed by ``catalog``.

    Parameters
    ----------
    catalog:
        Opened Catalog to serve.
    collection_id:
        STAC collection id used in all URLs and responses.
    """
    try:
        from fastapi import FastAPI, HTTPException, Query
        from fastapi.middleware.cors import CORSMiddleware

    except ImportError:
        raise ImportError(
            "basal[stac] is required. Install with: uv add 'basal[stac]'"
        ) from None

    app = FastAPI(title="basal STAC API", version="1.0.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
    )

    # --- helpers ---

    def _all_items() -> list[dict]:
        return [_entry_to_stac_item(e, collection_id) for e in catalog.list()]

    def _collection_extent(items: list[dict]) -> dict:
        bboxes = [i["bbox"] for i in items if i["bbox"] is not None]
        if bboxes:
            union_bbox: list[float] = [
                min(b[0] for b in bboxes),
                min(b[1] for b in bboxes),
                max(b[2] for b in bboxes),
                max(b[3] for b in bboxes),
            ]
        else:
            union_bbox = [-180.0, -90.0, 180.0, 90.0]

        starts = [
            p["start_datetime"]
            for i in items
            if (p := i.get("properties")) and p.get("start_datetime")
        ]
        ends = [
            p["end_datetime"]
            for i in items
            if (p := i.get("properties")) and p.get("end_datetime")
        ]
        t_start = min(starts) if starts else None
        t_end = max(ends) if ends else None

        return {
            "spatial": {"bbox": [union_bbox]},
            "temporal": {"interval": [[t_start, t_end]]},
        }

    def _collection_doc(items: list[dict]) -> dict:
        return {
            "type": "Collection",
            "id": collection_id,
            "stac_version": STAC_VERSION,
            "description": "Dataset catalog served by basal",
            "links": [
                {
                    "rel": "items",
                    "href": f"/collections/{collection_id}/items",
                    "type": "application/geo+json",
                },
                {"rel": "root", "href": "/", "type": "application/json"},
                {
                    "rel": "self",
                    "href": f"/collections/{collection_id}",
                    "type": "application/json",
                },
            ],
            "extent": _collection_extent(items),
            "license": "various",
        }

    def _not_found_collection(cid: str):
        raise HTTPException(status_code=404, detail=f"Collection '{cid}' not found")

    def _get_item(item_id: str) -> dict:
        try:
            entry = catalog.get(item_id)
        except Exception as err:
            raise HTTPException(
                status_code=404, detail=f"Item '{item_id}' not found"
            ) from err
        return _entry_to_stac_item(entry, collection_id)

    def _search_response(features: list[dict]) -> dict:
        return {
            "type": "FeatureCollection",
            "features": features,
            "numberReturned": len(features),
            "links": [
                {"rel": "self", "href": "/search", "type": "application/geo+json"}
            ],
        }

    # --- endpoints ---

    @app.get("/")
    def landing_page():
        return {
            "type": "Catalog",
            "id": collection_id,
            "stac_version": STAC_VERSION,
            "description": "basal STAC API",
            "conformsTo": CONFORMANCE_CLASSES,
            "links": [
                {"rel": "self", "href": "/", "type": "application/json"},
                {"rel": "root", "href": "/", "type": "application/json"},
                {
                    "rel": "conformance",
                    "href": "/conformance",
                    "type": "application/json",
                },
                {"rel": "data", "href": "/collections", "type": "application/json"},
                {
                    "rel": "search",
                    "href": "/search",
                    "type": "application/geo+json",
                    "method": "GET",
                },
            ],
        }

    @app.get("/conformance")
    def conformance():
        return {"conformsTo": CONFORMANCE_CLASSES}

    @app.get("/collections")
    def collections():
        items = _all_items()
        return {
            "collections": [_collection_doc(items)],
            "links": [
                {"rel": "self", "href": "/collections", "type": "application/json"}
            ],
        }

    @app.get("/collections/{cid}")
    def collection(cid: str):
        if cid != collection_id:
            _not_found_collection(cid)
        return _collection_doc(_all_items())

    @app.get("/collections/{cid}/items")
    def items(
        cid: str,
        limit: int = Query(default=10, ge=1, le=1000),
        token: str | None = Query(
            default=None, description="Pagination cursor (entry name)"
        ),
    ):
        if cid != collection_id:
            _not_found_collection(cid)
        all_items = sorted(_all_items(), key=lambda i: i["id"])

        if token is not None:
            names = [i["id"] for i in all_items]
            if token not in names:
                raise HTTPException(
                    status_code=400, detail=f"Invalid pagination token: {token!r}"
                )
            all_items = all_items[names.index(token) :]

        page = all_items[:limit]
        next_token = all_items[limit]["id"] if len(all_items) > limit else None

        links: list[dict] = [
            {
                "rel": "self",
                "href": f"/collections/{cid}/items",
                "type": "application/geo+json",
            }
        ]
        if next_token:
            links.append(
                {
                    "rel": "next",
                    "href": f"/collections/{cid}/items?token={next_token}&limit={limit}",
                    "type": "application/geo+json",
                }
            )

        return {
            "type": "FeatureCollection",
            "features": page,
            "numberMatched": len(all_items),
            "numberReturned": len(page),
            "links": links,
        }

    @app.get("/collections/{cid}/items/{item_id}")
    def item(cid: str, item_id: str):
        if cid != collection_id:
            _not_found_collection(cid)
        return _get_item(item_id)

    @app.get("/items/{item_id}")
    def item_root(item_id: str):
        """Root-level item endpoint — STAC browsers navigate here from item links."""
        return _get_item(item_id)

    def _apply_search_filters(
        bbox: list[float] | None,
        datetime_str: str | None,
        ids: list[str] | None,
        limit: int,
    ) -> list[dict]:
        entries = list(catalog.list())

        if ids is not None:
            id_set = set(ids)
            entries = [e for e in entries if e.name in id_set]

        if bbox is not None:
            entries = [
                e
                for e in entries
                if e.metadata.get("bbox") is not None
                and _bbox_overlaps(e.metadata["bbox"], bbox)
            ]

        if datetime_str is not None:
            parts = datetime_str.split("/")
            time_start = parts[0] if parts[0] != ".." else "*"
            time_end = parts[1] if len(parts) > 1 and parts[1] != ".." else "*"
            name_set = {e.name for e in entries}
            entries = [
                e
                for e in catalog.filter(time_start=time_start, time_end=time_end)
                if e.name in name_set
            ]

        return [_entry_to_stac_item(e, collection_id) for e in entries[:limit]]

    @app.get("/search")
    def search_get(
        bbox: str | None = Query(default=None, description="west,south,east,north"),
        datetime: str | None = Query(
            default=None,
            description="ISO8601 interval, e.g. 2020-01-01/2021-01-01 or ../2021-01-01",
        ),
        ids: str | None = Query(
            default=None, description="Comma-separated entry names"
        ),
        limit: int = Query(default=10, ge=1, le=1000),
    ):
        parsed_bbox = [float(x) for x in bbox.split(",")] if bbox else None
        parsed_ids = ids.split(",") if ids else None
        return _search_response(
            _apply_search_filters(parsed_bbox, datetime, parsed_ids, limit)
        )

    @app.post("/search")
    def search_post(body: dict):
        parsed_bbox = body.get("bbox")
        datetime_str = body.get("datetime")
        ids = body.get("ids")
        limit = int(body.get("limit", 10))
        return _search_response(
            _apply_search_filters(parsed_bbox, datetime_str, ids, limit)
        )

    return app


def _catalog_from_env() -> Catalog:
    """Build Catalog from env vars."""
    import icechunk

    from .catalog import Catalog

    bucket = os.environ["BASAL_BUCKET"]
    prefix = os.environ.get("BASAL_PREFIX", "")
    region = os.environ.get("BASAL_REGION", "us-east-1")
    anon = os.environ.get("BASAL_ANONYMOUS", "").lower() in ("1", "true", "yes")
    storage = icechunk.s3_storage(
        bucket=bucket, prefix=prefix, region=region, anonymous=anon
    )
    return Catalog.open(storage)


# Module-level app for `uvicorn basal.stac_api:app`.
# Only instantiated when BASAL_BUCKET is set so that importing the module
# for create_app() does not attempt a catalog connection.
if os.environ.get("BASAL_BUCKET"):
    _collection_id = os.environ.get("BASAL_COLLECTION", "basal-catalog")
    app = create_app(_catalog_from_env(), collection_id=_collection_id)
