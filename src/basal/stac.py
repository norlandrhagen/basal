"""Shared STAC conversion helpers used by Catalog.to_stac() and the STAC API server.

Single source of truth for entry -> STAC Item conversion so the static export
and the API server cannot drift.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .entry import Entry

STAC_VERSION = "1.0.0"


def bbox_overlaps(entry_bbox: list[float], query_bbox: list[float]) -> bool:
    """Inclusive rectangle intersection (touching edges count, per STAC convention)."""
    ew, es, ee, en = entry_bbox
    w, s, e, n = query_bbox
    return ee >= w and ew <= e and en >= s and es <= n


def entry_to_stac_item(
    entry: Entry,
    collection_id: str | None = None,
) -> dict[str, Any]:
    """Convert a catalog Entry to a STAC Item dict.

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


def union_bbox(items: list[dict]) -> list[float]:
    """Union of item bboxes; whole-globe fallback when none have a bbox."""
    bboxes = [i["bbox"] for i in items if i.get("bbox") is not None]
    if not bboxes:
        return [-180.0, -90.0, 180.0, 90.0]
    return [
        min(b[0] for b in bboxes),
        min(b[1] for b in bboxes),
        max(b[2] for b in bboxes),
        max(b[3] for b in bboxes),
    ]
