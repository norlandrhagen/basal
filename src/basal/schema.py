from __future__ import annotations

from datetime import datetime

REQUIRED_FIELDS = {"location", "format"}

RECOMMENDED_FIELDS: dict[str, str] = {
    "title": "Human-readable dataset name (STAC properties.title)",
    "owner": "Producing organization or person",
    "bbox": "Spatial extent [west, south, east, north] WGS84 (STAC bbox)",
    "start_datetime": "Coverage start, ISO 8601 (STAC properties.start_datetime)",
    "end_datetime": "Coverage end, ISO 8601, omit if ongoing (STAC properties.end_datetime)",
    "license": "SPDX identifier e.g. 'CC-BY-4.0' (STAC properties.license)",
    "tags": "List of keyword strings (STAC properties.keywords)",
    "doi": "Dataset DOI (STAC sci:doi extension)",
}
"""Recommended metadata fields with descriptions and STAC spec equivalents.

Full STAC Item spec: https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md
STAC scientific extension: https://github.com/stac-extensions/scientific
"""


def _bbox_to_geometry(bbox: list | tuple) -> dict:
    """Convert [west, south, east, north] bbox to GeoJSON Polygon."""
    w, s, e, n = bbox
    return {
        "type": "Polygon",
        "coordinates": [[[w, s], [e, s], [e, n], [w, n], [w, s]]],
    }


def validate(metadata: dict) -> None:
    missing = REQUIRED_FIELDS - metadata.keys()
    if missing:
        raise ValueError(f"Missing required fields: {missing}")
    if "bbox" in metadata:
        _validate_bbox(metadata["bbox"])
        if "geometry" not in metadata:
            metadata["geometry"] = _bbox_to_geometry(metadata["bbox"])
    for field in ("start_datetime", "end_datetime"):
        if field in metadata:
            _validate_datetime_str(metadata[field], field)


def _validate_bbox(bbox: object) -> None:
    if not isinstance(bbox, (list | tuple)) or len(bbox) != 4:
        raise ValueError("bbox must be a list of 4 numbers [west, south, east, north]")
    try:
        w, s, e, n = (float(x) for x in bbox)
    except (TypeError, ValueError) as err:
        raise ValueError("bbox values must be numeric") from err

    if not (-90 <= s <= 90 and -90 <= n <= 90):
        raise ValueError(f"bbox south={s}, north={n} must be in [-90, 90]")
    if s > n:
        raise ValueError(f"bbox south={s} must be <= north={n}")


def _validate_datetime_str(val: object, field: str) -> None:
    if val is None:
        return
    if not isinstance(val, str):
        raise ValueError(f"{field} must be an ISO 8601 string or None, got {type(val)}")
    try:
        datetime.fromisoformat(val.replace("Z", "+00:00"))
    except ValueError as err:
        raise ValueError(f"{field}={val!r} is not a valid ISO 8601 datetime") from err
