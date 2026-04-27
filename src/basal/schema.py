from __future__ import annotations

from datetime import datetime

REQUIRED_FIELDS = {"location", "format"}


def validate(metadata: dict) -> None:
    missing = REQUIRED_FIELDS - metadata.keys()
    if missing:
        raise ValueError(f"Missing required fields: {missing}")
    if "bbox" in metadata:
        _validate_bbox(metadata["bbox"])
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
