# STAC

!!! warning
    Experimental — may change at any time.

## Export

```python
stac = catalog.to_stac(collection_id="my-catalog")
stac["collection"]  # STAC Collection dict
stac["items"]       # list of STAC Item dicts
```

Entries without `bbox` are exported with null geometry (valid per STAC for non-spatial datasets). `geometry` (GeoJSON Polygon) is auto-derived from `bbox`. The export uses the same conversion as the STAC API server, so the two never drift.

## STAC API server

Requires `basal[stac]`.

```python
from basal.stac_api import create_app
app = create_app(catalog)
# uvicorn mymodule:app --host 0.0.0.0 --port 8000
```

CLI via env vars:

```bash
BASAL_BUCKET=carbonplan-share \
BASAL_PREFIX=basal/public_icechunk_stores \
BASAL_REGION=us-west-2 \
BASAL_ANONYMOUS=true \
uvicorn basal.stac_api:app --host 0.0.0.0 --port 8000
```

Endpoints: `GET /collections`, `GET /collections/{id}/items`, `GET /search`, `POST /search`. CORS enabled.
