"""Build zarr stores from URIs using obstore as the cloud backend.

Supports: s3://, gs:// / gcs://, az:// / abfs://, and local paths.
``store_config`` is forwarded directly to the obstore store constructor's
``config=`` parameter — use it for credentials and provider-specific options:

    {"skip_signature": True}                       # anonymous (S3 or GCS)
    {"skip_signature": True, "region": "us-east-1"}  # anonymous S3 with region
    {}                                             # default credentials from env
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

if TYPE_CHECKING:
    import zarr


def build_zarr_store(
    location: str,
    store_config: dict[str, Any] | None = None,
) -> zarr.storage.Store | str:
    """Return a zarr Store for ``location`` using obstore for cloud URIs.

    For local paths returns the path string unchanged — zarr handles those
    natively without obstore.

    Parameters
    ----------
    location:
        URI to the Zarr store. Supported schemes: ``s3://``, ``gs://`` /
        ``gcs://``, ``az://`` / ``abfs://``, or a local filesystem path.
    store_config:
        Dict passed to the obstore store's ``config=`` parameter.
        Use ``{"skip_signature": True}`` for public/anonymous access.
    """
    import zarr

    cfg = store_config or {}
    parsed = urlparse(location)
    scheme = parsed.scheme.lower()

    if scheme in ("gs", "gcs"):
        from obstore.store import GCSStore

        store = GCSStore(parsed.netloc, prefix=parsed.path.lstrip("/"), config=cfg)
        return zarr.storage.ObjectStore(store, read_only=True)

    if scheme == "s3":
        from obstore.store import S3Store

        store = S3Store(parsed.netloc, prefix=parsed.path.lstrip("/"), config=cfg)
        return zarr.storage.ObjectStore(store, read_only=True)

    if scheme in ("az", "abfs"):
        from obstore.store import AzureStore

        store = AzureStore(parsed.netloc, prefix=parsed.path.lstrip("/"), config=cfg)
        return zarr.storage.ObjectStore(store, read_only=True)

    if scheme in ("", "file"):
        # Local path — zarr handles natively; obstore LocalStore also works but
        # passing the path string is simpler and avoids the extra wrapping.
        path = parsed.path
        return path

    raise ValueError(
        f"Unsupported URI scheme {scheme!r} in location {location!r}. "
        "Supported: s3://, gs://, gcs://, az://, abfs://, or a local path."
    )
