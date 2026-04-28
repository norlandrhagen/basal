"""Icechunk storage construction and configuration utilities."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

import icechunk


def _parse_storage_repr(storage: icechunk.Storage) -> dict[str, str]:
    """Parse icechunk.Storage __str__ into a key-value dict.

    The repr format is stable: one "key: value" per line after the header line.
    Used by storage_to_config() to extract bucket, prefix, region, etc.
    """
    lines = str(storage).strip().splitlines()
    data: dict[str, str] = {}
    for line in lines[1:]:  # skip "<icechunk.Storage>"
        if ": " in line:
            key, _, value = line.partition(": ")
            data[key.strip()] = value.strip()
    return data


def storage_to_config(storage: icechunk.Storage) -> dict[str, Any]:
    """Derive a serializable config dict from an icechunk.Storage object.

    The resulting dict can be stored in catalog metadata and later passed to
    storage_from_config() to reconstruct the storage. Supports S3, GCS, local,
    HTTP, and redirect storage types.

    Note: from_env credentials cannot be detected from the repr and are omitted.
    For private stores needing no-arg to_xarray(), pass storage_config= explicitly.
    """
    data = _parse_storage_repr(storage)
    stype = data.get("type", "")

    if "S3" in stype:
        config: dict[str, Any] = {
            "type": "s3",
            "bucket": data["bucket"],
            "prefix": data.get("prefix"),
        }
        if "region" in data:
            config["region"] = data["region"]
        if data.get("anonymous") == "True":
            config["anonymous"] = True
        if "endpoint_url" in data:
            config["endpoint_url"] = data["endpoint_url"]
        return config

    if stype == "local filesystem":
        return {"type": "local", "path": data["path"]}

    if stype == "GCS":
        config = {"type": "gcs", "bucket": data["bucket"], "prefix": data.get("prefix")}
        if data.get("anonymous") == "True":
            config["anonymous"] = True
        return config

    if stype == "HTTP":
        return {"type": "http", "base_url": data["url"]}

    if stype == "redirect":
        return {"type": "redirect", "base_url": data["url"]}

    if stype == "in-memory":
        return {"type": "in_memory"}

    return {}


def storage_to_location(storage: icechunk.Storage) -> str:
    """Derive a canonical location URL string from an icechunk.Storage object."""
    config = storage_to_config(storage)
    stype = config.get("type")

    if stype == "s3":
        bucket = config["bucket"]
        prefix = config.get("prefix") or ""
        path = f"{bucket}/{prefix}".rstrip("/")
        return f"s3://{path}"

    if stype == "gcs":
        bucket = config["bucket"]
        prefix = config.get("prefix") or ""
        path = f"{bucket}/{prefix}".rstrip("/")
        return f"gs://{path}"

    if stype == "local":
        return f"file://{config['path']}"

    if stype in ("http", "redirect"):
        return config.get("base_url", "")

    if stype == "in_memory":
        return "memory://"

    return ""


def storage_from_config(config: dict) -> icechunk.Storage:
    """Reconstruct an icechunk.Storage from a serializable config dict.

    Accepts dicts produced by storage_to_config() or hand-written dicts with
    keys: type, bucket, prefix, region, anonymous, from_env, etc.
    No URL parsing — all parameters are explicit.
    """
    t = config.get("type")
    if t == "s3":
        return icechunk.s3_storage(
            bucket=config["bucket"],
            prefix=config.get("prefix"),
            region=config.get("region"),
            anonymous=config.get("anonymous") or None,
            from_env=config.get("from_env") or None,
            endpoint_url=config.get("endpoint_url"),
        )
    if t == "gcs":
        return icechunk.gcs_storage(
            bucket=config["bucket"],
            prefix=config.get("prefix"),
            anonymous=config.get("anonymous") or None,
            from_env=config.get("from_env") or None,
        )
    if t == "local":
        return icechunk.local_filesystem_storage(config["path"])
    if t == "http":
        return icechunk.http_storage(base_url=config["base_url"])
    if t == "redirect":
        return icechunk.redirect_storage(base_url=config["base_url"])
    if t == "in_memory":
        return icechunk.in_memory_storage()
    raise ValueError(
        f"Unknown storage type {t!r}. "
        "Expected one of: 's3', 'gcs', 'local', 'http', 'redirect', 'in_memory'."
    )


def _virtual_chunk_container_to_config(vc: Any) -> dict:
    """Serialize a VirtualChunkContainer object to a config dict.

    Extracts url_prefix, region, anonymous, and endpoint_url directly from
    the container's ObjectStoreConfig — no string parsing.
    """
    url_prefix = vc.url_prefix
    scheme = urlparse(url_prefix).scheme
    if scheme == "s3":
        opts = vc.store[0]  # S3Options
        result: dict = {"url_prefix": url_prefix}
        if opts.region:
            result["region"] = opts.region
        result["anonymous"] = bool(opts.anonymous)
        if opts.endpoint_url:
            result["endpoint_url"] = opts.endpoint_url
        return result
    raise NotImplementedError(
        f"Virtual chunk container scheme {scheme!r} not yet supported for "
        "serialization. Pass config= explicitly at read time."
    )


def _object_store_config_from_virtual_chunk_dict(c: dict) -> icechunk.ObjectStoreConfig:
    """Build an ObjectStoreConfig from a virtual chunk container config dict."""
    url_prefix = c["url_prefix"]
    scheme = urlparse(url_prefix).scheme
    if scheme == "s3":
        opts = icechunk.S3Options(
            region=c.get("region"),
            anonymous=c.get("anonymous", False),
            endpoint_url=c.get("endpoint_url"),
        )
        return icechunk.ObjectStoreConfig.S3(opts)
    raise NotImplementedError(
        f"Virtual chunk container scheme {scheme!r} not yet supported for "
        "automatic RepositoryConfig reconstruction. Pass config= explicitly."
    )


def _repo_config_from_virtual_chunks(
    containers: list[dict],
) -> icechunk.RepositoryConfig:
    """Build a RepositoryConfig with VirtualChunkContainers from serialized config dicts.

    Internal — used to reconstruct RepositoryConfig from stored catalog metadata.
    Each dict: {url_prefix, region?, anonymous?, endpoint_url?}.
    """
    config = icechunk.RepositoryConfig.default()
    for c in containers:
        store_cfg = _object_store_config_from_virtual_chunk_dict(c)
        vc = icechunk.VirtualChunkContainer(url_prefix=c["url_prefix"], store=store_cfg)
        config.set_virtual_chunk_container(vc)
    return config


def _virtual_chunk_credentials_from_config(
    containers: list[dict],
) -> icechunk.credentials.Credentials | None:
    """Build authorize_virtual_chunk_access credentials from stored container config dicts.

    Internal — used to reconstruct credentials from stored catalog metadata.
    Each dict: {url_prefix, anonymous?}.
    """
    if not containers:
        return None
    mapping: dict = {}
    for c in containers:
        prefix = c["url_prefix"]
        scheme = urlparse(prefix).scheme
        if scheme == "s3":
            if c.get("anonymous"):
                cred = icechunk.s3_anonymous_credentials()
            else:
                cred = icechunk.s3_from_env_credentials()
        elif scheme == "gs":
            if c.get("anonymous"):
                cred = icechunk.gcs_anonymous_credentials()
            else:
                cred = icechunk.gcs_from_env_credentials()
        elif scheme in ("http", "https"):
            cred = None
        else:
            raise ValueError(
                f"Cannot build credentials for virtual chunk prefix {prefix!r}. "
                "Pass authorize_virtual_chunk_access explicitly."
            )
        mapping[prefix] = cred
    return icechunk.containers_credentials(mapping)


def default_virtual_chunk_credentials(
    containers: list[str | dict],
) -> icechunk.credentials.Credentials | None:
    """Build anonymous credentials for virtual chunk containers.

    Accepts both string prefixes (treated as anonymous) and config dicts
    from stored virtual_chunk_containers_config metadata.
    Returns None if containers is empty.
    """
    if not containers:
        return None
    configs = [
        {"url_prefix": c, "anonymous": True} if isinstance(c, str) else c
        for c in containers
    ]
    return _virtual_chunk_credentials_from_config(configs)


def storage_from_location(location: str, **kwargs: Any) -> icechunk.Storage:
    """Parse a location URL into an icechunk Storage.

    Convenience utility for explicit use in scripts and tests.
    Not called internally by the catalog — use storage_to_config() /
    storage_from_config() for reproducible, credential-explicit construction.

    Supported schemes:
      s3://bucket/prefix
      gs://bucket/prefix
      http://host/path     (read-only HTTP storage, direct)
      https://host/path    (redirect storage — follows 302 → S3/GCS/etc)
      file:///abs/path
      /abs/path            (bare filesystem path)
    """
    if location.startswith("/") or location.startswith("./"):
        return icechunk.local_filesystem_storage(location)

    parsed = urlparse(location)
    scheme = parsed.scheme
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/").rstrip("/")

    if scheme == "file":
        return icechunk.local_filesystem_storage(parsed.path)
    if scheme == "s3":
        kwargs.setdefault("from_env", True)
        return icechunk.s3_storage(bucket=bucket, prefix=prefix, **kwargs)
    if scheme == "gs":
        kwargs.setdefault("from_env", True)
        return icechunk.gcs_storage(bucket=bucket, prefix=prefix, **kwargs)
    if scheme == "http":
        return icechunk.http_storage(base_url=location, **kwargs)
    if scheme == "https":
        return icechunk.redirect_storage(base_url=location)

    raise ValueError(f"Unsupported location scheme: {location!r}")
