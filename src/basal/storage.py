"""Icechunk storage construction and configuration utilities.

Two ways to record a dataset's storage config in the catalog:

- ``StorageSpec`` (preferred) — captures the exact kwargs passed to the icechunk
  constructor, so ``to_config()`` is exact and version-independent. Build specs with
  the ``basal.storage`` constructors (``s3_storage``, ``gcs_storage``, ...).
- raw ``icechunk.Storage`` — config is recovered heuristically by parsing
  ``str(storage)`` (``_parse_storage_repr``). icechunk exposes no serialization API, so
  this depends on an undocumented repr format and can break on an icechunk upgrade.
  ``register()`` warns when it falls back to this path.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

import icechunk

# Serializable config keys understood by storage_from_config(), per storage type.
# StorageSpec.to_config() emits only these (drops credential objects / unknown kwargs).
_CONFIG_KEYS: dict[str, tuple[str, ...]] = {
    "s3": ("bucket", "prefix", "region", "anonymous", "from_env", "endpoint_url"),
    "gcs": ("bucket", "prefix", "anonymous", "from_env"),
    "local": ("path",),
    "http": ("base_url",),
    "redirect": ("base_url",),
    "in_memory": (),
}


@dataclass
class StorageSpec:
    """Captured icechunk.Storage construction: exact kwargs + type tag.

    ``build()`` reconstructs the live ``icechunk.Storage`` with full fidelity
    (credentials included). ``to_config()`` emits a JSON-serializable subset for
    catalog metadata — no repr parsing, stable across icechunk versions.

    Build via the ``basal.storage`` constructors (``s3_storage`` etc.), not directly.
    """

    type: str
    kwargs: dict[str, Any] = field(default_factory=dict)

    def build(self) -> icechunk.Storage:
        t = self.type
        if t == "s3":
            return icechunk.s3_storage(**self.kwargs)
        if t == "gcs":
            return icechunk.gcs_storage(**self.kwargs)
        if t == "local":
            return icechunk.local_filesystem_storage(self.kwargs["path"])
        if t == "http":
            return icechunk.http_storage(**self.kwargs)
        if t == "redirect":
            return icechunk.redirect_storage(**self.kwargs)
        if t == "in_memory":
            return icechunk.in_memory_storage()
        raise ValueError(f"Unknown storage type {t!r}.")

    def to_config(self) -> dict[str, Any]:
        """Serializable config dict consumed by storage_from_config()."""
        config: dict[str, Any] = {"type": self.type}
        for key in _CONFIG_KEYS.get(self.type, ()):
            val = self.kwargs.get(key)
            if val is not None and isinstance(val, str | bool | int | float):
                config[key] = val
        return config


def s3_storage(**kwargs: Any) -> StorageSpec:
    """Capture an S3 storage spec. Same kwargs as ``icechunk.s3_storage``."""
    return StorageSpec("s3", kwargs)


def gcs_storage(**kwargs: Any) -> StorageSpec:
    """Capture a GCS storage spec. Same kwargs as ``icechunk.gcs_storage``."""
    return StorageSpec("gcs", kwargs)


def local_filesystem_storage(path: str) -> StorageSpec:
    """Capture a local-filesystem storage spec."""
    return StorageSpec("local", {"path": str(path)})


def http_storage(base_url: str, **kwargs: Any) -> StorageSpec:
    """Capture an HTTP storage spec."""
    return StorageSpec("http", {"base_url": base_url, **kwargs})


def redirect_storage(base_url: str) -> StorageSpec:
    """Capture a redirect storage spec."""
    return StorageSpec("redirect", {"base_url": base_url})


def in_memory_storage() -> StorageSpec:
    """Capture an in-memory storage spec."""
    return StorageSpec("in_memory", {})


def _parse_storage_repr(storage: icechunk.Storage) -> dict[str, str]:
    """Parse icechunk.Storage __str__ into a key-value dict.

    Heuristic: depends on an undocumented repr format. Used only as the fallback
    when register() is given a raw icechunk.Storage instead of a StorageSpec.
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
    return location_from_config(storage_to_config(storage))


def location_from_config(config: dict) -> str:
    """Derive a canonical location URL string from a storage config dict."""
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
