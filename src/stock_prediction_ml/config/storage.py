"""Storage abstraction for local filesystem and S3/MinIO backends."""

from pathlib import Path

import fsspec

from .settings import settings


def data_path(*parts: str) -> str:
    """
    Join path parts onto data_root, returning either a local path string or s3:// URI.

    Args:
        *parts: Path segments (e.g., "raw", "AAPL.parquet")

    Returns:
        Local path string (e.g., "data/raw/AAPL.parquet") or
        S3 URI (e.g., "s3://stock-dev/raw/AAPL.parquet")
    """
    if settings.data_root.startswith("s3://"):
        # S3 path: join with forward slashes, avoid Path which mangles s3://
        return settings.data_root.rstrip("/") + "/" + "/".join(parts)
    else:
        # Local path: use Path for portability
        return str(Path(settings.data_root).joinpath(*parts))


def ensure_parent_dir(path: str) -> None:
    """
    Create the parent directory for a local path. No-op for S3 URIs.

    pandas' to_parquet requires the parent directory to already exist for
    local paths, but has no such requirement for S3.

    Args:
        path: Local path string or s3:// URI, as returned by data_path().
    """
    if not path.startswith("s3://"):
        Path(path).parent.mkdir(parents=True, exist_ok=True)


def storage_options() -> dict:
    """
    Return fsspec kwargs for pandas read/write operations.

    For local storage, returns empty dict (default behavior).
    For S3/MinIO, returns credentials and endpoint configuration.

    Returns:
        Dict with keys: 'key', 'secret', 'client_kwargs' (if S3),
        or empty dict for local storage.
    """
    if settings.data_root.startswith("s3://"):
        return {
            "key": settings.aws_access_key_id,
            "secret": settings.aws_secret_access_key,
            "client_kwargs": {"endpoint_url": settings.s3_endpoint_url},
        }
    return {}


def list_parquet(prefix: str) -> list[str]:
    """
    List all parquet files under a prefix in the data store.

    Args:
        prefix: Data directory prefix (e.g., "raw", "feature")

    Returns:
        List of parquet file paths (local paths or s3:// URIs)
    """
    prefix_path = data_path(prefix)

    if settings.data_root.startswith("s3://"):
        # S3: use fsspec to list objects
        fs = fsspec.filesystem("s3", **storage_options())
        # Remove s3:// prefix for fsspec operations
        s3_path = prefix_path.replace("s3://", "")
        try:
            # fsspec.glob returns full s3:// URIs
            files = fs.glob(f"{s3_path}/*.parquet")
            return [f"s3://{f}" for f in files]
        except FileNotFoundError:
            return []
    else:
        # Local: use Path.glob
        prefix_dir = Path(prefix_path)
        if not prefix_dir.exists():
            return []
        return [str(f) for f in prefix_dir.glob("*.parquet")]
