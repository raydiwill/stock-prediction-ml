"""Unit tests for storage abstraction (data_path, storage_options, list_parquet)."""

from unittest.mock import MagicMock, patch

from stock_prediction_ml.config.settings import settings
from stock_prediction_ml.config.storage import (
    data_path,
    ensure_parent_dir,
    list_parquet,
    storage_options,
)


class TestDataPathLocal:
    """Test data_path() with local filesystem."""

    def test_data_path_single_part(self, monkeypatch):
        """data_path with single argument returns joined path."""
        monkeypatch.setattr(settings, "data_root", "data")
        result = data_path("raw")
        assert result == "data/raw" or result == "data\\raw"  # Platform-agnostic

    def test_data_path_multiple_parts(self, monkeypatch):
        """data_path with multiple arguments joins all parts."""
        monkeypatch.setattr(settings, "data_root", "data")
        result = data_path("raw", "AAPL.parquet")
        # Normalize for cross-platform comparison
        assert "raw" in result and "AAPL.parquet" in result

    def test_data_path_nested_parts(self, monkeypatch):
        """data_path handles multiple nested parts."""
        monkeypatch.setattr(settings, "data_root", "data")
        result = data_path("processed", "2024", "AAPL.parquet")
        assert "processed" in result and "2024" in result and "AAPL.parquet" in result

    def test_data_path_returns_string(self, monkeypatch):
        """data_path always returns a string, never a Path object."""
        monkeypatch.setattr(settings, "data_root", "data")
        result = data_path("raw", "AAPL.parquet")
        assert isinstance(result, str)


class TestDataPathS3:
    """Test data_path() with S3/MinIO URIs."""

    def test_data_path_s3_single_part(self, monkeypatch):
        """data_path with S3 root returns s3:// URI."""
        monkeypatch.setattr(settings, "data_root", "s3://stock-dev")
        result = data_path("raw")
        assert result == "s3://stock-dev/raw"

    def test_data_path_s3_multiple_parts(self, monkeypatch):
        """data_path with S3 root joins parts with forward slashes."""
        monkeypatch.setattr(settings, "data_root", "s3://stock-dev")
        result = data_path("raw", "AAPL.parquet")
        assert result == "s3://stock-dev/raw/AAPL.parquet"

    def test_data_path_s3_no_double_slash(self, monkeypatch):
        """data_path with S3 root does not collapse double slashes."""
        monkeypatch.setattr(settings, "data_root", "s3://stock-dev")
        result = data_path("raw", "AAPL.parquet")
        # Verify s3:// is intact (not collapsed to s3:/)
        assert result.startswith("s3://")
        assert result.count("://") == 1

    def test_data_path_s3_trailing_slash_removed(self, monkeypatch):
        """data_path strips trailing slash from s3:// root."""
        monkeypatch.setattr(settings, "data_root", "s3://stock-dev/")
        result = data_path("raw", "AAPL.parquet")
        assert result == "s3://stock-dev/raw/AAPL.parquet"
        assert result.count("//") == 1  # Only s3://

    def test_data_path_s3_returns_string(self, monkeypatch):
        """data_path with S3 root always returns a string."""
        monkeypatch.setattr(settings, "data_root", "s3://stock-dev")
        result = data_path("raw", "AAPL.parquet")
        assert isinstance(result, str)


class TestStorageOptionsLocal:
    """Test storage_options() with local filesystem."""

    def test_storage_options_local_empty(self, monkeypatch):
        """storage_options returns empty dict for local filesystem."""
        monkeypatch.setattr(settings, "data_root", "data")
        result = storage_options()
        assert result == {}
        assert isinstance(result, dict)


class TestStorageOptionsS3:
    """Test storage_options() with S3/MinIO."""

    def test_storage_options_s3_has_credentials(self, monkeypatch):
        """storage_options returns AWS credentials for S3."""
        monkeypatch.setattr(settings, "data_root", "s3://stock-dev")
        monkeypatch.setattr(settings, "aws_access_key_id", "test-key")
        monkeypatch.setattr(settings, "aws_secret_access_key", "test-secret")
        monkeypatch.setattr(settings, "s3_endpoint_url", "http://minio:9000")

        result = storage_options()
        assert result["key"] == "test-key"
        assert result["secret"] == "test-secret"

    def test_storage_options_s3_has_endpoint(self, monkeypatch):
        """storage_options includes S3 endpoint in client_kwargs."""
        monkeypatch.setattr(settings, "data_root", "s3://stock-dev")
        monkeypatch.setattr(settings, "aws_access_key_id", "test-key")
        monkeypatch.setattr(settings, "aws_secret_access_key", "test-secret")
        monkeypatch.setattr(settings, "s3_endpoint_url", "http://minio:9000")

        result = storage_options()
        assert "client_kwargs" in result
        assert result["client_kwargs"]["endpoint_url"] == "http://minio:9000"

    def test_storage_options_s3_structure(self, monkeypatch):
        """storage_options for S3 has expected keys."""
        monkeypatch.setattr(settings, "data_root", "s3://stock-dev")
        monkeypatch.setattr(settings, "aws_access_key_id", "key")
        monkeypatch.setattr(settings, "aws_secret_access_key", "secret")
        monkeypatch.setattr(settings, "s3_endpoint_url", "http://minio:9000")

        result = storage_options()
        assert set(result.keys()) == {"key", "secret", "client_kwargs"}


class TestEnsureParentDir:
    """Test ensure_parent_dir() for local paths."""

    def test_ensure_parent_dir_creates_directory(self, tmp_path, monkeypatch):
        """ensure_parent_dir creates parent directory for local path."""
        test_dir = tmp_path / "subdir" / "nested"
        path = str(test_dir / "file.parquet")

        ensure_parent_dir(path)
        assert test_dir.exists()

    def test_ensure_parent_dir_idempotent(self, tmp_path, monkeypatch):
        """ensure_parent_dir is idempotent (can be called multiple times)."""
        test_dir = tmp_path / "subdir"
        path = str(test_dir / "file.parquet")

        ensure_parent_dir(path)
        ensure_parent_dir(path)
        assert test_dir.exists()

    def test_ensure_parent_dir_s3_noop(self, monkeypatch):
        """ensure_parent_dir is a no-op for S3 URIs."""
        # Should not raise or do anything for S3 paths
        ensure_parent_dir("s3://stock-dev/raw/AAPL.parquet")
        # If it didn't raise, the test passes


class TestListParquetLocal:
    """Test list_parquet() with local filesystem."""

    def test_list_parquet_local_finds_files(self, tmp_path, monkeypatch):
        """list_parquet finds .parquet files in local directory."""
        monkeypatch.setattr(settings, "data_root", str(tmp_path))

        # Create test files
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        (raw_dir / "AAPL.parquet").touch()
        (raw_dir / "GOOGL.parquet").touch()
        (raw_dir / "README.txt").touch()  # Should not be included

        result = list_parquet("raw")
        assert len(result) == 2
        assert any("AAPL.parquet" in f for f in result)
        assert any("GOOGL.parquet" in f for f in result)
        assert not any("README.txt" in f for f in result)

    def test_list_parquet_local_empty_directory(self, tmp_path, monkeypatch):
        """list_parquet returns empty list for directory with no parquets."""
        monkeypatch.setattr(settings, "data_root", str(tmp_path))

        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()

        result = list_parquet("raw")
        assert result == []

    def test_list_parquet_local_nonexistent_directory(self, tmp_path, monkeypatch):
        """list_parquet returns empty list for nonexistent directory."""
        monkeypatch.setattr(settings, "data_root", str(tmp_path))

        result = list_parquet("raw")
        assert result == []

    def test_list_parquet_local_returns_strings(self, tmp_path, monkeypatch):
        """list_parquet returns list of strings."""
        monkeypatch.setattr(settings, "data_root", str(tmp_path))

        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        (raw_dir / "AAPL.parquet").touch()

        result = list_parquet("raw")
        assert isinstance(result, list)
        assert all(isinstance(f, str) for f in result)


class TestListParquetS3:
    """Test list_parquet() with S3/MinIO."""

    def test_list_parquet_s3_uses_fsspec(self, monkeypatch):
        """list_parquet uses fsspec for S3 URIs."""
        monkeypatch.setattr(settings, "data_root", "s3://stock-dev")
        monkeypatch.setattr(settings, "aws_access_key_id", "key")
        monkeypatch.setattr(settings, "aws_secret_access_key", "secret")
        monkeypatch.setattr(settings, "s3_endpoint_url", "http://minio:9000")

        mock_fs = MagicMock()
        mock_fs.glob.return_value = ["stock-dev/raw/AAPL.parquet", "stock-dev/raw/GOOGL.parquet"]

        with patch("stock_prediction_ml.config.storage.fsspec.filesystem", return_value=mock_fs):
            result = list_parquet("raw")

        assert len(result) == 2
        assert all(f.startswith("s3://") for f in result)

    def test_list_parquet_s3_handles_missing_prefix(self, monkeypatch):
        """list_parquet returns empty list for nonexistent S3 prefix."""
        monkeypatch.setattr(settings, "data_root", "s3://stock-dev")
        monkeypatch.setattr(settings, "aws_access_key_id", "key")
        monkeypatch.setattr(settings, "aws_secret_access_key", "secret")
        monkeypatch.setattr(settings, "s3_endpoint_url", "http://minio:9000")

        mock_fs = MagicMock()
        mock_fs.glob.side_effect = FileNotFoundError()

        with patch("stock_prediction_ml.config.storage.fsspec.filesystem", return_value=mock_fs):
            result = list_parquet("raw")

        assert result == []

    def test_list_parquet_s3_returns_s3_uris(self, monkeypatch):
        """list_parquet returns s3:// URIs for S3 storage."""
        monkeypatch.setattr(settings, "data_root", "s3://stock-dev")
        monkeypatch.setattr(settings, "aws_access_key_id", "key")
        monkeypatch.setattr(settings, "aws_secret_access_key", "secret")
        monkeypatch.setattr(settings, "s3_endpoint_url", "http://minio:9000")

        mock_fs = MagicMock()
        mock_fs.glob.return_value = ["stock-dev/raw/AAPL.parquet"]

        with patch("stock_prediction_ml.config.storage.fsspec.filesystem", return_value=mock_fs):
            result = list_parquet("raw")

        assert result[0] == "s3://stock-dev/raw/AAPL.parquet"
