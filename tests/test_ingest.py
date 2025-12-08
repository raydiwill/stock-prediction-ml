from unittest.mock import Mock

import pandas as pd
import pytest
from sqlalchemy.exc import IntegrityError

from stock_prediction_ml.db.ingest import (
    dataframe_to_rows,
    ingest_data_into_db,
    ingest_records_in_batches,
    input_hashing,
    map_records_to_db,
    map_to_db_dict,
    normalize_rows,
    read_validated_file,
)

# ========== Fixtures ==========


@pytest.fixture
def sample_dataframe():
    """
    Fixture to create a sample DataFrame for testing.
    Hint:
    - Return a DataFrame with 2-3 rows containing all required columns
    - Use realistic values
    """
    return pd.DataFrame(
        {
            "symbol": ["AAPL", "MSFT"],
            "date": ["2024-01-01", "2024-01-01"],
            "open": [150.0, 300.0],
            "high": [155.0, 305.0],
            "low": [149.0, 299.0],
            "close": [152.0, 303.0],
            "volume": [1000000, 2000000],
            "adj_close": [152.0, 303.0],
            "pulled_at": [pd.Timestamp.now(), pd.Timestamp.now()],
            "source": ["marketstack_api", "marketstack_api"],
        }
    )


@pytest.fixture
def sample_test_row():
    """
    Fixture for a test row dict.
    """
    return {
        "symbol": "aapl",
        "date": "2024-01-01",
        "open": 150.0,
        "high": 155.0,
        "low": 149.0,
        "close": 152.0,
        "volume": 1000000,
        "adj_close": 152.0,
        "pulled_at": pd.Timestamp.now(),
        "parquet_path": "test.parquet",
        "validated": True,
        "source": "marketstack_api",
    }


@pytest.fixture
def sample_normalized_row():
    """
    Fixture for a normalized row dict.
    """
    return {
        "symbol": "AAPL",
        "date": pd.Timestamp("2024-01-01"),
        "open": 150.0,
        "high": 155.0,
        "low": 149.0,
        "close": 152.0,
        "volume": 1000000,
        "adj_close": 152.0,
        "pulled_at": pd.Timestamp.now(),
        "parquet_path": "test.parquet",
        "validated": True,
        "source": "marketstack_api",
    }


@pytest.fixture
def make_mock_session(mocker):
    session = Mock()
    session.bulk_save_objects = Mock()
    session.commit = Mock()
    session.rollback = Mock()
    session.add = Mock()

    # This simulates calling get_db() and getting a generator back
    mocker.patch("stock_prediction_ml.db.ingest.get_db", return_value=(x for x in [session]))

    return session


# ========== Test: read_validated_file ==========


def test_read_validated_file_default_path(sample_dataframe, mocker, tmp_path):
    """
    Test reading from default path.
    """
    parquet_path = tmp_path / "data" / "processed" / "validated_data.parquet"
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    sample_dataframe.to_parquet(parquet_path)
    mocker.patch("stock_prediction_ml.db.ingest.PROJECT_ROOT", tmp_path)

    df = read_validated_file()

    pd.testing.assert_frame_equal(sample_dataframe, df)


def test_read_validated_file_custom_path(sample_dataframe, tmp_path):
    """
    Test reading from custom path.
    """
    custom_path = tmp_path / "data" / "processed" / "custom_data.parquet"
    custom_path.parent.mkdir(parents=True, exist_ok=True)

    sample_dataframe.to_parquet(custom_path)

    df = read_validated_file(custom_path)

    pd.testing.assert_frame_equal(sample_dataframe, df)


# ========== Test: dataframe_to_rows ==========


def test_dataframe_to_rows(sample_dataframe):
    """
    Test DataFrame conversion to list of dicts.
    """
    rows = dataframe_to_rows(sample_dataframe)

    assert len(rows) == 2
    assert rows[0]["validated"]
    assert "parquet_path" in rows[0]


# ========== Test: normalize_rows ==========


def test_normalize_rows_uppercases_symbol(sample_test_row):
    """
    Test symbol is uppercased.
    """
    norm_row = normalize_rows(sample_test_row)

    assert norm_row["symbol"] == "AAPL"


def test_normalize_rows_converts_date_to_timestamp(sample_test_row):
    """
    Test date is converted to pandas Timestamp.
    """
    norm_row = normalize_rows(sample_test_row)

    assert isinstance(norm_row["date"], pd.Timestamp)


# ========== Test: input_hashing ==========


def test_input_hashing_deterministic(sample_normalized_row):
    """
    Test hash is deterministic (same input = same hash).
    """
    row2 = sample_normalized_row.copy()

    hash1 = input_hashing(sample_normalized_row)
    hash2 = input_hashing(row2)

    assert hash1 == hash2
    assert len(hash1) == 64


def test_input_hashing_different_data_produces_different_hash(sample_normalized_row):
    """
    Test different data produces different hash.
    """
    row2 = sample_normalized_row.copy()
    row2["close"] = 160.2

    hash1 = input_hashing(sample_normalized_row)
    hash2 = input_hashing(row2)
    assert hash1 != hash2


# ========== Test: map_to_db_dict ==========

def test_map_to_db_dict_converts_types(sample_normalized_row):
    """
    Test mapping converts types correctly.
    """

    mapped = map_to_db_dict(sample_normalized_row)

    assert isinstance(mapped["open"], float)
    assert isinstance(mapped["symbol"], str)
    assert "hash_input" in mapped
    assert len(mapped["hash_input"]) == 64


def test_map_to_db_dict_sets_defaults(sample_normalized_row):
    """
    Test default values are set.
    """

    row = sample_normalized_row.copy()
    row.pop("source")

    mapped = map_to_db_dict(row)
    assert mapped["source"] == "marketstack_api"
    assert mapped["validated"]


# ========== Test: map_records_to_db ==========


def test_map_records_to_db_creates_orm_objects(sample_normalized_row):
    """
    Test ORM objects are instantiated.
    """
    sample_row_2 = {
        "symbol": "TSLA",
        "date": pd.Timestamp("2024-01-01"),
        "open": 152.0,
        "high": 151.0,
        "low": 143.0,
        "close": 112.0,
        "volume": 1200000,
        "adj_close": 112.0,
        "pulled_at": pd.Timestamp.now(),
        "parquet_path": "test.parquet",
        "validated": True,
        "source": "marketstack_api",
    }

    records = [sample_normalized_row, sample_row_2]
    orm_objects = map_records_to_db(records)

    assert len(orm_objects) == 2
    assert type(orm_objects[0]).__name__ == "RawStockData"
    assert orm_objects[0].symbol == "AAPL"


# ========== Test: ingest_records_in_batches ==========


def test_ingest_records_in_batches_small_dataset(make_mock_session):
    """
    Test single transaction for small dataset (<10K rows).
    """

    # Create 100 mock ORM objects
    orm_objects = [Mock() for _ in range(100)]

    # Call the function
    inserted = ingest_records_in_batches(orm_objects)

    # Assertions using the fixture
    make_mock_session.bulk_save_objects.assert_called_once_with(orm_objects)
    make_mock_session.commit.assert_called_once()
    assert inserted == 100


def test_ingest_records_in_batches_handles_integrity_error(make_mock_session):
    """
    Test fallback to row-by-row on IntegrityError.
    """
    # Configure the fixture to raise an error
    make_mock_session.bulk_save_objects.side_effect = IntegrityError(
        "dup", "params", "orig"
    )

    # Create 10 mock objects
    orm_objects = [Mock() for _ in range(10)]

    inserted = ingest_records_in_batches(orm_objects)

    # Assertions
    make_mock_session.rollback.assert_called()  # Should rollback the bulk attempt
    assert make_mock_session.add.call_count == 10  # Should try adding one by one
    assert inserted == 10


# ========== Test: ingest_data_into_db ==========


def test_ingest_data_into_db_returns_summary(mocker):
    """
    Test full ingestion pipeline returns summary.
    """
    df = pd.DataFrame(
        {
            "symbol": ["AAPL", "MSFT", "GOOGL"],
            "date": ["2024-01-01", "2024-01-01", "2024-01-01"],
            "open": [150.0, 300.0, 2800.0],
            "high": [155.0, 305.0, 2850.0],
            "low": [149.0, 299.0, 2790.0],
            "close": [152.0, 303.0, 2820.0],
            "volume": [1000000, 2000000, 1500000],
            "adj_close": [152.0, 303.0, 2820.0],
        }
    )

    mocker.patch(
        "stock_prediction_ml.db.ingest.ingest_records_in_batches", return_value=3
    )
    summary = ingest_data_into_db(df)

    assert summary["total_rows"] == 3
    assert summary["total_inserted"] == 3
    assert summary["skipped"] == 0
    assert "date" in summary


def test_ingest_data_into_db_handles_duplicates(mocker):
    """
    Test ingestion skips duplicates.
    """
    df = pd.DataFrame(
        {
            "symbol": ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"],
            "date": ["2024-01-01"] * 5,
            "open": [150.0, 300.0, 2800.0, 3300.0, 900.0],
            "high": [155.0, 305.0, 2850.0, 3350.0, 950.0],
            "low": [149.0, 299.0, 2790.0, 3250.0, 850.0],
            "close": [152.0, 303.0, 2820.0, 3320.0, 920.0],
            "volume": [1000000] * 5,
            "adj_close": [152.0, 303.0, 2820.0, 3320.0, 920.0],
        }
    )

    mocker.patch(
        "stock_prediction_ml.db.ingest.ingest_records_in_batches", return_value=3
    )
    summary = ingest_data_into_db(df)

    assert summary["total_rows"] == 5
    assert summary["total_inserted"] == 3
    assert summary["skipped"] == 2
