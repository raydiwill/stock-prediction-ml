from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from stock_prediction_ml.db.export import (
    EXPORT_COLUMNS,
    export_validated_data,
    save_to_parquet,
)
from stock_prediction_ml.db.models import Base, PredictionResult, RawStockData
from stock_prediction_ml.db.session import get_db


@pytest.fixture
def test_db():
    """
    Create a temporary in-memory database for testing.
    """
    # Create a fresh database in memory (not saved to disk)
    engine = create_engine("sqlite:///:memory:")

    # Create all tables
    Base.metadata.create_all(bind=engine)

    # Create a session to interact with database
    TestSession = sessionmaker(bind=engine)
    session = TestSession()

    yield session

    # Cleanup after test
    session.close()


@pytest.fixture
def sample_stock():
    """Create a sample stock data record for testing."""
    return RawStockData(
        symbol="AAPL",
        date=datetime(2024, 1, 1, 9, 30),
        open=150.0,
        high=152.0,
        low=149.0,
        close=151.0,
        volume=1000000.0,
        adj_close=151.0,
        source="marketstack",
        hash_input="test123",
        pulled_at=datetime(2024, 1, 1, 18, 0),
        parquet_path="/data/test.parquet",
        validated=False,
    )


# ==== Test ====


def test_tables_exist():
    """Test that our database tables are created."""
    # Create engine
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)

    # Check what tables exist
    inspector = inspect(engine)
    table_names = inspector.get_table_names()

    # We should have these 2 tables
    assert "RawStockData" in table_names
    assert "PredictionResults" in table_names


def test_able_to_insert_stock_data_into_db(test_db, sample_stock):
    """Test that we can save stock data to database."""
    # Save to database
    test_db.add(sample_stock)
    test_db.commit()

    # Check it was saved (ID should be assigned)
    assert sample_stock.id is not None
    assert sample_stock.id > 0


def test_stock_data_can_be_queried(test_db, sample_stock):
    """Test that we can retrieve stock data from database."""
    # Save to database
    test_db.add(sample_stock)
    test_db.commit()

    # Query newly inserted
    found = test_db.query(RawStockData).filter_by(symbol="AAPL").first()

    # Check we got the right data back
    assert found is not None
    assert found.symbol == "AAPL"
    assert found.close == 151.0


def test_update_stock_data_with_new_value(test_db, sample_stock):
    """Test that we can update existing data."""
    # Save to database
    test_db.add(sample_stock)
    test_db.commit()

    # Update the close price
    sample_stock.close = 155.0
    sample_stock.validated = True
    test_db.commit()

    # Check it was updated
    found = test_db.query(RawStockData).filter_by(symbol="AAPL").first()
    assert found.close == 155.0
    assert found.validated is True


def test_able_to_delete_stock_data_from_db(test_db, sample_stock):
    """Test that we can delete data."""
    # Save to database
    test_db.add(sample_stock)
    test_db.commit()
    stock_id = sample_stock.id

    # Delete it
    test_db.delete(sample_stock)
    test_db.commit()

    # Check it's gone
    found = test_db.query(RawStockData).filter_by(id=stock_id).first()
    assert found is None


def test_relationship_between_tables(test_db, sample_stock):
    """Test that RawStockData and PredictionResult are linked."""
    # Save to database
    test_db.add(sample_stock)
    test_db.commit()

    # Create a prediction linked to this stock data
    prediction = PredictionResult(
        raw_stock_data_id=sample_stock.id,
        predicted_at=datetime(2024, 1, 2, 10, 0),
        model_name="catboost_v1",
        prediction=1,
        probability=0.85,
        features_used={"return": 0.5},
    )

    test_db.add(prediction)
    test_db.commit()

    # Check we can access prediction from stock data
    test_db.refresh(sample_stock)
    assert len(sample_stock.predictions) == 1
    assert sample_stock.predictions[0].model_name == "catboost_v1"


def test_duplicate_hash_not_allowed(test_db, sample_stock):
    """Test that we can't insert two records with same hash_input."""
    # Save to database
    test_db.add(sample_stock)
    test_db.commit()

    # Try to save another with same hash_input
    duplicate = RawStockData(
        symbol="MSFT",
        date=datetime(2024, 1, 2),
        open=200.0,
        high=202.0,
        low=199.0,
        close=201.0,
        volume=500000.0,
        adj_close=201.0,
        source="marketstack",
        hash_input="test123",
        pulled_at=datetime.now(),
        parquet_path="/data/test2.parquet",
    )

    test_db.add(duplicate)

    # Should fail because hash_input must be unique
    with pytest.raises(IntegrityError):
        test_db.commit()


def test_get_db_yields_session(mocker):
    """Test that get_db() gives us a working session."""
    patched_session = MagicMock()
    mocker.patch(
        "stock_prediction_ml.db.session.SessionLocal", return_value=patched_session
    )

    db_generator = get_db()
    session = next(db_generator)

    assert session is patched_session

    # Cleanup
    try:
        next(db_generator)
    except StopIteration:
        pass

    patched_session.close.assert_called_once()


# ==== Export Tests ====


@pytest.fixture
def seeded_db(test_db):
    """Seed the test DB with a mix of validated/unvalidated rows across dates."""
    rows = [
        RawStockData(
            symbol="AAPL",
            date=datetime(2024, 1, 5),
            open=150.0,
            high=152.0,
            low=149.0,
            close=151.0,
            volume=1_000_000.0,
            adj_close=151.0,
            source="test",
            hash_input="hash_1",
            pulled_at=datetime.now(),
            parquet_path="/tmp/test.parquet",
            validated=True,
        ),
        RawStockData(
            symbol="AAPL",
            date=datetime(2024, 2, 10),
            open=155.0,
            high=157.0,
            low=154.0,
            close=156.0,
            volume=1_200_000.0,
            adj_close=156.0,
            source="test",
            hash_input="hash_2",
            pulled_at=datetime.now(),
            parquet_path="/tmp/test.parquet",
            validated=True,
        ),
        RawStockData(
            symbol="MSFT",
            date=datetime(2024, 1, 15),
            open=300.0,
            high=305.0,
            low=298.0,
            close=303.0,
            volume=800_000.0,
            adj_close=303.0,
            source="test",
            hash_input="hash_3",
            pulled_at=datetime.now(),
            parquet_path="/tmp/test.parquet",
            validated=True,
        ),
        RawStockData(
            symbol="MSFT",
            date=datetime(2024, 3, 1),
            open=310.0,
            high=315.0,
            low=308.0,
            close=312.0,
            volume=900_000.0,
            adj_close=312.0,
            source="test",
            hash_input="hash_4",
            pulled_at=datetime.now(),
            parquet_path="/tmp/test.parquet",
            validated=False,
        ),
        RawStockData(
            symbol="AAPL",
            date=datetime(2024, 4, 20),
            open=160.0,
            high=162.0,
            low=159.0,
            close=161.0,
            volume=1_100_000.0,
            adj_close=161.0,
            source="test",
            hash_input="hash_5",
            pulled_at=datetime.now(),
            parquet_path="/tmp/test.parquet",
            validated=False,
        ),
    ]
    test_db.add_all(rows)
    test_db.commit()
    return test_db


@pytest.fixture
def patched_session(mocker, seeded_db):
    mock = MagicMock()

    mock.__enter__.return_value = seeded_db
    mock.__exit__.return_value = False
    mocker.patch("stock_prediction_ml.db.export.SessionLocal", return_value=mock)

    return seeded_db


# ---- export_validated_data ----


def test_export_returns_only_validated_rows(patched_session):
    """Only rows with validated=True should appear in the result."""
    df = export_validated_data("2024-01-01", "2024-12-31")

    assert len(df) == 3  # only the 3 rows where validated=True
    assert all(df["symbol"].isin(["AAPL", "MSFT"]))


def test_export_filters_by_date_range(patched_session):
    """Rows outside the requested [start_date, end_date] should be excluded."""
    df = export_validated_data("2024-01-15", "2024-02-10")

    assert len(df) == 2
    assert set(df["symbol"]) == {"AAPL", "MSFT"}
    assert df["date"].min() >= pd.Timestamp("2024-01-15")
    assert df["date"].max() <= pd.Timestamp("2024-02-10")


def test_export_returns_expected_columns(patched_session):
    """The returned DataFrame should have exactly the EXPORT_COLUMNS."""
    df = export_validated_data("2024-01-15", "2024-02-10")

    columns = df.columns.tolist()
    assert columns == EXPORT_COLUMNS


def test_export_returns_empty_df_when_no_matches(patched_session):
    """When no validated rows exist in the date range, result should be empty."""
    df = export_validated_data("2024-03-01", "2024-04-01")

    assert len(df) == 0
    assert df.columns.tolist() == EXPORT_COLUMNS


# ---- save_to_parquet ----


@pytest.fixture
def sample_df():
    """A small DataFrame that matches EXPORT_COLUMNS for parquet tests."""
    return pd.DataFrame(
        {
            "date": [datetime(2024, 1, 1), datetime(2024, 1, 2)],
            "symbol": ["AAPL", "AAPL"],
            "open": [150.0, 151.0],
            "high": [152.0, 153.0],
            "low": [149.0, 150.0],
            "close": [151.0, 152.0],
            "volume": [1_000_000.0, 1_100_000.0],
            "adj_close": [151.0, 152.0],
        }
    )


def test_save_to_parquet_default_path(mocker, sample_df, tmp_path):
    """When output_path is None, file lands under data_root/processed/."""
    mocker.patch(
        "stock_prediction_ml.config.storage.settings.data_root", str(tmp_path)
    )
    save_to_parquet(sample_df, "2024-01-01", "2024-01-02")

    file_path = tmp_path / "processed" / "history_2024-01-01_2024-01-02.parquet"

    assert file_path.exists()

    df = pd.read_parquet(file_path)
    pd.testing.assert_frame_equal(sample_df, df)


def test_save_to_parquet_custom_path(sample_df, tmp_path):
    """When an explicit output_path is given, use it directly."""
    file_path = Path(tmp_path / "custom_file.parquet")
    save_to_parquet(sample_df, "2024-01-01", "2024-01-02", file_path)

    df = pd.read_parquet(file_path)
    pd.testing.assert_frame_equal(sample_df, df)


def test_save_to_parquet_adds_suffix_if_missing(sample_df, tmp_path):
    """If the given path doesn't end in .parquet, the function appends it."""
    file_path = Path(tmp_path / "custom_file.csv")
    save_to_parquet(sample_df, "2024-01-01", "2024-01-02", file_path)

    expected_path = file_path.with_suffix(".parquet")

    assert expected_path.exists()

    df = pd.read_parquet(expected_path)
    pd.testing.assert_frame_equal(sample_df, df)
