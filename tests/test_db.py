from datetime import datetime

import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

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

    # We should have these 3 tables
    assert "RawStockData" in table_names
    assert "PredictionResults" in table_names
    assert "ModelMetadata" in table_names


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


def test_get_db_yields_session():
    """Test that get_db() gives us a working session."""
    # Get database session
    db_generator = get_db()
    session = next(db_generator)

    # Should be able to use it
    assert session is not None

    # Cleanup
    try:
        next(db_generator)
    except StopIteration:
        pass
