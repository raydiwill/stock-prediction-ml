import argparse
import hashlib
import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy.exc import IntegrityError

from stock_prediction_ml.db import models
from stock_prediction_ml.db.session import get_db

TARGET_TABLE = models.RawStockData
PROJECT_ROOT = Path(__file__).parent.parent.parent
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def read_validated_file(path: str | Path | None = None) -> pd.DataFrame:
    """
    Read a validated parquet file into a pandas DataFrame.

    Args:
        path: Path to the validated parquet file. If None, uses default location
              at PROJECT_ROOT/data/processed/validated_data.parquet.

    Returns:
        pd.DataFrame: DataFrame containing validated stock data with columns
                      like symbol, date, open, high, low, close, volume, adj_close.

    Examples:
        >>> # Read from default location
        >>> df = read_validated_file()
        >>> print(df.shape)
        (1000, 8)

        >>> # Read from custom path
        >>> df = read_validated_file("data/custom/stocks.parquet")
        >>> print(df.columns)
        Index(['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'adj_close'],
    dtype='object')
    """
    if path is None:
        path = PROJECT_ROOT / "data" / "processed" / "validated_data.parquet"

    path = Path(path)
    df = pd.read_parquet(path)

    return df


def dataframe_to_rows(df: pd.DataFrame) -> list[dict]:
    """
    Convert a validated DataFrame to a list of dicts matching the model fields.

    Adds metadata fields (validated, parquet_path) required by the RawStockData model.

    Args:
        df: DataFrame with stock data columns (symbol, date, open, high, low,
            close, volume, adj_close).

    Returns:
        list[dict]: List of dictionaries where each dict represents one row
                    with keys matching RawStockData model fields.

    Examples:
        >>> df = pd.DataFrame({
        ...     'symbol': ['AAPL', 'MSFT'],
        ...     'date': ['2024-01-01', '2024-01-01'],
        ...     'open': [150.0, 300.0],
        ...     'high': [155.0, 305.0],
        ...     'low': [149.0, 299.0],
        ...     'close': [152.0, 303.0],
        ...     'volume': [1000000, 2000000],
        ...     'adj_close': [152.0, 303.0]
        ... })
        >>> rows = dataframe_to_rows(df)
        >>> print(len(rows))
        2
        >>> print(rows[0]['validated'])
        True
        >>> print('parquet_path' in rows[0])
        True
    """
    df["validated"] = True
    df["parquet_path"] = str(
        PROJECT_ROOT / "data" / "processed" / "validated_data.parquet"
    )

    return df.to_dict(orient="records")


def normalize_rows(row: dict) -> dict:
    """
    Normalize a single row dict by standardizing date format and symbol casing.

    Args:
        row: Dictionary representing one stock data record with at minimum
             'date' and 'symbol' keys.

    Returns:
        dict: Normalized row with date converted to pandas Timestamp and
              symbol uppercased.

    Examples:
        >>> row = {
        ...     'symbol': 'aapl',
        ...     'date': '2024-01-01',
        ...     'open': 150.0,
        ...     'close': 152.0
        ... }
        >>> normalized = normalize_rows(row)
        >>> print(normalized['symbol'])
        AAPL
        >>> print(type(normalized['date']))
        <class 'pandas._libs.tslibs.timestamps.Timestamp'>
        >>> print(normalized['date'])
        2024-01-01 00:00:00
    """
    normalized = row.copy()
    normalized["date"] = pd.to_datetime(row["date"])
    normalized["symbol"] = str(row["symbol"]).upper()

    return normalized


def input_hashing(row: dict) -> str:
    """
    Generate a deterministic SHA256 hash for a stock data row.

    Creates a hash based on key financial fields (symbol, date, prices, volume)
    to enable deduplication and data versioning. The hash is deterministic:
    identical data always produces the same hash.

    Args:
        row: Dictionary with stock data. Must contain keys: symbol, date, open,
             high, low, close, volume, adj_close.

    Returns:
        str: 64-character hexadecimal SHA256 hash string.

    Examples:
        >>> row1 = {
        ...     'symbol': 'AAPL',
        ...     'date': '2024-01-01',
        ...     'open': 150.0,
        ...     'high': 155.0,
        ...     'low': 149.0,
        ...     'close': 152.0,
        ...     'volume': 1000000,
        ...     'adj_close': 152.0
        ... }
        >>> hash1 = input_hashing(row1)
        >>> print(len(hash1))
        64
        >>> print(hash1[:8])
        'a3f2b1c4'

        >>> # Same data produces same hash
        >>> row2 = row1.copy()
        >>> hash2 = input_hashing(row2)
        >>> print(hash1 == hash2)
        True

        >>> # Different data produces different hash
        >>> row2['close'] = 153.0
        >>> hash3 = input_hashing(row2)
        >>> print(hash1 == hash3)
        False
    """
    key_fields = [
        "symbol",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "adj_close",
    ]
    hash_data = {k: str(row[k]) for k in key_fields if k in row}

    # Sort and serialize
    canonical = json.dumps(hash_data, sort_keys=True)

    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def map_to_db_dict(normalized_row: dict) -> dict:
    """
    Map a normalized row to database column keys for RawStockData model.

    Converts row values to appropriate types (str, float, datetime) and
    adds computed fields like input_hash. Ensures all required model fields
    are present with correct types.

    Args:
        normalized_row: Dictionary with normalized stock data (symbol uppercased,
                       date as Timestamp). Must include: symbol, date, open, high,
                       low, close, volume, adj_close, pulled_at.

    Returns:
        dict: Dictionary with keys matching RawStockData ORM model columns,
              ready for ORM instantiation.

    Examples:
        >>> normalized = {
        ...     'symbol': 'AAPL',
        ...     'date': pd.Timestamp('2024-01-01'),
        ...     'open': 150.0,
        ...     'high': 155.0,
        ...     'low': 149.0,
        ...     'close': 152.0,
        ...     'volume': 1000000,
        ...     'adj_close': 152.0,
        ...     'pulled_at': pd.Timestamp('2024-12-07'),
        ...     'parquet_path': '/path/to/file.parquet',
        ...     'validated': True
        ... }
        >>> mapped = map_to_db_dict(normalized)
        >>> print(type(mapped['open']))
        <class 'float'>
        >>> print(type(mapped['symbol']))
        <class 'str'>
        >>> print('hash_input' in mapped)
        True
        >>> print(len(mapped['hash_input']))
        64
    """
    mapping = {
        "symbol": str(normalized_row["symbol"]),
        "date": normalized_row["date"],
        "open": float(normalized_row["open"]),
        "high": float(normalized_row["high"]),
        "low": float(normalized_row["low"]),
        "close": float(normalized_row["close"]),
        "volume": float(normalized_row["volume"]),
        "adj_close": float(normalized_row["adj_close"]),
        "source": normalized_row.get("source", "marketstack_api"),
        "hash_input": input_hashing(normalized_row),
        "pulled_at": normalized_row["pulled_at"],
        "parquet_path": normalized_row.get("parquet_path"),
        "validated": normalized_row.get("validated", True),
    }

    return mapping


def map_records_to_db(records):
    """
    Convert a list of normalized dicts to RawStockData ORM objects.

    First maps each record to database column schema, then instantiates
    ORM objects ready for bulk insertion.

    Args:
        records: List of normalized record dictionaries with required fields.

    Returns:
        list: List of RawStockData SQLAlchemy ORM instances.

    Examples:
        >>> records = [
        ...     {
        ...         'symbol': 'AAPL',
        ...         'date': pd.Timestamp('2024-01-01'),
        ...         'open': 150.0,
        ...         'high': 155.0,
        ...         'low': 149.0,
        ...         'close': 152.0,
        ...         'volume': 1000000,
        ...         'adj_close': 152.0,
        ...         'pulled_at': pd.Timestamp.now(),
        ...         'parquet_path': 'data.parquet',
        ...         'validated': True
        ...     },
        ...     {
        ...         'symbol': 'MSFT',
        ...         'date': pd.Timestamp('2024-01-01'),
        ...         'open': 300.0,
        ...         'high': 305.0,
        ...         'low': 299.0,
        ...         'close': 303.0,
        ...         'volume': 2000000,
        ...         'adj_close': 303.0,
        ...         'pulled_at': pd.Timestamp.now(),
        ...         'parquet_path': 'data.parquet',
        ...         'validated': True
        ...     }
        ... ]
        >>> orm_objects = map_records_to_db(records)
        >>> print(len(orm_objects))
        2
        >>> print(type(orm_objects[0]))
        <class 'stock_prediction_ml.db.models.RawStockData'>
        >>> print(orm_objects[0].symbol)
        AAPL
    """
    mapped = [map_to_db_dict(record) for record in records]
    return [TARGET_TABLE(**map) for map in mapped]


def ingest_records_in_batches(orm_objects, batch_size=None):
    """
    Insert ORM objects with adaptive batching for scalability.

    Automatically selects optimal batch size based on dataset size:
    - <10K rows: single transaction
    - 10K-1M rows: 5K row batches
    - >1M rows: 10K row batches

    Handles IntegrityErrors by falling back to row-by-row insertion to skip
    duplicates while saving valid records.

    Args:
        orm_objects: List of RawStockData ORM instances ready for insertion.
        batch_size: Optional explicit batch size. If None, auto-detects based
                   on total row count.

    Returns:
        int: Total number of successfully inserted rows (may be less than
             input if duplicates were skipped).

    Raises:
        SQLAlchemyError: For database errors other than IntegrityError.

    Examples:
        >>> # Small dataset (single transaction)
        >>> orm_objects = [RawStockData(...) for _ in range(1000)]
        >>> inserted = ingest_records_in_batches(orm_objects)
        >>> print(inserted)
        1000

        >>> # Medium dataset (batched)
        >>> orm_objects = [RawStockData(...) for _ in range(50000)]
        >>> inserted = ingest_records_in_batches(orm_objects)
        # Logs: "Ingesting 50000 rows with batch_size=5000"
        # Logs: "Batch 1: inserted 5000 rows (total: 5000/50000)"
        # ...
        >>> print(inserted)
        50000

        >>> # With duplicates (some skipped)
        >>> orm_objects = [RawStockData(...) for _ in range(100)]
        >>> # Assume 10 are duplicates
        >>> inserted = ingest_records_in_batches(orm_objects)
        >>> print(inserted)
        90
    """
    total_rows = len(orm_objects)

    # Adaptive batch size
    if batch_size is None:
        if total_rows < 10_000:
            batch_size = total_rows  # Single transaction for small datasets
        elif total_rows < 1_000_000:
            batch_size = 5_000  # Medium batches
        else:
            batch_size = 10_000  # Larger batches for huge datasets

    logger.info(f"Ingesting {total_rows} rows with batch_size={batch_size}")

    db_gen = get_db()
    db = next(db_gen)
    total_inserted = 0

    try:
        for i in range(0, total_rows, batch_size):
            batch = orm_objects[i : i + batch_size]
            batch_num = i // batch_size + 1

            try:
                db.bulk_save_objects(batch)
                db.commit()
                total_inserted += len(batch)

                if total_rows > 10_000:  # Only log progress for large datasets
                    logger.info(
                        f"Batch {batch_num}: inserted {len(batch)} rows \
                            (total: {total_inserted}/{total_rows})"
                    )

            except IntegrityError as e:
                db.rollback()
                logger.warning(
                    f"Batch {batch_num} failed: {e}. Attempting row-by-row insert..."
                )

                # Fallback: insert one-by-one to skip duplicates
                for obj in batch:
                    try:
                        db.add(obj)
                        db.commit()
                        total_inserted += 1
                    except IntegrityError:
                        db.rollback()  # Skip duplicate
                        continue

        logger.info(f"✅ Successfully inserted {total_inserted}/{total_rows} rows")

    finally:
        db_gen.close()

    return total_inserted


def ingest_data_into_db(df):
    """
    Orchestrate the full ingestion pipeline from DataFrame to database.

    Pipeline steps:
    1. Convert DataFrame to row dictionaries
    2. Normalize each row (uppercase symbols, parse dates)
    3. Map to database schema (add hashes, metadata)
    4. Bulk insert with adaptive batching
    5. Generate summary report

    Args:
        df: pandas DataFrame with validated stock data. Must contain columns:
            symbol, date, open, high, low, close, volume, adj_close.

    Returns:
        dict: Summary with keys:
            - date: ingestion date (YYYY-MM-DD)
            - total_rows: number of input rows
            - total_inserted: number successfully inserted
            - skipped: number skipped (duplicates)

    Examples:
        >>> df = pd.DataFrame({
        ...     'symbol': ['AAPL', 'MSFT', 'AAPL'],  # One duplicate
        ...     'date': ['2024-01-01', '2024-01-01', '2024-01-01'],
        ...     'open': [150.0, 300.0, 150.0],
        ...     'high': [155.0, 305.0, 155.0],
        ...     'low': [149.0, 299.0, 149.0],
        ...     'close': [152.0, 303.0, 152.0],
        ...     'volume': [1000000, 2000000, 1000000],
        ...     'adj_close': [152.0, 303.0, 152.0],
        ...     'pulled_at': [pd.Timestamp.now()] * 3,
        ...     'source': ['marketstack_api'] * 3
        ... })
        >>> summary = ingest_data_into_db(df)
        >>> print(summary)
        {
            'date': '2024-12-07',
            'total_rows': 3,
            'total_inserted': 2,
            'skipped': 1
        }

        >>> # All unique records
        >>> df = pd.DataFrame({...})  # 1000 unique rows
        >>> summary = ingest_data_into_db(df)
        >>> print(summary['total_inserted'])
        1000
        >>> print(summary['skipped'])
        0
    """
    # 1. Convert to dicts
    rows = dataframe_to_rows(df)
    total_rows = len(rows)

    # 2. Normalize
    normalized_records = [normalize_rows(row) for row in rows]

    # 3. Map to ORM objects
    orm_objects = map_records_to_db(normalized_records)

    # 4. Insert into DB
    inserted_row = ingest_records_in_batches(orm_objects)

    # 5. Output summary
    summary = {
        "date": datetime.today().strftime("%Y-%m-%d"),
        "total_rows": total_rows,
        "total_inserted": inserted_row,
        "skipped": total_rows - inserted_row,
    }

    return summary


def main(path: str | Path | None = None, dry_run: bool = False) -> None:
    logger.info("Starting data ingestion pipeline...")

    df = read_validated_file(path)
    logger.info(f"Loaded {len(df)} rows from validated file")

    if dry_run:
        logger.info("🔍 DRY RUN MODE - No data will be inserted")
        rows = dataframe_to_rows(df)
        logger.info(f"Would insert {len(rows)} rows")
        logger.info(f"Sample row: {rows[0]}")
        return

    logger.info("Begin inserting into DB...")
    summary = ingest_data_into_db(df)
    logger.info(f"✅ Ingestion complete: {summary}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ingest validated stock data into the database."
    )
    parser.add_argument(
        "--path",
        type=str,
        default=None,
        help="Path to the validated Parquet file.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be inserted without writing to DB",
    )
    args = parser.parse_args()
    main(args.path, dry_run=args.dry_run)
