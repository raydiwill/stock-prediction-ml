"""Export validated stock data from the DB to parquet for feature engineering.

Usage:
    python -m stock_prediction_ml.db.export --output data/processed/full_history.parquet
    python -m stock_prediction_ml.db.export --start_date 2024-01-01
    python -m stock_prediction_ml.db.export --start_date 2024-01-01 --end_date 2025-12-31
"""

import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import select

from stock_prediction_ml.config.storage import data_path, ensure_parent_dir, storage_options
from stock_prediction_ml.db.models import RawStockData
from stock_prediction_ml.db.session import SessionLocal

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# These are the columns build_features.py expects from its input parquet
EXPORT_COLUMNS = [
    "date",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "adj_close",
]


def export_validated_data(
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """Query validated rows from RawStockData within an optional date range.

    Args:
        start_date: Inclusive lower bound (YYYY-MM-DD). None = no lower bound.
        end_date: Inclusive upper bound (YYYY-MM-DD). None = yesterday.

    Hints:
        - Parse date strings to datetime for SQLAlchemy comparison
        - Chain additional .where() clauses onto the existing query
        - RawStockData.date is a DateTime column — use >= and <= for inclusive range
        - Log which date range is being used so Airflow logs are informative

    Returns:
        pd.DataFrame: Validated stock data with EXPORT_COLUMNS.
    """
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    with SessionLocal() as db:
        query = (
            select(RawStockData)
            .where(RawStockData.validated.is_(True))
            .where(RawStockData.date >= start_dt)
            .where(RawStockData.date <= end_dt)
        )
        results = db.execute(query).scalars().all()

    rows = [
        {column: getattr(row, column) for column in EXPORT_COLUMNS} for row in results
    ]

    df = pd.DataFrame.from_dict(rows) if rows else pd.DataFrame(columns=EXPORT_COLUMNS)
    df = df.sort_values(by=["symbol", "date"]).reset_index(drop=True)

    logger.info(f"Date exported: from {start_date} until {end_date}")
    logger.info(f"Row count: {len(df)}")
    logger.info(f"Symbol count: {df['symbol'].nunique()}")

    return df


def save_to_parquet(
    df: pd.DataFrame,
    start_date: str,
    end_date: str,
    output_path: str | Path | None = None,
) -> None:
    """Write DataFrame to parquet, creating parent dirs if needed.

    Hint: same pattern as save_feature_data in build_features.py
    """
    if output_path is None:
        file_name = f"history_{start_date}_{end_date}.parquet"
        output_path = data_path("processed", file_name)
    else:
        output_path = Path(output_path)
        if output_path.suffix != ".parquet":
            output_path = output_path.with_suffix(".parquet")
        output_path = str(output_path)

    ensure_parent_dir(output_path)
    logger.info(f"Saving exported data from DB to: {output_path}")
    df.to_parquet(output_path, index=False, storage_options=storage_options())
    logger.info(f"Successfully saved {len(df)} rows to {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export raw data from DB with date range."
    )
    parser.add_argument("--start_date", type=str, default=None)
    parser.add_argument("--end_date", type=str, default=None)
    parser.add_argument("--output", type=Path, default=None)

    args = parser.parse_args()

    # Resolve defaults once, keep everything as strings
    start_date = args.start_date or (datetime.today() - timedelta(days=180)).strftime(
        "%Y-%m-%d"
    )
    end_date = args.end_date or (datetime.today() - timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )

    logger.info(f"Exporting data from {start_date} to {end_date}")

    try:
        df = export_validated_data(start_date, end_date)

        if df.empty:
            logger.warning("No data found for the given date range. Skipping export.")
            return

        output = args.output
        save_to_parquet(df, start_date, end_date, output)
        logger.info("Export completed successfully.")

    except Exception as e:
        logger.error(str(e))
        raise


if __name__ == "__main__":
    main()
