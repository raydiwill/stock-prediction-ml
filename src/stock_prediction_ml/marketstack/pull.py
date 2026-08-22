import argparse
import logging
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

from stock_prediction_ml.config.settings import settings
from stock_prediction_ml.config.storage import (
    data_path,
    ensure_parent_dir,
    list_parquet,
    storage_options,
)

API_URL = "https://api.marketstack.com/v2"


# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def fetch_ticker_data(
    api_key: str,
    symbol: str,
    start_date: str,
    end_date: str,
    limit: int = 1000,
) -> list[dict]:
    """
    Fetch all EOD data for a single ticker, handling pagination automatically.

    Args:
        api_key (str): MarketStack API key.
        symbol (str): Stock ticker symbol (e.g., 'AAPL').
        start_date (str): Start date in 'YYYY-MM-DD' format.
        end_date (str): End date in 'YYYY-MM-DD' format.
        limit (int, optional): Number of records per API call. Defaults to 1000.

    Returns:
        list[dict]: A list of dictionaries containing the raw EOD data.

    Examples:
        >>> data = fetch_ticker_data("my_key", "AAPL", "2023-01-01", "2023-01-05")
        >>> print(len(data))
        4
    """
    api_url = f"{API_URL}/eod"
    all_data = []
    offset = 0
    rate_limit_retries = 0
    max_rate_limit_retries = 5

    logger.info(f"Fetching data for {symbol}...")

    while True:
        params = {
            "access_key": api_key,
            "symbols": symbol,
            "limit": limit,
            "offset": offset,
            "sort": "ASC",
            "date_from": start_date,
            "date_to": end_date,
        }

        try:
            response = requests.get(api_url, params=params)

            if response.status_code == 429:
                error_code = response.json().get("error", {}).get("code", "")
                if error_code == "usage_limit_reached":
                    raise RuntimeError(f"MarketStack monthly usage limit reached: {response.text}")

                rate_limit_retries += 1
                if rate_limit_retries > max_rate_limit_retries:
                    raise RuntimeError(
                        f"Rate limit exceeded {max_rate_limit_retries} retries "
                        f"for {symbol}; giving up."
                    )
                logger.warning(
                    f"Rate limit hit ({rate_limit_retries}/{max_rate_limit_retries}). "
                    "Waiting 1 second..."
                )
                time.sleep(1)
                continue

            rate_limit_retries = 0

            if response.status_code != 200:
                logger.error(f"API call failed: {response.status_code} - {response.text}")
                break

            data = response.json()
            batch_data = data.get("data", [])

            if not batch_data:
                logger.info(f"No more data found for {symbol}.")
                break

            all_data.extend(batch_data)

            pagination = data.get("pagination", {})
            total = pagination.get("total", 0)

            logger.info(f"Fetched {len(all_data)}/{total} records for {symbol}")

            if len(all_data) >= total:
                break

            offset += limit
            # small sleep to be nice to the API
            time.sleep(0.2)

        except RuntimeError:
            raise

        except Exception as e:
            logger.error(f"Error fetching {symbol}: {e}")
            break

    return all_data


def process_dataframe(stock_data: list[dict]) -> pd.DataFrame:
    """
    Process raw stock data into a pandas DataFrame.

    Converts date columns to datetime objects, adds metadata columns ('pulled_at', 'source'),
    and filters the DataFrame to keep only relevant columns.

    Args:
        stock_data (list[dict]): Raw stock data list returned from the API.

    Returns:
        pd.DataFrame: Processed DataFrame with standardized columns.

    Examples:
        >>> raw_data = [{"date": "2023-01-01", "symbol": "AAPL", "close": 150.0}]
        >>> df = process_dataframe(raw_data)
        >>> print(df.columns.tolist())
        ['date', 'symbol', 'close', 'pulled_at', 'source']
    """
    if not stock_data:
        logger.warning("No data to process.")
        return pd.DataFrame()

    df = pd.DataFrame(stock_data)

    # Ensure date is datetime
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])

    # Add metadata columns
    df["pulled_at"] = pd.Timestamp.now()
    df["source"] = "marketstack_api"

    cols = [
        "date",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "adj_close",
        "pulled_at",
        "source",
    ]

    # Only keep columns that actually exist in the response
    existing_cols = [c for c in cols if c in df.columns]
    df = df[existing_cols]

    if "date" in df.columns and "symbol" in df.columns:
        df = df.sort_values(["date", "symbol"]).reset_index(drop=True)

    return df


def save_to_parquet(df: pd.DataFrame, filename: str) -> None:
    """
    Save DataFrame to a Parquet file in the data/raw directory.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        filename (str): The name of the output file (e.g., 'AAPL_eod.parquet').

    Returns:
        None

    Examples:
        >>> df = pd.DataFrame({"col": [1, 2]})
        >>> save_to_parquet(df, "test_data.parquet")
        # Saves to PROJECT_ROOT/data/raw/test_data.parquet
    """
    if df.empty:
        logger.warning(f"Skipping save for {filename}: DataFrame is empty.")
        return

    # Output path to save parquet file
    output_path = data_path("raw", filename)

    ensure_parent_dir(output_path)
    df.to_parquet(output_path, index=False, storage_options=storage_options(output_path))
    logger.info(f"Saved {len(df)} rows to {output_path}")


def combine_and_save_to_parquet(path: str | None = None, files: list[str] | None = None) -> None:
    """
    Read parquet files, combine them, and save to processed folder.

    Args:
        path (str | None, optional): Input directory containing raw parquet files.
            Defaults to data_path("raw"). Ignored if `files` is provided.
        files (list[str] | None, optional): Explicit list of parquet files to
            combine. When provided, `path` is ignored and stale files left
            over from previous runs in the raw directory are not picked up.

    Returns:
        None

    Examples:
        >>> combine_and_save_to_parquet()
        # Reads all .parquet files in data/raw, combines them,
        # and saves 'combined_eod.parquet' in data/processed.
    """
    if files is not None:
        parquet_files = files
    else:
        # Determine Input Directory
        input_dir = path if path is not None else "raw"

        # Find all parquet files
        parquet_files = [f for f in list_parquet(input_dir) if "combined_eod" not in f]

    if not parquet_files:
        logger.warning("No .parquet files to combine.")
        return

    logger.info(f"Found {len(parquet_files)} files. Combining...")

    # Read and Combine
    dfs = []
    for file in parquet_files:
        try:
            df = pd.read_parquet(file, storage_options=storage_options(file))
            if not df.empty:
                dfs.append(df)
        except Exception as e:
            logger.error(f"Failed to read {file}: {e}")

    if not dfs:
        logger.warning("No valid dataframes to combine.")
        return

    combined_df = pd.concat(dfs, ignore_index=True)

    # Save to Processed
    output_path = data_path("processed", "combined_eod.parquet")
    ensure_parent_dir(output_path)
    combined_df.to_parquet(output_path, index=False, storage_options=storage_options(output_path))

    logger.info(f"Successfully combined {len(combined_df)} rows from {len(dfs)} files.")
    logger.info(f"Saved to {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Fetch EOD stock data from MarketStack API")
    parser.add_argument(
        "--tickers",
        nargs="+",
        default=["AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "TSLA"],
        help="List of stock symbols",
    )
    parser.add_argument("--start_date", default="2012-05-18", help="Start date in YYYY-MM-DD")
    parser.add_argument(
        "--end_date",
        default=f"{(datetime.now() - timedelta(1)).strftime('%Y-%m-%d')}",
        help="End date in YYYY-MM-DD",
    )

    args = parser.parse_args()

    try:
        api_key = settings.marketstack_api_key

        if not api_key:
            raise ValueError("MARKETSTACK_API_KEY not found in configuration.")

        fetched_files = []
        for symbol in args.tickers:
            # 1. Fetch all pages for this symbol
            raw_data = fetch_ticker_data(
                api_key=api_key,
                symbol=symbol,
                start_date=args.start_date,
                end_date=args.end_date,
            )

            # 2. Process
            df = process_dataframe(raw_data)

            if df.empty:
                logger.warning(f"No data returned for {symbol}; nothing to save.")
                continue

            # 3. Save individually
            filename = f"{symbol}_eod.parquet"
            save_to_parquet(df, filename)
            fetched_files.append(data_path("raw", filename))

        # 4. Combine only this run's downloaded files (not stale ones from
        # previous runs still sitting in the raw directory)
        combine_and_save_to_parquet(files=fetched_files)

    except Exception as error:
        logger.error(f"Critical error: {error}")
        raise


if __name__ == "__main__":
    main()
