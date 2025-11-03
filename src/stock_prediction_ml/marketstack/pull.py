import argparse
import logging
import os
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

API_URL = "https://api.marketstack.com/v2"

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_config() -> str:
    """Load configuration from .env file and return the API key.

    :Returns:
        str: MarketStack API key

    :Examples:
    >>> load_config()
    "your_api_key_here"
    """
    env_path = Path(__file__).parent.parent.parent.parent / "config.env"
    load_dotenv(env_path)

    api_key = os.getenv("MARKETSTACK_API_KEY")

    return api_key


def fetch_eod_with_date(
    api_key: str,
    tickers: list[str],
    start_date: str,
    end_date: str,
    limit: int = 1000,
    offset: int = 0,
) -> list[dict]:
    """Fetch EOD stock data from MarketStack API within a date range.
    :Args:
        api_key (str): MarketStack API key
        tickers (list[str]): List of stock symbols
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        limit (int): Number of results per page
        offset (int): Pagination offset
    :Returns:
        list[dict]: List of EOD stock data records

    :Examples:
    >>> fetch_eod_with_date(
    ...     api_key="your_api_key_here",
    ...     tickers=["AAPL", "MSFT"],
    ...     start_date="2025-01-01",
    ...     end_date="2025-01-10",
    ...     limit=1000,
    ...     offset=0,
    ... )
    [  # Example output
        {"date": "2025-01-02",
         "symbol": "AAPL",
         "open": 150.0,
         "high": 155.0,
         "low": 149.0,
         "close": 154.0,
         "volume": 1000000,
         "adj_close": 154.0,
        },
    ]
    """
    api_url = f"{API_URL}/eod"  # EOD endpoint

    # Define your parameters for the API request
    params = {
        "access_key": api_key,
        "symbols": tickers,  # Stock symbols
        "limit": limit,  # Number of results per page
        "offset": offset,  # Pagination offset (Number of results to skip)
        "sort": "ASC",  # Sort order
        "date_from": start_date,  # Start date
        "date_to": end_date,  # End date
    }

    # Make the API call
    response = requests.get(api_url, params=params)

    if response.status_code != 200:
        logger.error(f"API call failed: {response.status_code} - {response.text}")
        raise Exception(f"API request failed: {response.status_code}")

    data = response.json()
    logger.info(f"Successfully fetched {len(data.get('data', []))} records")
    return data["data"]


def process_dataframe(stock_data: list[dict]) -> pd.DataFrame:
    """Process raw stock data into a pandas DataFrame.

    :Args:
        stock_data (list[dict]): Raw stock data from API

    :Returns:
        pd.DataFrame: Processed DataFrame with selected columns

    :Examples:
    >>> stock_data = [
    ...     {"date": "2025-01-02",
    ...      "symbol": "AAPL",
    ...      "open": 150.0,
    ...      "high": 155.0,
    ...      "low": 149.0,
    ...      "close": 154.0,
    ...      "volume": 1000000,
    ...      "adj_close": 154.0,
    ...     },
    ... ]
    >>> df = process_dataframe(stock_data)
    >>> df.head()
            date symbol   open   high    low  close   volume  adj_close
    0 2025-01-02   AAPL  150.0  155.0  149.0  154.0  1000000      154.0
    """
    df = pd.DataFrame(stock_data)
    df["date"] = pd.to_datetime(df["date"])
    df = df[["date", "symbol", "open", "high", "low", "close", "volume", "adj_close"]]
    df = df.sort_values(["date", "symbol"]).reset_index(drop=True)

    return df


def save_to_parquet(df: pd.DataFrame, filename: str) -> None:
    """Save DataFrame to a Parquet file.

    :Args:
        df (pd.DataFrame): DataFrame to save
        filename (str): Output Parquet filename

    :Returns:
        None

    :Examples:
    >>> df = pd.DataFrame({
    ...     "date": ["2025-01-02"],
    ...     "symbol": ["AAPL"],
    ...     "open": [150.0],
    ...     "high": [155.0],
    ...     "low": [149.0],
    ...     "close": [154.0],
    ...     "volume": [1000000],
    ...     "adj_close": [154.0],
    ... })
    >>> save_to_parquet(df, "eod_data.parquet")
    Saved to data/raw/eod_data.parquet
    """
    output_path = Path("data/raw") / filename
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(output_path, index=False)
    logger.info(f"Saved to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Fetch EOD stock data from MarketStack API"
    )
    parser.add_argument(
        "--tickers",
        nargs="+",
        default=["AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "TSLA"],
        help="List of stock symbols",
    )
    parser.add_argument(
        "--start_date", default="2025-01-01", help="Start date in YYYY-MM-DD"
    )
    parser.add_argument(
        "--end_date", default="2025-01-10", help="End date in YYYY-MM-DD"
    )
    parser.add_argument(
        "--output", default="eod_data.parquet", help="Output Parquet filename"
    )
    parser.add_argument(
        "--limit", type=int, default=1000, help="Number of results per page"
    )
    parser.add_argument("--offset", type=int, default=0, help="Pagination offset")

    args = parser.parse_args()

    try:
        api_key = load_config()
        stock_data = fetch_eod_with_date(
            api_key=api_key,
            tickers=args.tickers,
            start_date=args.start_date,
            end_date=args.end_date,
            limit=args.limit,
            offset=args.offset,
        )
        df = process_dataframe(stock_data)
        save_to_parquet(df, args.output)

    except Exception as error:
        logger.error(f"Error occurred: {error}")
        raise


if __name__ == "__main__":
    main()
