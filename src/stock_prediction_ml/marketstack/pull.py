import os
import logging

import pandas as pd
import requests
from pathlib import Path
from dotenv import load_dotenv
from typing import List

from stock_prediction_ml.marketstack import API_URL

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_config() -> str:
    env_path = Path(__file__).parent.parent.parent.parent / "config.env"
    load_dotenv(env_path)

    api_key = os.getenv("MARKETSTACK_API_KEY")

    return api_key


def fetch_eod_with_date(
    api_key: str,
    tickers: List[str],
    start_date: str,
    end_date: str,
    limit: int = 1000,
    offset: int = 0,
) -> List[dict]:
    api_url = f"{API_URL}/eod"
    # Define your parameters
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


def process_dataframe(stock_data: List[dict]) -> pd.DataFrame:
    df = pd.DataFrame(stock_data)
    df['date'] = pd.to_datetime(df['date'])
    df = df[['date', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'adj_close']]
    df = df.sort_values(['date', 'symbol']).reset_index(drop=True)

    return df


def save_to_parquet(df: pd.DataFrame, filename: str) -> None:
    output_path = Path("data/raw") / filename
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(output_path, index=False)
    logger.info(f'Saved to {output_path}')


def main():
    pass


if __name__ == "__main__":
    main()
