import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


PROJECT_ROOT = Path(__file__).resolve().parents[3]


def read_raw_data(
    folder_path: str | Path | None = None,
    column_to_keep: list[str] = [
        "date",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "adj_close",
    ],
) -> pd.DataFrame:
    """
    Load and combine Parquet files into a single DataFrame.

    Parameters
    ----------
    folder_path : str | Path | None, optional
        Directory containing the Parquet files.
        If None, the default project data/raw folder is used.
    column_to_keep : list[str], optional
        Columns to retain in the final DataFrame.

    Returns
    -------
    pd.DataFrame
        Combined DataFrame with only the selected columns.
    """
    if folder_path is None:
        folder = PROJECT_ROOT / "data" / "raw"
    else:
        folder = Path(folder_path)

    logger.info(f"Reading raw data from folder: {folder}")
    files = list(folder.glob("*.parquet"))

    if not files:
        logger.error(f"No Parquet files found in {folder}")
        raise ValueError(f"No Parquet files found in {folder}")

    logger.info(f"Found {len(files)} Parquet file(s)")
    logger.info(f"Files to read: {[f.name for f in files]}")

    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    logger.info(f"Combined data shape: {df.shape}")

    df["date"] = pd.to_datetime(df["date"], utc=True)
    logger.info(
        f"Date column converted to datetime. Date range: {df['date'].min()} to {df['date'].max()}"
    )
    logger.info(f"Filtered to {len(column_to_keep)} columns: {column_to_keep}")

    return df[column_to_keep]


def save_combined_data(df: pd.DataFrame, file_path: str | Path | None = None) -> None:
    """Save combined raw data to a Parquet file.

    Args:
        df (pd.DataFrame): Combined DataFrame to save.
        file_path (str | Path | None): Path to the file.
            If None, the default project data/processed folder is used.
    """
    if file_path is None:
        file_path = PROJECT_ROOT / "data" / "processed" / "combined_eod.parquet"
    else:
        file_path = Path(file_path)

    logger.info(f"Saving combined data to: {file_path}")
    file_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(file_path, index=False)
    logger.info(f"Successfully saved {len(df)} rows to {file_path}")


def read_combined_data(file_path: str | Path | None = None) -> pd.DataFrame:
    """Read combined raw data from a Parquet file.

    Args:
        file_path (str | Path | None): Path to the Parquet file.

    Returns:
        pd.DataFrame: DataFrame read from the Parquet file.
    """
    if file_path is None:
        file_path = PROJECT_ROOT / "data" / "processed" / "combined_eod.parquet"
    else:
        file_path = Path(file_path)

    logger.info(f"Reading combined data from: {file_path}")
    df = pd.read_parquet(file_path)
    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    logger.info(f"Columns: {df.columns.tolist()}")

    return df


def create_target(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create target varible indicating whetther
    the next day's closing price is higher than the current day's.

    Args:
        df (pd.DataFrame): Input DataFram

    Returns:
        pd.DataFrame: DataFrame with added 'target' column
                        (1 if next close > current close, else 0)

    Examples:
        >>> df = pd.DataFrame({
        ...     'symbol': ['AAPL', 'AAPL'],
        ...     'date': pd.to_datetime(['2020-01-01', '2020-01-02']),
        ...     'close': [100, 105]
        ... })
        >>> result = create_target(df)
        >>> result['target'].tolist()
        [1]
    """
    logger.info("Creating target variable")
    initial_len = len(df)

    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)
    df["target"] = (df.groupby("symbol")["close"].shift(-1) > df["close"]).astype(int)
    df = df.dropna()

    target_dist = df["target"].value_counts()
    logger.info(f"Target created. Class distribution: {target_dist.to_dict()}")
    logger.info(f"Rows dropped (NaN): {initial_len - len(df)}")

    return df


def create_range_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates range and return features based on price differences.

    Args:
        df (pd.DataFrame): Input DataFrame

    Returns:
        pd.DataFrame: DataFrame with added features.

    Examples:
        >>> df = pd.DataFrame({
        ...     'high': [110, 115],
        ...     'low': [100, 105],
        ...     'close': [105, 110],
        ...     'open': [100, 105]
        ... })
        >>> result = create_range_features(df)
        >>> result[['high_low', 'close_open']].iloc[0].tolist()
        [10, 5]
    """
    logger.info("Creating range features: high_low, close_open, return")
    df["high_low"] = df["high"] - df["low"]
    df["close_open"] = df["close"] - df["open"]
    df["return"] = df["close"].pct_change()
    logger.info("Range features created successfully")
    return df


def create_lag_features(df: pd.DataFrame, lag_days: list[int]) -> pd.DataFrame:
    """
    Creates lagged return features for specified days.

    Args:
        df (pd.DataFrame): Input DataFrame with return.
        lag_days (List[int]): List of lag days (e.g., [1, 2, 5]).

    Returns:
        pd.DataFrame: DataFrame with added lagged return columns.

    Examples:
        >>> df = pd.DataFrame({'return': [0.01, 0.02, 0.03]})
        >>> result = create_lag_features(df, [1])
        >>> result['return_lag_1'].tolist()
        [nan, 0.01, 0.02]
    """
    logger.info(f"Creating lag features for days: {lag_days}")
    for day in lag_days:
        df[f"return_lag_{day}"] = df["return"].shift(day)
    logger.info(f"Created {len(lag_days)} lag features")
    return df


def create_rolling_features(
    df: pd.DataFrame, rolling_windows: list[int]
) -> pd.DataFrame:
    """
    Creates rolling mean and standard deviation features for returns.

    Args:
        df (pd.DataFrame): Input DataFrame with return.
        rolling_windows (List[int]): List of rolling window sizes (e.g., [5, 10]).

    Returns:
        pd.DataFrame: DataFrame with added rolling mean and std columns.

    Examples:
        >>> df = pd.DataFrame({'return': [0.01, 0.02, 0.03, 0.04, 0.05]})
        >>> result = create_rolling_features(df, [3])
        >>> result['return_roll_mean_3'].iloc[2]
        0.02
    """
    logger.info(f"Creating rolling features for windows: {rolling_windows}")
    for window in rolling_windows:
        df[f"return_roll_mean_{window}"] = df["return"].rolling(window).mean()
        df[f"return_roll_std_{window}"] = df["return"].rolling(window).std()
    logger.info(f"Created rolling features for {len(rolling_windows)} windows")
    return df


def create_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates time-based features from the date column.

    Args:
        df (pd.DataFrame): Input DataFrame with date (datetime).

    Returns:
        pd.DataFrame: DataFrame with added time features.

    Examples:
        >>> df = pd.DataFrame({'date': pd.to_datetime(['2020-01-01'])})
        >>> result = create_time_features(df)
        >>> result[['day_of_week', 'month']].iloc[0].tolist()
        [2, 1]
    """
    logger.info("Creating time-based features")
    df["date"] = pd.to_datetime(df["date"])
    df["day_of_week"] = df["date"].dt.weekday
    df["month"] = df["date"].dt.month
    df["day_of_month"] = df["date"].dt.day
    df["quarter"] = df["date"].dt.quarter
    df["is_quarter_end"] = df["date"].dt.is_quarter_end.astype(int)
    logger.info(
        "Time features created: day_of_week, month, day_of_month, quarter, is_quarter_end"
    )
    return df


def create_sma_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates Simple Moving Average (SMA) features.

    Args:
        df (pd.DataFrame): Input DataFrame with close.

    Returns:
        pd.DataFrame: DataFrame with added sma columns.

    Examples:
        >>> df = pd.DataFrame({'close': [List of close prices]})
        >>> result = create_sma_features(df)
        >>> result['sma_10'].iloc[9]
        104.5
    """
    logger.info("Creating SMA features: sma_10, sma_20")
    df["sma_10"] = df["close"].rolling(window=10, min_periods=10).mean()
    df["sma_20"] = df["close"].rolling(window=20, min_periods=20).mean()
    logger.info("SMA features created successfully")
    return df


def create_rsi_features(df: pd.DataFrame, window: int = 14) -> pd.DataFrame:
    """
    Creates Relative Strength Index (RSI) feature.

    Args:
        df (pd.DataFrame): Input DataFrame with close.
        window (int, optional): Window size for RSI calculation. Defaults to 14.

    Returns:
        pd.DataFrame: DataFrame with added rsi column.

    Examples:
        >>> df = pd.DataFrame({'close': [List of close prices]})
        >>> result = create_rsi_features(df, window=14)
        >>> result['rsi_14'].iloc[13]  # Approximate value
        100.0
    """
    logger.info(f"Creating RSI feature with window={window}")
    delta = df["close"].diff()

    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)

    avg_gain = pd.Series(gain).rolling(window=window, min_periods=window).mean()
    avg_loss = pd.Series(loss).rolling(window=window, min_periods=window).mean()

    rs = avg_gain / (avg_loss.replace(0, np.nan))
    df["rsi_14"] = 100 - (100 / (1 + rs))
    logger.info(f"RSI feature created with window={window}")
    return df


def create_ema_macd_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates Exponential Moving Average (EMA) and MACD features.

    Args:
        df (pd.DataFrame): Input DataFrame with close.

    Returns:
        pd.DataFrame: DataFrame with added ema and macdcolumns.

    Examples:
        >>> df = pd.DataFrame({'close': [List of close prices]})
        >>> result = create_ema_macd_features(df)
        >>> result[['ema_12', 'macd']].iloc[25]  # Approximate values
        (ema_12=..., macd=...)
    """
    logger.info("Creating EMA and MACD features")
    df["ema_12"] = df["close"].ewm(span=12, adjust=False).mean()
    df["ema_26"] = df["close"].ewm(span=26, adjust=False).mean()

    df["macd"] = df["ema_12"] - df["ema_26"]
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
    logger.info("EMA and MACD features created: ema_12, ema_26, macd, macd_signal")
    return df


def create_features(
    df: pd.DataFrame,
    lag_days: list[int] = [1, 2, 5, 10],
    rolling_windows: list[int] = [5, 10],
) -> pd.DataFrame:
    """
    Applies all feature creation functions to the DataFrame and handles NaNs.

    Args:
        df (pd.DataFrame): Input DataFrame with required columns
            ('symbol', 'date', 'open', 'high', 'low', 'close').
        lag_days (List[int], optional): List of lag days for lagged features.
            Defaults to [1, 2, 5, 10].
        rolling_windows (List[int], optional): List of rolling window sizes.
            Defaults to [5, 10].

    Returns:
        pd.DataFrame: DataFrame with all features added,
                        initial rows lagged dropped,
                        and NaNs removed.

    Examples:
        >>> df = pd.DataFrame({stock data here})
        >>> result = create_features(df)
        >>> result.shape[0] < 50  # Should be less due to drops
        True
    """
    logger.info("Starting feature creation pipeline")
    initial_shape = df.shape
    logger.info(f"Initial DataFrame shape: {initial_shape}")

    df = create_target(df)
    df = create_range_features(df)
    df = create_lag_features(df, lag_days)
    df = create_rolling_features(df, rolling_windows)
    df = create_time_features(df)
    df = create_sma_features(df)
    df = create_rsi_features(df)
    df = create_ema_macd_features(df)

    # drop the initial warm-up rows for rolling/lag features
    W_DROP = 20
    logger.info(f"Dropping first {W_DROP} rows for warm-up period")
    df = df.iloc[W_DROP:]

    # drop any leftover NaNs
    before_dropna = len(df)
    df = df.dropna().reset_index(drop=True)
    dropped_nan = before_dropna - len(df)

    if dropped_nan > 0:
        logger.info(f"Dropped {dropped_nan} rows with NaN values")

    final_shape = df.shape
    logger.info(
        f"Feature creation complete. Final shape: {final_shape} (from {initial_shape})"
    )
    logger.info(f"Total features created: {final_shape[1] - initial_shape[1]}")

    return df


def save_feature_data(
    df: pd.DataFrame,
    file_name: str | None = None,
) -> None:
    """Save feature DataFrame to a Parquet file.

    :Args:
        df (pd.DataFrame): Feature DataFrame to save
        filename (str): Output Parquet filename

    :Examples:
    >>> df = pd.DataFrame({
    ...     new_features_created_here
    ... })
    >>> save_feature_data(df, "feature_data.parquet")
    Saved to data/processed/feature_data.parquet
    """
    file_path = PROJECT_ROOT / "data" / "feature"
    if file_name is None:
        out_path = file_path / "stock_eod_features.parquet"
    else:
        if not file_name.endswith(".parquet"):
            file_name += ".parquet"
        out_path = file_path / file_name

    output_path = Path(out_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Saving feature data to: {output_path}")
    df.to_parquet(output_path, index=False)
    logger.info(f"Successfully saved {len(df)} rows to {output_path}")
    logger.debug(f"File size: {output_path.stat().st_size / 1024:.2f} KB")


def main():
    parser = argparse.ArgumentParser(
        description="Build features for stock prediction from raw EOD data."
    )
    parser.add_argument(
        "--input_folder",
        type=str,
        default=None,
        help="Input folder containing raw Parquet files.",
    )
    parser.add_argument(
        "--output_filename",
        type=str,
        default="stock_eod_features.parquet",
        help="Output filename for the feature Parquet file.",
    )
    args = parser.parse_args()
    logger.info("=== Feature Engineering Pipeline Started ===")

    try:
        df = read_raw_data(args.input_folder)
        logger.info("Raw data read successfully.")

        save_combined_data(df)
        logger.info("Combined raw data saved successfully.")

        df = read_combined_data()
        df_features = create_features(df)

        save_feature_data(df_features, args.output_filename)
        logger.info("Feature data saved successfully.")

        logger.info("=== Feature Engineering Pipeline Completed Successfully ===")

    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
