from datetime import timedelta
from pathlib import Path

from feast import FileSource, FeatureView, Field
from feast.types import Float32, Int32

from stock_prediction_ml.feast.entities import stock


PROJECT_ROOT = Path(__file__).resolve().parents[3]


stock_features_source = FileSource(
    path=str(PROJECT_ROOT / "data" / "feature" / "stock_eod_features.parquet"),
    timestamp_field="date",
)


stock_basic_features = FeatureView(
    name="stock_basic_features",
    entities=[stock],
    ttl=timedelta(days=1),
    schema=[
        Field(name="open", dtype=Float32),
        Field(name="high", dtype=Float32),
        Field(name="low", dtype=Float32),
        Field(name="close", dtype=Float32),
        Field(name="adj_close", dtype=Float32),
        Field(name="volume", dtype=Float32),
    ],
    source=stock_features_source,
    online=True,
    tags={"team": "ML"},
)


stock_technical_features = FeatureView(
    name="stock_technical_features",
    entities=[stock],
    ttl=timedelta(days=1),
    schema=[
        # Price-based features
        Field(name="high_low", dtype=Float32),
        Field(name="close_open", dtype=Float32),
        # Return features
        Field(name="return", dtype=Float32),
        Field(name="return_lag_1", dtype=Float32),
        Field(name="return_lag_2", dtype=Float32),
        Field(name="return_lag_5", dtype=Float32),
        Field(name="return_lag_10", dtype=Float32),
        # Rolling statistics
        Field(name="return_roll_mean_5", dtype=Float32),
        Field(name="return_roll_std_5", dtype=Float32),
        Field(name="return_roll_mean_10", dtype=Float32),
        Field(name="return_roll_std_10", dtype=Float32),
        # Technical indicators
        Field(name="sma_10", dtype=Float32),
        Field(name="sma_20", dtype=Float32),
        Field(name="rsi_14", dtype=Float32),
        Field(name="ema_12", dtype=Float32),
        Field(name="ema_26", dtype=Float32),
        Field(name="macd", dtype=Float32),
        Field(name="macd_signal", dtype=Float32),
    ],
    source=stock_features_source,
    online=True,
    tags={"team": "ML"},
)


stock_timeseries_features = FeatureView(
    name="stock_timeseries_features",
    entities=[stock],
    ttl=timedelta(days=1),
    schema=[
        Field(name="day_of_week", dtype=Int32),
        Field(name="month", dtype=Int32),
        Field(name="day_of_month", dtype=Int32),
        Field(name="quarter", dtype=Int32),
        Field(name="is_quarter_end", dtype=Int32),
    ],
    source=stock_features_source,
    online=True,
    tags={"team": "ML"},
)


stock_target_label = FeatureView(
    name="stock_target_label",
    entities=[stock],
    ttl=timedelta(days=1),
    schema=[
        Field(name="target", dtype=Int32)
    ],
    source=stock_features_source,
    online=False,
    tags={"team": "ML"},
)