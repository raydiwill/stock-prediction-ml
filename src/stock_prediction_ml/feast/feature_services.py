from feast import FeatureService

# Import the feature view objects using relative import
# This won't cause duplicates because Python caches module imports
from .features_definition import (
    stock_basic_features,
    stock_technical_features,
    stock_timeseries_features,
)


stock_prediction_service = FeatureService(
    name="stock_prediction_service",
    features=[
        stock_basic_features,
        stock_technical_features,
        stock_timeseries_features,
    ],
    tags={"model": "catboost_v1"},
)
