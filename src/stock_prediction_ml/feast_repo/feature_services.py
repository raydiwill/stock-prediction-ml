from feast import FeatureService

from stock_prediction_ml.feast_repo.features_definition import (
    stock_basic_features,
    stock_target_label,
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


stock_training_service = FeatureService(
    name="stock_training_service",
    features=[
        stock_basic_features,
        stock_technical_features,
        stock_timeseries_features,
        stock_target_label
    ],
    tags={"model": "catboost_v1"},
)