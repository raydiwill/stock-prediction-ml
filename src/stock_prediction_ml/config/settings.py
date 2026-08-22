import os

from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
)


class Settings(BaseSettings):
    # Required

    database_url: str
    mlflow_tracking_uri: str
    marketstack_api_key: str
    api_host: str
    api_port: int

    # Optional
    # API
    api_log_level: str = "info"
    api_reload: bool = False

    # Serving model
    model_name: str = "catboost_model"
    default_experiment_name: str = "stock_prediction_experiment"

    # Model Registry settings
    registered_model_name: str = "stock_prediction_classifier"
    model_alias: str = "champion"

    # Feast settings
    feast_service_name: str = "stock_prediction_service"

    # Feature casting (SQLite returns int64, model expects int32)
    int_columns: list[str] = ["day_of_month", "day_of_week", "month"]

    # Supported stock symbols
    valid_symbols: list[str] = ["AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "TSLA"]

    # Storage configuration
    data_root: str = "data"
    s3_endpoint_url: str | None = None
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None

    cors_origins: list[str]

    model_config = SettingsConfigDict(
        env_file=os.getenv("ENV_FILE", "configs/config.env.dev"),
        env_file_encoding="utf-8",
        extra="ignore",  # don't crash if env has unrelated vars
    )


settings = Settings()
