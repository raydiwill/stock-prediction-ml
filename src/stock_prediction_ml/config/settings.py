import os

from pydantic import model_validator
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
    environment: str = "dev"

    # API
    api_log_level: str = "info"
    api_reload: bool = False

    # Serving model
    model_name: str = "catboost_model"
    default_experiment_name: str | None = None

    # Model Registry settings
    registered_model_name: str | None = None
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

    @model_validator(mode="after")
    def _apply_environment_defaults(self) -> "Settings":
        """Scope MLflow model/experiment names per environment unless overridden.

        `dev` keeps the unsuffixed names for backward compatibility; staging/prod
        get a suffix so a staging training run can never promote a model version
        under the same registered name the prod API loads.
        """
        suffix = "" if self.environment == "dev" else f"_{self.environment}"
        if self.registered_model_name is None:
            self.registered_model_name = f"stock_prediction_classifier{suffix}"
        if self.default_experiment_name is None:
            self.default_experiment_name = f"stock_prediction_experiment{suffix}"
        return self


settings = Settings()
