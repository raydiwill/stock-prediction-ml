import os
from pathlib import Path

from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ENV_FILE = PROJECT_ROOT / "configs" / "config.env.local"


class Settings(BaseSettings):
    # Default settings for local def

    database_url: str = "sqlite:///./dev.db"

    mlflow_tracking_uri: str = "file:./mlruns"

    marketstack_api_key: str | None = None

    api_host: str = "127.0.0.1"
    api_port: int = 8000
    api_log_level: str = "debug"
    api_reload: bool = True

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

    cors_origins: str = "*"

    @property
    def split_cors_origins_to_list(self) -> list[str]:
        """Conver cors_origin string to list for FastAPI"""
        return [origin.strip() for origin in self.cors_origins.split(",")]

    model_config = SettingsConfigDict(
        env_file=os.getenv("ENV_FILE", str(DEFAULT_ENV_FILE)), env_file_encoding="utf_8"
    )


settings = Settings()
