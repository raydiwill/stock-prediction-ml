import os
from pathlib import Path
from typing import List
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

    cors_origins: str = "*"

    @property
    def split_cors_origins_to_list(self) -> List[str]:
        """Conver cors_origin string to list for FastAPI"""
        return [origin.strip() for origin in self.cors_origins.split(",")]

    model_config = SettingsConfigDict(
        env_file=os.getenv("ENV_FILE", str(DEFAULT_ENV_FILE)), env_file_encoding="utf_8"
    )


settings = Settings()
