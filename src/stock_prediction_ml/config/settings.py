from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "sqlite:///./dev.db"  # default for local dev
    mlflow_tracking_uri: str = "file:./mlruns"
    model_name: str = "catboost_model"

    marketstack_api_key: str | None = None

    class Config:
        env_file = "config.env"


settings = Settings()
