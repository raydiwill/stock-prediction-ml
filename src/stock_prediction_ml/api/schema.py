from datetime import datetime

import pandas as pd
from pydantic import BaseModel, Field, field_validator


class StockRequest(BaseModel):
    symbol: str = Field(..., example="AAPL", description="Stock ticker symbol")
    date: str = Field(
        ..., example="2025-01-12", description="Trading date in format YYYY-MM-DD"
    )

    @field_validator("symbol")
    @classmethod
    def validate_symbol_string(cls, sent_symbol):
        allowed_symbol = ["AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "TSLA"]
        if sent_symbol.upper() not in allowed_symbol:
            raise ValueError(
                f"Stock must be one of: {", ".join(sorted(allowed_symbol))}"
            )
        else:
            return sent_symbol.upper()

    @field_validator("date")
    @classmethod
    def validate_date_string(cls, sent_date):
        try:
            parsed_date = pd.to_datetime(sent_date)
        except (ValueError, TypeError):
            raise ValueError("Invalid format! Must be in YYYY-MM-DD.")

        if parsed_date.weekday() >= 5:
            raise ValueError(f"Date {sent_date} is a weekend (markets closed)")

        return sent_date


class PredictionResponse(BaseModel):
    symbol: str = Field(..., example="AAPL")
    date: str = Field(..., example="2025-01-12")
    prediction: int = Field(..., ge=0, le=1, description="0=down, 1=up")
    prediction_label: str = Field(
        ..., example="UP", description="Human-readable prediction"
    )
    probability: float = Field(..., ge=0.0, le=1.0, description="Model confidence")
    predicted_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="UTC timestamp when prediction was made",
    )
    model_version: str = Field(..., example="1")


class HealthResponse(BaseModel):
    status: str = Field(..., example="healthy")
    model_loaded: bool
    feast_online_store: bool
    model_version: str | None = Field(None, example="1")
