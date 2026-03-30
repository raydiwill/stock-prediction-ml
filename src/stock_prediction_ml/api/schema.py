"""Pydantic schemas for Stock Prediction API request/response models."""

from datetime import UTC, datetime

import pandas as pd
from pydantic import BaseModel, Field, field_validator


class StockRequest(BaseModel):
    symbol: str = Field(..., examples=["AAPL"], description="Stock ticker symbol")
    date: str = Field(
        ..., examples=["2025-01-12"], description="Trading date in format YYYY-MM-DD"
    )

    @field_validator("symbol")
    @classmethod
    def validate_symbol_string(cls, sent_symbol):
        allowed_symbol = ["AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "TSLA"]
        if sent_symbol.upper() not in allowed_symbol:
            raise ValueError(
                f"Stock must be one of: {', '.join(sorted(allowed_symbol))}"
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
    symbol: str = Field(..., examples=["AAPL"])
    date: str = Field(..., examples=["2025-01-12"])
    prediction: int = Field(..., ge=0, le=1, description="0=down, 1=up")
    prediction_label: str = Field(
        ..., examples=["UP"], description="Human-readable prediction"
    )
    probability: float = Field(..., ge=0.0, le=1.0, description="Model confidence")
    predicted_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="UTC timestamp when prediction was made",
    )
    model_version: str = Field(..., examples=["1"])


class HealthResponse(BaseModel):
    status: str = Field(..., examples=["healthy"])
    model_loaded: bool
    feast_online_store: bool
    model_version: str | None = Field(None, examples=["1"])


class ModelInfoResponse(BaseModel):
    """Response schema for GET /model/info — champion model metadata and diagnostics."""

    model_version: str = Field(..., examples=["3"])
    model_alias: str = Field(..., examples=["champion"])
    run_id: str = Field(..., examples=["7d4dbfd5a9264229841cfdbc742f7f07"])
    metrics: dict[str, float] = Field(
        ...,
        examples=[{"test_accuracy": 0.68, "test_roc_auc": 0.72}],
    )

    # Base64-encoded PNG strings keyed by plot name
    # Keys: feature_importance, confusion_matrix, roc_curve
    diagnostics: dict[str, str] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Historical Data
# ---------------------------------------------------------------------------


class DailyRecord(BaseModel):
    """One trading day: close price, actual movement, and optional prediction."""

    date: str = Field(..., examples=["2025-01-12"])
    close: float = Field(..., description="Closing price")
    actual_direction: str | None = Field(
        None,
        examples=["UP"],
        description="Actual price movement vs previous day (UP/DOWN/None for first day)",
    )
    predicted_direction: str | None = Field(
        None, examples=["UP"], description="Model prediction if one exists for this day"
    )
    probability: float | None = Field(
        None, description="Model confidence if prediction exists"
    )
    correct: bool | None = Field(
        None, description="Whether prediction matched actual (None if either is missing)"
    )


class HistoricalDataResponse(BaseModel):
    """Response schema for GET /stock/history."""

    symbol: str
    start_date: str
    end_date: str
    total_records: int
    records: list[DailyRecord]
