from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class RawStockData(Base):
    __tablename__ = "RawStockData"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    symbol = Column(String(10), nullable=False, index=True)
    date = Column(DateTime, nullable=False, index=True)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)
    adj_close = Column(Float, nullable=False)
    source = Column(String(50), nullable=False)
    hash_input = Column(String(64), nullable=False, unique=True, index=True)
    pulled_at = Column(DateTime, nullable=False, index=True)
    parquet_path = Column(Text, nullable=False)
    validated = Column(Boolean, default=False)
    predictions = relationship("PredictionResult", back_populates="raw_data")


class PredictionResult(Base):
    __tablename__ = "PredictionResults"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    raw_stock_data_id = Column(
        Integer, ForeignKey("RawStockData.id"), nullable=False, index=True
    )
    predicted_at = Column(DateTime, nullable=False, index=True)
    model_name = Column(String(100), nullable=False, index=True)
    prediction = Column(Integer, nullable=False)
    probability = Column(Float, nullable=False)
    features_used = Column(JSON, nullable=False)
    raw_data = relationship("RawStockData", back_populates="predictions")


class ModelMetadata(Base):
    __tablename__ = "ModelMetadata"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    model_name = Column(String(100), nullable=False, unique=True, index=True)
    mlflow_run_id = Column(String(100), nullable=False, index=True)
    registry_version = Column(Integer, nullable=False, index=True)
    stage = Column(String(50), nullable=False, index=True)
    created_at = Column(DateTime, nullable=False, index=True)
    metrics = Column(JSON, nullable=False)
    parameters = Column(JSON, nullable=False)
    features = Column(JSON, nullable=False)
    notes = Column(Text, nullable=True)
    active = Column(Boolean, default=True)
