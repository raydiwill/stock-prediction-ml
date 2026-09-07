from loguru import logger

from stock_prediction_ml.config.logging import setup_logging
from stock_prediction_ml.db.models import Base
from stock_prediction_ml.db.session import engine


def create_all_tables():
    Base.metadata.create_all(bind=engine)


if __name__ == "__main__":
    setup_logging()

    create_all_tables()
    logger.info("Database tables created successfully.")
