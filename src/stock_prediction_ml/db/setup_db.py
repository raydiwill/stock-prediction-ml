import logging

from src.stock_prediction_ml.db.models import Base
from src.stock_prediction_ml.db.session import engine

# Configure enhanced logging with timestamps and better formatting
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def create_all_tables():
    Base.metadata.create_all(bind=engine)


if __name__ == "__main__":
    create_all_tables()
    logger.info("Database tables created successfully.")
