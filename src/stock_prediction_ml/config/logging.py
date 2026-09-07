import sys

from loguru import logger

__all__ = ["logger", "setup_logging"]


def setup_logging(level: str | None = None, json: bool | None = None) -> None:
    """Reset loguru's default sink and install project sinks.

    Args:
        level: Minimum level to emit (e.g. "DEBUG", "INFO", "WARNING"). 
            Defaults to settings.log_level.
        json: Emit JSON-formatted logs. Defaults to settings.log_json.
    """
    from stock_prediction_ml.config.settings import settings

    if level is None:
        level = settings.log_level
    if json is None:
        json = settings.log_json

    logger.remove()
    logger.add(
        sys.stderr,
        level=level.upper(),
        serialize=json,
    )
