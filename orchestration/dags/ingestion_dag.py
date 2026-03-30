"""Ingestion DAG - Fetch market data, validate, and ingest into DB.

Pipeline:
    1. **fetch_data**: Pull EOD prices from MarketStack API for the
       configured tickers over the current schedule interval
       (``ds`` → ``data_interval_end``).  Retries 3× with a 5-min backoff
       to handle transient API errors.
    2. **validate_data**: Run the Great Expectations suite against the
       combined parquet file to enforce schema and quality checks.
    3. **ingest_to_db**: Load validated rows into the SQLite
       ``RawStockData`` table via the DB ingestion module.

Schedule:
    Daily at 07:00 UTC (after US market close + settlement window).
    Catchup is disabled — only the latest interval is backfilled on
    first enable.

Configuration:
    Airflow Variable ``project_root`` must point to the repo checkout so
    that every task can ``cd`` into it and activate the project virtualenv.

Params:
    tickers (list[str]): Ticker symbols to ingest.  Defaults to the
        magnificent-seven basket.  Override via the Airflow UI trigger
        form or ``--conf '{"tickers": ["AAPL"]}'``.

Usage:
    $ airflow dags test ingestion_dag 2024-01-01
"""

from datetime import datetime, timedelta

from airflow.sdk import Param, dag, task
from airflow.timetables.trigger import CronTriggerTimetable

# Shared env for every @task.bash — ensures the project venv and src/
# are on PATH / PYTHONPATH so CLI entry-points resolve correctly.
_ENV = {
    "PYTHONPATH": "{{ var.value.project_root }}/src",
    "PATH": "{{ var.value.project_root }}/.venv/bin:/usr/local/bin:/usr/bin:/bin",
}


@dag(
    dag_id="ingestion_dag",
    schedule=CronTriggerTimetable("0 7 * * *", timezone="UTC"),
    start_date=datetime(2025, 2, 24),
    catchup=False,
    params={
        "tickers": Param(
            ["AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "TSLA"],
            type="array"
        )
    },
    tags=["ingestion", "data"],
)
def ingestion_dag():
    """Fetch, validate, and ingest stock market data.

    Task graph::

        fetch_data >> validate_data >> ingest_to_db
    """

    @task.bash(env=_ENV, retries=3, retry_delay=timedelta(minutes=5))
    def fetch_data() -> str:
        """Pull EOD prices from MarketStack for the schedule interval.

        Writes ``data/processed/combined_eod.parquet``.
        """
        return (
            "cd {{ var.value.project_root }} && "
            "python -m stock_prediction_ml.marketstack.pull "
            "--tickers {{ params.tickers | join(' ') }} "
            "--start_date {{ ds }} "
            "--end_date {{ data_interval_end | ds }}"
        )

    @task.bash(env=_ENV, retries=2)
    def validate_data() -> str:
        """Run Great Expectations checks on the combined parquet.

        Reads ``data/processed/combined_eod.parquet``, produces
        ``data/processed/validated_data.parquet`` on success.
        """
        return (
            "cd {{ var.value.project_root }} && "
            "python -m stock_prediction_ml.data_validation.validation "
            "--input data/processed/combined_eod.parquet"
        )

    @task.bash(env=_ENV, retries=2)
    def ingest_to_db() -> str:
        """Load validated parquet rows into the ``RawStockData`` table."""
        return (
            "cd {{ var.value.project_root }} && "
            "python -m stock_prediction_ml.db.ingest "
            "--path data/processed/validated_data.parquet"
        )

    fetch_data() >> validate_data() >> ingest_to_db()


ingestion_dag()
