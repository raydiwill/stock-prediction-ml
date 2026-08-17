"""Daily Prediction DAG - Batch predict and persist to DB.

Pipeline:
    1. **materialize_features**: Incrementally materialize Feast features
       to the online store so the batch predictor reads fresh values.
    2. **batch_predict**: Load the champion model from MLflow, pull online
       features for every ticker, run inference, and write PredictionResult
       rows back to the DB.

Schedule:
    Daily at 08:00 UTC — runs after ingestion_dag (07:00) has landed fresh
    data in the DB.

Params:
    tickers (list[str]): Symbols to predict.  Same default basket as
        ingestion_dag.

Usage:
    $ airflow dags test prediction_dag 2024-01-01

TODO:
    - Add ExternalTaskSensor to wait for ingestion_dag success instead
      of relying on cron offset alone
    - Swap @task.bash for @task.docker once containerised
"""

from datetime import datetime

from airflow.sdk import Param, dag, task
from airflow.timetables.trigger import CronTriggerTimetable

_ENV = {
    "PYTHONPATH": "{{ var.value.project_root }}/src",
    "PATH": "{{ var.value.project_root }}/.venv/bin:/usr/local/bin:/usr/bin:/bin",
}


@dag(
    dag_id="prediction_dag",
    schedule=CronTriggerTimetable("0 8 * * *", timezone="UTC"),
    start_date=datetime(2025, 2, 24),
    catchup=False,
    params={
        "tickers": Param(
            ["AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "TSLA"],
            type="array",
        ),
    },
    tags=["prediction", "daily"],
)
def prediction_dag():
    """Materialize features, run batch predictions, persist to DB.

    Task graph::

        materialize_features >> batch_predict
    """

    @task.bash(env=_ENV, append_env=True)
    def materialize_features() -> str:
        """Push latest feature rows into the Feast online store."""
        return (
            "cd {{ var.value.project_root }}/src/stock_prediction_ml/feast_repo && "
            "feast materialize-incremental $(date +%Y-%m-%dT%H:%M:%S)"
        )

    @task.bash(env=_ENV, append_env=True, retries=2)
    def batch_predict() -> str:
        """Run champion model against every ticker and save PredictionResult rows."""
        return (
            "cd {{ var.value.project_root }} && "
            "python -m stock_prediction_ml.model.batch_predict "
            "--tickers {{ params.tickers | join(' ') }} "
            "--date {{ ds }}"
        )

    materialize_features() >> batch_predict()


prediction_dag()
