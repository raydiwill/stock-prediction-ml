"""Training DAG - Train model and auto-promote.

Pipeline:
    1. Train CatBoost model using Feast features, log to MLflow
    2. Auto-promote model version if it meets champion/challenger thresholds

Usage:
    $ airflow dags test training_dag 2024-01-01
"""

from datetime import datetime

from airflow.models import Variable
from airflow.sdk import Param, dag, task
from airflow.timetables.trigger import CronTriggerTimetable

PROJECT_ROOT = Variable.get("project_root", default_var="/app")

_ENV = {
    "PYTHONPATH": "{{ var.value.project_root }}/src",
    "PATH": "{{ var.value.project_root }}/.venv/bin:/usr/local/bin:/usr/bin:/bin",
}


@dag(
    dag_id="training_dag",
    # Weekly Sunday 09:00 UTC; switch to "0 9 1 * *" for monthly
    schedule=CronTriggerTimetable("0 9 * * 0", timezone="UTC"),
    start_date=datetime(2025, 2, 24),
    catchup=False,
    tags=["training", "mlflow"],
    params={
        "config_path": Param(
            default=f"configs/training/{Variable.get('environment', default_var='local')}.yaml",
            type="string",
        ),
        "start_date": Param(
            default="",
            type="string",
            description=(
                "Earliest training date (YYYY-MM-DD). "
                "Leave empty to default to 180 days before execution date."
            ),
        ),
    },
)
def training_dag():
    """Train CatBoost model and promote if thresholds are met."""

    @task.bash(env=_ENV)
    def train_model() -> str:
        return (
            f"cd {PROJECT_ROOT} && "
            "python -m stock_prediction_ml.model.train "
            "--config {{ params.config_path }} "
            "--start-date {{ params.start_date or macros.ds_add(ds, -180) }} "
            "--end-date {{ ds }}"
        )

    @task.bash(env=_ENV)
    def promote_model() -> str:
        return (
            f"cd {PROJECT_ROOT} && "
            "python -m stock_prediction_ml.model.promote "
            "--config {{ params.config_path }}"
        )

    train_model() >> promote_model()


training_dag()
