"""Feature Engineering DAG - Build features and apply to Feast.

Pipeline:
    1. Export full history from DB to parquet (feature engineering needs rolling windows)
    2. Build technical/temporal features from full history
    3. Apply Feast feature definitions (registers feature views)
    4. Materialize features incrementally to online store

Usage:
    $ airflow dags test feature_engineering_dag 2024-01-01

TODO:
    - Swap @task.bash for @task.docker once Dockerfile is ready
    - Add sensor to wait for ingestion_dag completion (or use TriggerDagRunOperator)
    - Parameterize input/output paths via Airflow Variables
"""

from datetime import datetime

from airflow.sdk import dag, task

_ENV = {
    "PYTHONPATH": "{{ var.value.project_root }}/src",
    "PATH": "{{ var.value.project_root }}/.venv/bin:/usr/local/bin:/usr/bin:/bin",
}
_START_DATE = "{{ macros.ds_add(ds, -180) }}"
_END_DATE = "{{ ds }}"
_HISTORY_FILE = f"data/processed/history_{_START_DATE}_{_END_DATE}.parquet"


@dag(
    dag_id="feature_engineering_dag",
    schedule=None,
    start_date=datetime(2025, 2, 24),
    catchup=False,
    tags=["features", "feast"],
)
def feature_engineering_dag():
    """Build features from validated data and materialize to Feast."""

    @task.bash(env=_ENV)
    def export_from_db() -> str:
        return (
            "cd {{ var.value.project_root }} && "
            "python -m stock_prediction_ml.db.export "
            f"--start_date {_START_DATE} "
            f"--end_date {_END_DATE} "
            f"--output {_HISTORY_FILE}"
        )

    @task.bash(env=_ENV)
    def build_features() -> str:
        return (
            "cd {{ var.value.project_root }} && "
            "python -m stock_prediction_ml.features.build_features "
            f"--input_file {_HISTORY_FILE}"  
        )

    @task.bash(env=_ENV)
    def feast_apply() -> str:
        return "cd {{ var.value.project_root }}/src/stock_prediction_ml/feast_repo && feast apply"

    @task.bash(env=_ENV)
    def feast_materialize() -> str:
        return (
            "cd {{ var.value.project_root }}/src/stock_prediction_ml/feast_repo && "
            "feast materialize-incremental $(date +%Y-%m-%dT%H:%M:%S)"
        )

    export_from_db() >> build_features() >> feast_apply() >> feast_materialize()


feature_engineering_dag()
