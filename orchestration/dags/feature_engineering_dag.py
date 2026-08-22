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


@dag(
    dag_id="feature_engineering_dag",
    schedule=None,
    start_date=datetime(2025, 2, 24),
    catchup=False,
    tags=["features", "feast"],
)
def feature_engineering_dag():
    """Build features from validated data and materialize to Feast."""

    @task.bash(env=_ENV, append_env=True)
    def export_from_db() -> str:
        """Exports to db.export's own data_path() default and prints the
        resolved path as the last stdout line, which Airflow auto-pushes to
        XCom (BashOperator's default do_xcom_push=True)."""
        return (
            "cd {{ var.value.project_root }} && "
            "python -m stock_prediction_ml.db.export "
            f"--start_date {_START_DATE} "
            f"--end_date {_END_DATE}"
        )

    @task.bash(env=_ENV, append_env=True)
    def build_features() -> str:
        history_file = "{{ ti.xcom_pull(task_ids='export_from_db') }}"
        return (
            "cd {{ var.value.project_root }} && "
            "python -m stock_prediction_ml.features.build_features "
            f"--input_file {history_file}"
        )

    @task.bash(env=_ENV, append_env=True)
    def feast_apply() -> str:
        return "cd {{ var.value.project_root }}/src/stock_prediction_ml/feast_repo && feast apply"

    @task.bash(env=_ENV, append_env=True)
    def feast_materialize() -> str:
        return (
            "cd {{ var.value.project_root }}/src/stock_prediction_ml/feast_repo && "
            "feast materialize-incremental $(date +%Y-%m-%dT%H:%M:%S)"
        )

    export_from_db() >> build_features() >> feast_apply() >> feast_materialize()


feature_engineering_dag()
