"""Shared fixtures for Airflow DAG tests.

Provides DagBag loading with mocked Airflow Variables so DAG files
can be imported without a running Airflow metadata database.

Why mock Variable.get?
    feature_engineering_dag.py and training_dag.py call Variable.get()
    at **module level** (import time). Without the mock, DagBag will
    try to hit the metadata DB and crash.

Why initialize the DB here?
    render_templates() internally queries the dag_run table. CI runs
    `airflow db migrate` before pytest; locally that step is often skipped.
    Forcing an in-memory DB + migration makes the suite self-contained.
"""

import os
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pytest
from airflow.models import DagBag, DagRun, TaskInstance
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

DAGS_DIR = str(Path(__file__).parent.parent / "dags")


@pytest.fixture(scope="session", autouse=True)
def _airflow_db():
    """Initialize an in-memory Airflow metadata DB for the test session.

    This mirrors the `airflow db migrate` step in CI and ensures tests are
    self-contained regardless of local Airflow state.
    """
    os.environ.setdefault(
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "sqlite:///:memory:"
    )
    from airflow.utils.db import initdb  # noqa: PLC0415

    initdb()


# Add new Variables here as your DAGs evolve.
_VARIABLE_DEFAULTS: dict[str, str] = {
    "project_root": "/app",
}


def _mock_variable_get(key: str, default_var: str | None = None, **kwargs) -> str:
    """Stand-in for airflow.models.Variable.get during tests.

    Accepts **kwargs because render-time calls pass deserialize_json=...
    """
    return _VARIABLE_DEFAULTS.get(key, default_var or "")


@pytest.fixture(scope="session")
def dag_bag() -> DagBag:
    """Load all DAGs from airflow/dags/ with Variables mocked.

    Session-scoped because DAG parsing is expensive and all tests
    are read-only against the bag.
    """
    with patch("airflow.models.Variable.get", side_effect=_mock_variable_get):
        bag = DagBag(dag_folder=DAGS_DIR, include_examples=False)
    return bag


# ---------------------------------------------------------------------------
# Per-DAG convenience fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def ingestion_dag(dag_bag: DagBag):
    """Return the parsed ingestion_dag."""
    ingestion_dag = dag_bag.dags.get("ingestion_dag")
    assert ingestion_dag is not None

    return ingestion_dag


@pytest.fixture(scope="session")
def feature_engineering_dag(dag_bag: DagBag):
    """Return the parsed feature_engineering_dag."""
    feature_engineering_dag = dag_bag.dags.get("feature_engineering_dag")
    assert feature_engineering_dag is not None

    return feature_engineering_dag


@pytest.fixture(scope="session")
def training_dag(dag_bag: DagBag):
    """Return the parsed training_dag."""
    training_dag = dag_bag.dags.get("training_dag")
    assert training_dag is not None

    return training_dag


@pytest.fixture(scope="session")
def prediction_dag(dag_bag: DagBag):
    """Return the parsed prediction_dag."""
    prediction_dag = dag_bag.dags.get("prediction_dag")
    assert prediction_dag is not None

    return prediction_dag


# ---------------------------------------------------------------------------
# Rendered-task fixtures (template rendering needs DagRun + TaskInstance)
# ---------------------------------------------------------------------------

_LOGICAL_DATE = datetime(2025, 7, 1, tzinfo=UTC)


def _render_dag_tasks(dag) -> dict[str, object]:
    """Render all tasks in a DAG and return {task_id: rendered_task}.

    Uses deepcopy so rendering doesn't mutate the session-scoped DAG object.
    """
    dag = deepcopy(dag)
    dr = DagRun(
        dag_id=dag.dag_id,
        run_id="test_render",
        run_type=DagRunType.MANUAL,
        logical_date=_LOGICAL_DATE,
        run_after=_LOGICAL_DATE,
        start_date=_LOGICAL_DATE,
        state=DagRunState.RUNNING,
    )
    rendered = {}
    with patch("airflow.models.Variable.get", side_effect=_mock_variable_get):
        for task in dag.tasks:
            ti = TaskInstance(task=task, run_id=dr.run_id, dag_version_id=None)
            ti.dag_run = dr
            ti.render_templates()
            rendered[task.task_id] = task
    return rendered


@pytest.fixture(scope="session")
def rendered_fe_tasks(feature_engineering_dag) -> dict[str, object]:
    """Return rendered tasks for the feature engineering DAG."""
    return _render_dag_tasks(feature_engineering_dag)

@pytest.fixture(scope="session")
def rendered_training_tasks(training_dag) -> dict[str, object]:
    """Return rendered tasks for the training DAG."""
    return _render_dag_tasks(training_dag)


@pytest.fixture(scope="session")
def rendered_prediction_tasks(prediction_dag) -> dict[str, object]:
    """Return rendered tasks for the prediction DAG."""
    return _render_dag_tasks(prediction_dag)
