"""Shared fixtures for Airflow DAG tests.

Provides DagBag loading with mocked Airflow Variables so DAG files
can be imported without a running Airflow metadata database.

Why mock Variable.get?
    feature_engineering_dag.py and training_dag.py call Variable.get()
    at **module level** (import time). Without the mock, DagBag will
    try to hit the metadata DB and crash.
"""

from pathlib import Path
from unittest.mock import patch

import pytest
from airflow.models import DagBag

DAGS_DIR = str(Path(__file__).parent.parent / "dags")

# Add new Variables here as your DAGs evolve.
_VARIABLE_DEFAULTS: dict[str, str] = {
    "project_root": "/app",
}


def _mock_variable_get(key: str, default_var: str | None = None) -> str:
    """Stand-in for airflow.models.Variable.get during tests."""
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
    ingestion_dag = dag_bag.get_dag("ingestion_dag")
    assert ingestion_dag is not None

    return ingestion_dag


@pytest.fixture(scope="session")
def feature_engineering_dag(dag_bag: DagBag):
    """Return the parsed feature_engineering_dag."""
    feature_engineering_dag = dag_bag.get_dag("feature_engineering_dag")
    assert feature_engineering_dag is not None

    return feature_engineering_dag


@pytest.fixture(scope="session")
def training_dag(dag_bag: DagBag):
    """Return the parsed training_dag."""
    training_dag = dag_bag.get_dag("training_dag")
    assert training_dag is not None

    return training_dag


@pytest.fixture(scope="session")
def prediction_dag(dag_bag: DagBag):
    """Return the parsed prediction_dag."""
    prediction_dag = dag_bag.get_dag("prediction_dag")
    assert prediction_dag is not None

    return prediction_dag
