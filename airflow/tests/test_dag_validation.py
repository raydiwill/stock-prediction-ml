"""Level 1: DAG validation tests.

Verifies that all DAG files in airflow/dags/ parse without errors,
have unique IDs, and meet baseline Airflow requirements.

These tests catch syntax errors, missing imports, and circular
dependencies BEFORE code reaches production.
"""

import pytest

# Every DAG ID that should exist after parsing.
EXPECTED_DAG_IDS = {
    "ingestion_dag",
    "feature_engineering_dag",
    "training_dag",
    "prediction_dag",
}


class TestDagLoading:
    """Ensure DagBag loads cleanly."""

    def test_no_import_errors(self, dag_bag):
        """DagBag should have zero import errors across all DAG files.

        Hint: dag_bag.import_errors is a dict {filepath: error_string}.
        An empty dict means every file parsed without errors.
        """
        error_dict = dag_bag.import_errors
        assert len(error_dict) == 0, f"DAG import errors: {error_dict}"

    def test_expected_dags_present(self, dag_bag):
        """All expected DAG IDs should be present in the bag.

        Hint: dag_bag.dags is a dict keyed by dag_id.
        Compare its keys against EXPECTED_DAG_IDS.
        """
        dag_keys = dag_bag.dags.keys()

        assert dag_keys == EXPECTED_DAG_IDS

    @pytest.mark.parametrize("dag_id", EXPECTED_DAG_IDS)
    def test_dag_has_tags(self, dag_bag, dag_id):
        """Every DAG should have at least one tag for UI filtering.

        Hint: dag_bag.get_dag(dag_id).tags returns a set or list.
        """
        tag_list = dag_bag.get_dag(dag_id).tags

        assert len(tag_list) > 0

    @pytest.mark.parametrize("dag_id", EXPECTED_DAG_IDS)
    def test_catchup_disabled(self, dag_bag, dag_id):
        """Catchup should be False to prevent accidental backfill storms.

        Hint: dag_bag.get_dag(dag_id).catchup is a bool.
        """
        catchup = dag_bag.get_dag(dag_id).catchup

        assert catchup is False
