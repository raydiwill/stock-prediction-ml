"""Level 2: Feature Engineering DAG structure and configuration tests.

The feature engineering DAG is manually triggered (schedule=None) and
builds the full feature pipeline: export history from DB, compute
technical/temporal features, then apply and materialize to Feast.

All four tasks are @task.bash with a shared _ENV dict that relies on
the Airflow Variable ``project_root`` for PYTHONPATH and PATH.
"""



class TestFeatureEngineeringStructure:
    """Verify task graph shape."""

    def test_task_count(self, feature_engineering_dag):
        """Should have exactly 4 tasks."""
        assert len(feature_engineering_dag.tasks) == 4

    def test_task_ids(self, feature_engineering_dag):
        """Task IDs: export_from_db, build_features, feast_apply, feast_materialize."""
        tasks = {task.task_id for task in feature_engineering_dag.tasks}
        assert tasks == {"export_from_db", "build_features", "feast_apply", "feast_materialize"}

    def test_dependency_chain(self, feature_engineering_dag):
        """export_from_db → build_features → feast_apply → feast_materialize."""
        task_map = {task.task_id: task for task in feature_engineering_dag.tasks}

        expected = {
            "export_from_db": {"upstream": set(), "downstream": {"build_features"}},
            "build_features": {"upstream": {"export_from_db"}, "downstream": {"feast_apply"}},
            "feast_apply": {"upstream": {"build_features"}, "downstream": {"feast_materialize"}},
            "feast_materialize": {"upstream": {"feast_apply"}, "downstream": set()},
        }


        for task_id, deps in expected.items():
            assert task_map[task_id].upstream_task_ids == deps["upstream"]
            assert task_map[task_id].downstream_task_ids == deps["downstream"]


# class TestFeatureEngineeringSchedule:
#     """Verify this DAG is manual-trigger only."""
#
#     def test_no_schedule(self, feature_engineering_dag):
#         """Schedule should be None (manual trigger only)."""
#         ...


class TestFeatureEngineeringEnv:
    """Verify Jinja templates in env render to actual paths."""

    def test_rendered_pythonpath(self, rendered_fe_tasks):
        """After rendering, PYTHONPATH should resolve to /app/src."""
        for task in rendered_fe_tasks.values():
            assert task.env["PYTHONPATH"] == "/app/src"

    def test_rendered_path(self, rendered_fe_tasks):
        """After rendering, PATH should start with /app/.venv/bin."""
        for task in rendered_fe_tasks.values():
            assert task.env["PATH"] == "/app/.venv/bin:/usr/local/bin:/usr/bin:/bin"
