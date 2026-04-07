"""Level 2: Prediction DAG structure and configuration tests.

The prediction DAG runs daily after ingestion, materializes Feast
features, then runs batch inference with the champion model.
"""

from airflow.timetables.trigger import CronTriggerTimetable


class TestPredictionDagStructure:
    """Verify task graph shape."""

    def test_task_count(self, prediction_dag):
        """Should have exactly 2 tasks."""
        assert len(prediction_dag.tasks) == 2

    def test_task_ids(self, prediction_dag):
        """Task IDs: materialize_features, batch_predict."""
        tasks = {task.task_id for task in prediction_dag.tasks}

        assert tasks == {"materialize_features", "batch_predict"}

    def test_dependency_chain(self, prediction_dag):
        """materialize_features → batch_predict.
        """
        tasks_map = {task.task_id: task for task in prediction_dag.tasks}

        expected = {
            "materialize_features": {"upstream": set(), "downstream": {"batch_predict"}},
            "batch_predict": {"upstream": {"materialize_features"}, "downstream": set()}
        }

        for task_id, deps in expected.items():
            assert tasks_map[task_id].upstream_task_ids == deps["upstream"]
            assert tasks_map[task_id].downstream_task_ids == deps["downstream"]

    def test_tags(self, prediction_dag):
        """DAG should be tagged for UI filtering."""
        assert set(prediction_dag.tags) == {"prediction", "daily"}


class TestPredictionDagSchedule:
    """Verify daily schedule and timing."""

    def test_daily_cron_schedule(self, prediction_dag):
        """Should use CronTriggerTimetable at 08:00 UTC.

        Hint: the timetable object stores the cron expression — dig into its attributes.
        """
        assert isinstance(prediction_dag.timetable, CronTriggerTimetable)

    def test_runs_after_ingestion(self, prediction_dag, ingestion_dag):
        """Prediction (08:00) should be scheduled after ingestion (07:00).
        """
        pred_dag_cron = prediction_dag.timetable._expression
        ingest_dag_cron = ingestion_dag.timetable._expression

        pred_dag_cron_expression = pred_dag_cron.split()
        ingest_dag_cron_expression = ingest_dag_cron.split()

        assert pred_dag_cron_expression[1] > ingest_dag_cron_expression[1]


    def test_catchup_disabled(self, prediction_dag):
        """No backfill — each run uses latest features only."""
        assert prediction_dag.catchup is False


class TestPredictionDagRetries:
    """Verify fault-tolerance on the inference task."""

    def test_batch_predict_retries(self, prediction_dag):
        """batch_predict should retry (model serving can be flaky).
        """
        assert prediction_dag.get_task("batch_predict").retries == 2

    def test_materialize_no_extra_retries(self, prediction_dag):
        """materialize_features uses default retries (0).
        """
        assert prediction_dag.get_task("materialize_features").retries == 0


class TestPredictionDagParams:
    """Verify user-configurable parameters."""

    def test_tickers_param_exists(self, prediction_dag):
        """DAG should accept a 'tickers' parameter."""
        assert "tickers" in prediction_dag.params

    def test_tickers_default_matches_ingestion(self, prediction_dag):
        """Default basket should be the same mag-7 list as ingestion_dag.
        """
        tickers = prediction_dag.params["tickers"]
        expected = ["AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "TSLA"]

        assert set(tickers) == set(expected)


class TestPredictionDagEnv:
    """Verify rendered env vars resolve correctly."""

    def test_rendered_pythonpath(self, rendered_prediction_tasks):
        """PYTHONPATH should resolve to /app/src after rendering."""
        for task in rendered_prediction_tasks.values():
            assert task.env["PYTHONPATH"] == "/app/src"

    def test_rendered_path(self, rendered_prediction_tasks):
        """PATH should start with /app/.venv/bin after rendering."""
        for task in rendered_prediction_tasks.values():
            assert task.env["PATH"] == "/app/.venv/bin:/usr/local/bin:/usr/bin:/bin"
