"""Level 2: Ingestion DAG structure and configuration tests.

The ingestion DAG has the most configuration surface of all DAGs:
scheduled runs, user params, per-task retries, and Jinja-templated
bash commands.
"""

from datetime import datetime, timedelta


class TestIngestionDagStructure:
    """Verify task graph shape and wiring."""

    def test_task_count(self, ingestion_dag):
        """Should have exactly 3 tasks."""
        assert len(ingestion_dag.tasks) == 3

    def test_task_ids(self, ingestion_dag):
        """Task IDs should be fetch_data, validate_data, ingest_to_db."""
        tasks = {task.task_id for task in ingestion_dag.tasks}
        assert tasks == {"fetch_data", "validate_data", "ingest_to_db"}

    def test_tags(self, ingestion_dag):
        """DAG should be tagged for UI filtering."""
        assert set(ingestion_dag.tags) == {"ingestion", "data"}

    def test_dependency_chain(self, ingestion_dag):
        """Task wiring should be fetch_data >> validate_data >> ingest_to_db."""
        task_map = {task.task_id: task for task in ingestion_dag.tasks}

        expected = {
            "fetch_data": {"upstream": set(), "downstream": {"validate_data"}},
            "validate_data": {"upstream": {"fetch_data"}, "downstream": {"ingest_to_db"}},
            "ingest_to_db": {"upstream": {"validate_data"}, "downstream": set()},
        }

        for task_id, deps in expected.items():
            assert task_map[task_id].upstream_task_ids == deps["upstream"]
            assert task_map[task_id].downstream_task_ids == deps["downstream"]


class TestIngestionDagSchedule:
    """Verify schedule and timing configuration."""

    def test_has_schedule(self, ingestion_dag):
        """Should have a CronTriggerTimetable (not None)."""
        assert ingestion_dag.schedule is not None

    def test_start_date(self, ingestion_dag):
        """start_date should be 2025-02-24."""
        assert ingestion_dag.start_date.date() == datetime(2025, 2, 24).date()

    def test_catchup_disabled(self, ingestion_dag):
        """Catchup should be disabled to avoid backfilling historical runs."""
        assert ingestion_dag.catchup is False


class TestIngestionDagRetries:
    """Verify retry and fault-tolerance configuration."""

    def test_fetch_data_retries(self, ingestion_dag):
        """fetch_data should have 3 retries (API calls are flaky)."""
        assert ingestion_dag.get_task("fetch_data").retries == 3


    def test_fetch_data_retry_delay(self, ingestion_dag):
        """fetch_data retry_delay should be 5 minutes."""
        assert ingestion_dag.get_task("fetch_data").retry_delay == timedelta(minutes=5)


    def test_validate_data_retries(self, ingestion_dag):
        """validate_data should have 2 retries."""
        assert ingestion_dag.get_task("validate_data").retries == 2

    def test_ingest_to_db_retries(self, ingestion_dag):
        """ingest_to_db should have 2 retries."""
        assert ingestion_dag.get_task("ingest_to_db").retries == 2


class TestIngestionDagParams:
    """Verify user-configurable parameters."""

    def test_tickers_param_exists(self, ingestion_dag):
        """DAG should accept a 'tickers' parameter."""
        assert "tickers" in ingestion_dag.params


    def test_tickers_default_is_mag7(self, ingestion_dag):
        """Default tickers should be the magnificent-seven basket."""
        tickers = ingestion_dag.params["tickers"]
        expected_tickers = ["AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "TSLA"]

        assert set(tickers) == set(expected_tickers)


    def test_tickers_param_type(self, ingestion_dag):
        """tickers param should be typed as 'array'."""
        assert isinstance(ingestion_dag.params["tickers"], list)
