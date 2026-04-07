"""Level 2: Training DAG structure and configuration tests."""

from airflow.timetables.trigger import CronTriggerTimetable


class TestTrainingDagStructure:
    """Verify task graph shape."""

    def test_task_count(self, training_dag):
        """Should have exactly 2 tasks."""
        assert len(training_dag.tasks) == 2

    def test_task_ids(self, training_dag):
        """Task IDs: train_model, promote_model.
        """
        tasks = {task.task_id for task in training_dag.tasks}
        assert tasks == {"train_model", "promote_model"}

    def test_dependency_chain(self, training_dag):
        """train_model → promote_model.
        """
        tasks_map = {task.task_id: task for task in training_dag.tasks}

        expected = {
            "train_model": {"upstream": set(), "downstream": {"promote_model"}},
            "promote_model": {"upstream": {"train_model"}, "downstream": set()}
        }

        for task_id, deps in expected.items():
            assert tasks_map[task_id].upstream_task_ids == deps["upstream"]
            assert tasks_map[task_id].downstream_task_ids == deps["downstream"]


class TestTrainingDagSchedule:
    """Verify the DAG runs on a weekly schedule."""

    def test_weekly_schedule(self, training_dag):
        """Schedule should be a CronTriggerTimetable (weekly).
        """
        assert isinstance(training_dag.timetable, CronTriggerTimetable)


class TestTrainingDagParams:
    """Verify DAG params for date-aware training."""

    def test_config_path_param_exists(self, training_dag):
        """Should have a config_path param.

        Hint: "config_path" in training_dag.params
        """
        assert "config_path" in training_dag.params

    def test_start_date_param_exists(self, training_dag):
        """Should have a start_date param for training window control.
        """
        assert "start_date" in training_dag.params

    def test_start_date_default_is_none(self, training_dag):
        """start_date default should be None (optional; falls back to ds-180 in template).
        """
        assert training_dag.params["start_date"] is None


class TestTrainingDagEnv:
    """Verify environment setup uses the mocked project_root '/app'."""

    def test_env_uses_project_root(self, rendered_training_tasks):
        """Task env should reference '/app' in both PYTHONPATH and PATH.
        """
        for task in rendered_training_tasks.values():
            assert task.env["PYTHONPATH"] == '/app/src'
            assert '/app/.venv/bin' in task.env["PATH"]
