import sys
import os
import pytest
from unittest.mock import patch

airflow = pytest.importorskip("airflow", reason="apache-airflow not installed")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../orchestration/dags"))

os.environ.setdefault("AIRFLOW_HOME", "/tmp/airflow-ci")

from wb_pipeline import push_pipeline_status


class TestPushPipelineStatus:
    def test_skips_when_no_url(self, monkeypatch):
        monkeypatch.delenv("PUSHGATEWAY_URL", raising=False)
        with patch("wb_pipeline.push_to_gateway") as mock_push:
            push_pipeline_status("failure")
        mock_push.assert_not_called()

    def test_success_pushes_gauge_value_1(self, monkeypatch):
        monkeypatch.setenv("PUSHGATEWAY_URL", "http://fake:9091")
        captured = {}

        def fake_push(url, job, registry):
            for metric in registry._names_to_collectors.values():
                if hasattr(metric, "_value"):
                    captured[metric._name] = metric._value.get()

        with patch("wb_pipeline.push_to_gateway", side_effect=fake_push):
            push_pipeline_status("success")

        assert captured.get("pipeline_last_run_status") == 1.0

    def test_failure_pushes_gauge_value_0(self, monkeypatch):
        monkeypatch.setenv("PUSHGATEWAY_URL", "http://fake:9091")
        captured = {}

        def fake_push(url, job, registry):
            for metric in registry._names_to_collectors.values():
                if hasattr(metric, "_value"):
                    captured[metric._name] = metric._value.get()

        with patch("wb_pipeline.push_to_gateway", side_effect=fake_push):
            push_pipeline_status("failure")

        assert captured.get("pipeline_last_run_status") == 0.0
