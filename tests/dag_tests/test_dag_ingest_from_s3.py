import io
import pytest
from unittest.mock import patch
from airflow.utils.state import DagRunState
from datetime import datetime, timezone

from dags.ingestion import ingestion_dag


@pytest.fixture
def _disable_retries():
    """Disable retries on all DAG tasks for the duration of the test."""
    originals = [
        (t, t.retries, t.retry_exponential_backoff) for t in ingestion_dag.tasks
    ]
    for t in ingestion_dag.tasks:
        t.retries = 0
        t.retry_exponential_backoff = False

    yield

    for t, retries, backoff in originals:
        t.retries = retries
        t.retry_exponential_backoff = backoff


@patch("airflow.providers.smtp.notifications.smtp.SmtpNotifier.notify")
@patch("include.ingestion.read_sources.prepare_s3_sources")
@patch("include.ingestion.read_sources.get_dag_sources")
def test_smtp_notifier_fires_on_task_failure(
    mock_get_dag_sources, mock_prepare_s3_sources, mock_notify, _disable_retries
):
    mock_get_dag_sources.return_value = ({}, {})
    mock_prepare_s3_sources.side_effect = RuntimeError("forced failure for test")

    dag_run = ingestion_dag.test(
        logical_date=datetime(2026, 7, 29, tzinfo=timezone.utc)
    )

    assert dag_run.state == DagRunState.FAILED
    mock_notify.assert_called_once()


@patch("airflow.providers.smtp.notifications.smtp.SmtpNotifier.notify")
@patch("include.ingestion.read_sources.prepare_s3_sources")
@patch("include.ingestion.read_sources.get_dag_sources")
def test_smtp_notifier_fires_on_malformed_sources(
    mock_get_dag_sources, mock_prepare_s3_sources, mock_notify, _disable_retries
):
    mock_get_dag_sources.return_value = (
        {"valid_source": {"base_url": "s3://ok", "endpoints": ["path"]}},
        {"malformed_source": ["Invalid scheme", "Missing base_url"]},
    )

    mock_prepare_s3_sources.return_value = []

    dag_run = ingestion_dag.test(
        logical_date=datetime(2026, 7, 29, tzinfo=timezone.utc)
    )

    assert dag_run.state == DagRunState.SUCCESS

    mock_notify.assert_called_once()
