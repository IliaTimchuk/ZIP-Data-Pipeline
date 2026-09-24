import os
import pytest
import pyarrow as pa
import src.bronze.context as con
from datetime import datetime, timezone
from unittest.mock import patch


@pytest.fixture
def set_env(monkeypatch):
    """
    Sets any environment variables passed by their names. A variable with the
    None value is removed from the environment, so a test can rely on it being
    unset regardless of the shell it runs in.
    """

    def _set_env(**variables):
        for name, value in variables.items():
            if value is None:
                monkeypatch.delenv(name, raising=False)
            else:
                monkeypatch.setenv(name, value)

    return _set_env


# get_context


def test_get_context_returns_all_five_values(set_env):
    set_env(
        LANDING_BUCKET="landing",
        LANDING_KEY="test/landing/key",
        DESTINATION_BUCKET="bronze",
        DATASET_NAME="test_dataset",
        AWS_ENDPOINT="http://test:9000",
    )

    context = con.get_context()

    assert context == {
        "landing_bucket": "landing",
        "landing_key": "test/landing/key",
        "destination_bucket": "bronze",
        "dataset_name": "test_dataset",
        "aws_endpoint": "http://test:9000",
    }


def test_get_context_not_raise_on_empty_aws_endpoint(set_env):
    set_env(
        LANDING_BUCKET="landing",
        LANDING_KEY="test/landing/key",
        DESTINATION_BUCKET="bronze",
        DATASET_NAME="test_dataset",
        AWS_ENDPOINT=None,
    )

    context = con.get_context()

    assert context["aws_endpoint"] is None
    assert context == {
        "landing_bucket": "landing",
        "landing_key": "test/landing/key",
        "destination_bucket": "bronze",
        "dataset_name": "test_dataset",
        "aws_endpoint": None,
    }


@pytest.mark.parametrize(
    "missing_var",
    ["LANDING_BUCKET", "LANDING_KEY", "DESTINATION_BUCKET", "DATASET_NAME"],
)
def test_get_context_raises_key_error_on_missing_required_vars(set_env, missing_var):
    env = {
        "LANDING_BUCKET": "landing",
        "LANDING_KEY": "test/landing/key",
        "DESTINATION_BUCKET": "bronze",
        "DATASET_NAME": "test_dataset",
    }
    env[missing_var] = None

    set_env(**env)

    with pytest.raises(
        KeyError,
        match=f"The required environment variables were not provided: '{missing_var}'",
    ):
        con.get_context()


# get_airflow_metadata_columns


def test_airflow_metadata_columns_build_correct_columns(set_env):
    test_dag_run_id = "scheduled__2026-08-20T12:00:00+00:00"
    test_landing_key = "test_source/test_dataset/ingest_date=2026-08-20/archive.zip"
    fixed_now = datetime(2026, 8, 20, 12, 0, 0, 123000, tzinfo=timezone.utc)
    set_env(_DAG_RUN_ID=test_dag_run_id)

    with patch("src.bronze.context.datetime") as mock_datetime:
        mock_datetime.now.return_value = fixed_now
        columns = con.get_airflow_metadata_columns(test_landing_key)

    assert columns == {
        "_bronze_processed_at": pa.scalar(
            fixed_now, type=pa.timestamp("ms", tz="UTC")
        ),
        "_dag_run_id": pa.scalar(test_dag_run_id, type=pa.string()),
        "_zip_file_name": pa.scalar("archive.zip", type=pa.string()),
    }