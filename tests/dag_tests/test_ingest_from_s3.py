import pytest
from unittest.mock import patch
from airflow.utils.state import DagRunState
from datetime import datetime, timezone

from dags.ingestion import ingestion_dag


@pytest.mark.parametrize(
    "mapped_sources",
    [
        [
            {
                "source_name": "my_source",
                "dataset_name": "dataset",
                "bucket": "my_bucket",
                "key": "path/to/file.csv",
                "aws_conn_id": "my_conn",
            }
        ],
        [
            {
                "source_name": "my_source",
                "dataset_name": "dataset_a",
                "bucket": "bucket_a",
                "key": "a.csv",
                "aws_conn_id": "my_conn",
            },
            {
                "source_name": "my_source",
                "dataset_name": "dataset_b",
                "bucket": "bucket_b",
                "key": "b.csv",
                "aws_conn_id": "my_conn",
            },
            {
                "source_name": "other_source",
                "dataset_name": "dataset_c",
                "bucket": "bucket_c",
                "key": "c.csv",
                "aws_conn_id": "default_conn",
            },
        ],
    ],
    ids=["single_source", "three_sources"],
)
@patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_conn")
@patch("airflow.providers.amazon.aws.sensors.s3.S3KeySensor.execute", return_value=True)
@patch("scripts.ingestion.extract_datasets.upload_stream_to_s3")
@patch("scripts.ingestion.extract_datasets.validate_file_size")
@patch("scripts.ingestion.read_sources.prepare_s3_sources")
@patch("scripts.ingestion.read_sources.get_dag_sources", return_value=([], []))
def test_task_group_mapping_expansion(
    mock_get_dag_sources,
    mock_prepare_s3_sources,
    mock_validate,
    mock_upload,
    mock_sensor_execute,
    mock_s3_conn,
    mapped_sources,
):
    mock_prepare_s3_sources.return_value = mapped_sources

    dag_run = ingestion_dag.test(logical_date=datetime(2026, 1, 1, tzinfo=timezone.utc))

    assert dag_run.state == DagRunState.SUCCESS

    task_instances = dag_run.get_task_instances()

    def _get_mapped_tasks(task_id: str):
        return [
            ti for ti in task_instances if ti.task_id == task_id and ti.map_index != -1
        ]

    wait_tasks = _get_mapped_tasks("extract_files.wait_for_s3_file")
    stream_tasks = _get_mapped_tasks("extract_files.stream_file")

    expected = len(mapped_sources)
    assert len(wait_tasks) == expected
    assert len(stream_tasks) == expected
