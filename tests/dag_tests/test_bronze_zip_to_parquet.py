from types import SimpleNamespace
from unittest.mock import patch

from dags.bronze import bronze_dag
from settings.airflow_assets import LANDING_ASSET

# load_bronze_config


def test_load_bronze_config_builds_one_config_per_event():
    load_bronze_config = bronze_dag.get_task("load_bronze_config").python_callable
    events = [
        SimpleNamespace(
            extra={
                "landing_bucket": "landing",
                "landing_key": "source/dataset_a/ingest_date=2026-01-01/a.zip",
                "dataset_name": "dataset_a",
            }
        ),
        SimpleNamespace(
            extra={
                "landing_bucket": "landing",
                "landing_key": "source/dataset_b/ingest_date=2026-01-01/b.zip",
                "dataset_name": "dataset_b",
            }
        ),
    ]

    configs = load_bronze_config(
        triggering_asset_events={LANDING_ASSET: events},
        run_id="asset_triggered__2026-01-01T00:00:00+00:00",
    )

    assert configs == [
        {
            "LANDING_BUCKET": "landing",
            "LANDING_KEY": "source/dataset_a/ingest_date=2026-01-01/a.zip",
            "DATASET_NAME": "dataset_a",
            "DESTINATION_BUCKET": "bronze",
            "_DAG_RUN_ID": "asset_triggered__2026-01-01T00:00:00+00:00",
        },
        {
            "LANDING_BUCKET": "landing",
            "LANDING_KEY": "source/dataset_b/ingest_date=2026-01-01/b.zip",
            "DATASET_NAME": "dataset_b",
            "DESTINATION_BUCKET": "bronze",
            "_DAG_RUN_ID": "asset_triggered__2026-01-01T00:00:00+00:00",
        },
    ]


# DAG structure


def test_bronze_dag_is_scheduled_on_landing_asset():
    assert bronze_dag.schedule == [LANDING_ASSET]


def test_bronze_transformation_runs_after_load_bronze_config():
    transformation = bronze_dag.get_task("transform_zip_archives.bronze_transformation")

    assert transformation.upstream_task_ids == {"load_bronze_config"}


def test_bronze_transformation_runs_bronze_entrypoint():
    transformation = bronze_dag.get_task("transform_zip_archives.bronze_transformation")

    assert transformation.command == [
        "python",
        "-m",
        "src.bronze.entrypoint",
    ]


def test_bronze_transformation_passes_aws_credentials_privately():
    transformation = bronze_dag.get_task("transform_zip_archives.bronze_transformation")
    private_environment = transformation._private_environment

    assert "AWS_ACCESS_KEY_ID" in private_environment
    assert "AWS_SECRET_ACCESS_KEY" in private_environment


def test_alert_runs_after_bronze_transformation():
    alert = bronze_dag.get_task(
        "transform_zip_archives.alert_files_with_invalid_schema"
    )

    assert alert.upstream_task_ids == {
        "load_bronze_config",
        "transform_zip_archives.bronze_transformation",
    }


# alert_files_with_invalid_schema


@patch("dags.bronze.SmtpNotifier")
@patch(
    "airflow.providers.amazon.aws.hooks.s3.S3Hook.list_keys",
    return_value=[
        "source/dataset_a/ingest_date=2026-01-01/zip_name=a/schema_status=invalid/a.csv_part-0.parquet",
        "source/dataset_a/ingest_date=2026-01-01/zip_name=a/schema_status=invalid/a.csv_part-1.parquet",
        "source/dataset_a/ingest_date=2026-01-01/zip_name=a/schema_status=invalid/sub__b.json_part-0.parquet",
    ],
)
def test_alert_sends_email_with_invalid_file_names(mock_list_keys, mock_notifier):
    alert = bronze_dag.get_task(
        "transform_zip_archives.alert_files_with_invalid_schema"
    ).python_callable

    alert(
        config={
            "LANDING_KEY": "source/dataset_a/ingest_date=2026-01-01/a.zip",
            "DESTINATION_BUCKET": "bronze",
        }
    )

    assert mock_notifier.call_args.kwargs["html_content"] == (
        "Files with invalid schema: a.csv, sub__b.json<br>"
        "Location: s3://bronze/source/dataset_a/ingest_date=2026-01-01/zip_name=a/schema_status=invalid/"
    )


@patch("dags.bronze.SmtpNotifier")
@patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.list_keys", return_value=[])
def test_alert_sends_no_email_without_invalid_files(mock_list_keys, mock_notifier):
    alert = bronze_dag.get_task(
        "transform_zip_archives.alert_files_with_invalid_schema"
    ).python_callable

    alert(
        config={
            "LANDING_KEY": "source/dataset_a/ingest_date=2026-01-01/a.zip",
            "DESTINATION_BUCKET": "bronze",
        }
    )

    mock_notifier.assert_not_called()
