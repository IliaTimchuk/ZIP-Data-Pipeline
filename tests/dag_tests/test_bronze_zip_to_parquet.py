from types import SimpleNamespace

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
    transformation = bronze_dag.get_task("bronze_transformation")

    assert transformation.upstream_task_ids == {"load_bronze_config"}


def test_bronze_transformation_runs_bronze_entrypoint():
    transformation = bronze_dag.get_task("bronze_transformation")

    assert transformation.partial_kwargs["command"] == [
        "python",
        "-m",
        "src.bronze.entrypoint",
    ]


def test_bronze_transformation_passes_aws_credentials_privately():
    transformation = bronze_dag.get_task("bronze_transformation")
    private_environment = transformation.partial_kwargs["private_environment"]

    assert "AWS_ACCESS_KEY_ID" in private_environment
    assert "AWS_SECRET_ACCESS_KEY" in private_environment