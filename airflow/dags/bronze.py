import os
import settings.pipeline_config as conf
from docker.types import Mount
from airflow.sdk import dag, task, literal
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from datetime import datetime, timedelta
from settings.airflow_assets import LANDING_ASSET

AIRFLOW_PROJ_DIR = os.environ["AIRFLOW_PROJ_DIR"]

default_args = {
    "owner": "IliaTimchuk",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": [SmtpNotifier(to=conf.ALERT_EMAILS)],
}


@dag(
    schedule=[LANDING_ASSET],
    start_date=datetime(2026, 1, 1),
    description="Decompresses extracted ZIP files from the landing bucket in a"
    " separate Docker container and writes them as Parquet in the bronze bucket.",
    tags=["bronze"],
    default_args=default_args,
    catchup=False,
)
def bronze_zip_to_parquet():

    @task
    def load_bronze_config(**context):
        """
        Prepare environment variables for the bronze transformation.

        The returned dictionary is stored in XCom, so it mustn't include
        any confidential data.
        """
        events = context["triggering_asset_events"][LANDING_ASSET]
        return [
            {
                "LANDING_BUCKET": e.extra["landing_bucket"],
                "LANDING_KEY": e.extra["landing_key"],
                "DATASET_NAME": e.extra["dataset_name"],
                "DESTINATION_BUCKET": conf.BRONZE_BUCKET,
                "_DAG_RUN_ID": context["run_id"],
            }
            for e in events
        ]

    files_to_transform = load_bronze_config()

    container = DockerOperator.partial(
        task_id="bronze_transformation",
        image="pyarrow-custom",
        command=["python", "-m", conf.BRONZE_ENTRYPOINT_MODULE],
        mounts=[
            Mount(target="/src", source=f"{AIRFLOW_PROJ_DIR}/src", type="bind"),
            Mount(
                target="/settings", source=f"{AIRFLOW_PROJ_DIR}/settings", type="bind"
            ),
            Mount(target=literal("/.env"), source=literal(f"{AIRFLOW_PROJ_DIR}/.env"), type="bind"),
        ],
        auto_remove="success",
        mount_tmp_dir=False,
        network_mode=os.environ["DOCKER_NETWORK_NAME"]
    ).expand(environment=files_to_transform)


bronze_dag = bronze_zip_to_parquet()