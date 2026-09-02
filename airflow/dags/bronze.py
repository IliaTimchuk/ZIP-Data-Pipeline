import os
import settings.pipeline_config as conf
from airflow.sdk import dag, task
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from datetime import datetime, timedelta

default_args = {
    "owner": "IliaTimchuk",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": [SmtpNotifier(to=conf.ALERT_EMAILS)],
}


@dag(
    description="Decompresses extracted ZIP files from the landing bucket in a"
    " separate Docker container and writes them as Parquet in the bronze bucket.",
    tags=["bronze"],
    default_args=default_args,
)
def bronze_transformation_zip_to_parquet():

    @task
    def load_unprocessed_landing_files():
        pass

    files_to_transform = load_unprocessed_landing_files()

    # build_image_if_not_exist = BashOperator(
    #     task_id=...,
    #     bash_command="""
    #     if ! docker inspect custom-pyarrow:latest >/dev/null 2>&1; then
    #         echo "The pyarrow image hasn't built. Building..."
    #         docker build -t custom-pyarrow ./docker/pyarrow/
    #     else 
    #         echo "The pyarrow image already exist. Skipping building."
    #     fi
    #     """,
    # )

    container = DockerOperator.partial(
        task_id="bronze_transformation",
        image="custom-pyarrow",
        container_name="pyarrow",
        command=["python", "-m", "scripts.bronze.bronze_entrypoint"],
        volumes=[
            f"{AIRFLOW_PROJ_DIR}/scripts:/scripts",
            f"{AIRFLOW_PROJ_DIR}/settings:/settings",
        ],
        auto_remove="success",
    ).expand(
        env_file=...
    )  # files_to_transform
