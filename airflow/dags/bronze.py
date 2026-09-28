import os
import settings.pipeline_config as conf
from airflow.sdk import dag, task, task_group
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from datetime import datetime, timedelta
from settings.airflow_assets import LANDING_ASSET

default_args = {
    "owner": "IliaTimchuk",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": [SmtpNotifier(to=conf.ALERT_EMAILS)],
}


def bronze_private_env() -> dict[str, str]:
    env = {
        "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
        "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
    }

    endpoint = os.environ.get("AWS_ENDPOINT")
    if endpoint:
        env["AWS_ENDPOINT"] = endpoint

    return env


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

    bronze_config = load_bronze_config()

    @task_group
    def transform_zip_archives(config):
        """
        Transforms each ZIP archive in a separate Docker container and alerts all
        decompressed files with invalid schema.
        """

        container = DockerOperator(
            task_id="bronze_transformation",
            image="pyarrow-custom",
            command=["python", "-m", conf.BRONZE_ENTRYPOINT_MODULE],
            private_environment=bronze_private_env(),
            auto_remove="success",
            mount_tmp_dir=False,
            network_mode=os.environ.get(
                "DOCKER_NETWORK_NAME", "zip-data-pipeline_default"
            ),
            max_active_tis_per_dag=4,
            environment=config,
        )

        @task
        def alert_files_with_invalid_schema(config, **context):

            import posixpath
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            from src.utils.build_layer_key import build_bronze_schema_status_key

            hook = S3Hook(aws_conn_id=conf.AWS_CONN_NAME)
            bucket = config["DESTINATION_BUCKET"]
            landing_key = config["LANDING_KEY"]

            invalid_files_bronze_key = build_bronze_schema_status_key(
                landing_key, conf.INVALID_PREFIX
            )

            invalid_schema_keys = hook.list_keys(
                bucket_name=bucket, prefix=f"{invalid_files_bronze_key}/"
            )

            if invalid_schema_keys:
                file_names = sorted(
                    {
                        posixpath.basename(key).rsplit("_part-", 1)[0]
                        for key in invalid_schema_keys
                    }
                )
                SmtpNotifier(
                    to=conf.ALERT_EMAILS,
                    subject=f"[Airflow Warning] Files with invalid schema in ZIP: {landing_key}",
                    html_content=(
                        f"Files with invalid schema: {', '.join(file_names)}<br>"
                        f"Location: s3://{bucket}/{invalid_files_bronze_key}/"
                    ),
                )(context)

        container >> alert_files_with_invalid_schema(config)

    transform_zip_archives.expand(config=bronze_config)


bronze_dag = bronze_zip_to_parquet()
