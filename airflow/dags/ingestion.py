import settings.pipeline_config as conf
from scripts.utils.build_layer_key import build_landing_key
from airflow.sdk import dag, task, task_group
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from datetime import datetime, timedelta

default_args = {
    "owner": "IliaTimchuk",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": [SmtpNotifier(to=conf.ALERT_EMAILS)],
}

INGEST_FROM_S3_SOURCE_ALLOWED_SCHEMES = ["s3"]


@dag(
    schedule="0 0 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="This DAG extracts files from the S3 bucket once they "
    "are available and loads them to the landing bucket.",
    tags=["ingestion"],
    default_args=default_args,
)
def ingest_from_s3():

    @task(retry_delay=timedelta(seconds=30))
    def load_sources_config(**context) -> list[dict]:
        """
        Loads sources by dag_id from the sources configuration yaml file,
        taking the date and the dag_id from the context to the parser function.
        Processes the output to the format required by the task_group (extract_files)
        to expand the tasks.

        Works only with validated sources. If there are malformed sources for
        this DAG in the configuration file, they will be skipped, and an email
        notification will be sent.

        Returns:
            A list of dictionaries (each representing a single source) with the
            following keys:
                - source_name: The source name from the configuration file.
                - dataset_name: The dataset name from the configuration file that
                    is used to build a key in the landing bucket.
                - bucket: The source bucket name (assumed as base_url in the
                    sources configuration).
                - key: The source key to the file in the bucket.
                - aws_conn_id: The Airflow connection ID for the source S3 bucket.
                    If the connection is not provided in the sources configuration,
                    it defaults to AWS_ANONYMOUS_CONN_NAME (the source assumed to
                    be public).
        """

        from scripts.ingestion.read_sources import (
            get_dag_sources,
            prepare_s3_sources,
            get_error_message,
        )

        dag_id = context["dag"].dag_id
        task_id = context["ti"].task_id
        ds = context["ds"]

        valid_sources, errors = get_dag_sources(
            dag_id=dag_id,
            file_path=conf.SOURCES_YAML_PATH,
            allowed_schemes=INGEST_FROM_S3_SOURCE_ALLOWED_SCHEMES,
            date=ds,
        )

        if errors:
            SmtpNotifier(
                to=conf.ALERT_EMAILS,
                subject=f"[Airflow Warning] Malformed sources skipped in DAG: {dag_id}, Task: {task_id}",
                html_content=get_error_message(errors),
            )(context)

        kwargs_to_expand = prepare_s3_sources(
            valid_sources, conf.AWS_ANONYMOUS_CONN_NAME
        )

        return kwargs_to_expand

    @task_group(group_id="extract_files")
    def extract_files(source_name, dataset_name, bucket, key, aws_conn_id):
        """
        Waits for the file to be available, and then streams it to the landing bucket.

        One instance of this task group is created per source/endpoint combination,
        so each instance handles exactly one (bucket, key) pair.
        """

        wait_for_file = S3KeySensor(
            task_id="wait_for_s3_file",
            bucket_name=bucket,
            bucket_key=key,
            aws_conn_id=aws_conn_id,
            mode="reschedule",
            poke_interval=60 * 60,
            timeout=24 * 60 * 60,
        )

        @task(
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=30),
        )
        def stream_file(
            source_name: str,
            dataset_name: str,
            source_bucket: str,
            source_key: str,
            source_conn_id: str,
            ds: str,
        ) -> None:
            """
            Streams a single file from a source S3 bucket into the internal raw
            data bucket, in chunks. It also checks the uploaded object's size
            against the size on the source platform, deletes it if the size
            mismatches. Builds the following landing key:
            bucket/source_name/dataset_name/date=year-month-day/file_name.
            """

            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            from scripts.ingestion.extract_datasets import (
                upload_stream_to_s3,
                validate_file_size,
            )
            from botocore.config import Config

            source_hook = S3Hook(aws_conn_id=source_conn_id)
            source_client = source_hook.get_conn()

            obj = source_client.get_object(Bucket=source_bucket, Key=source_key)

            dest_hook = S3Hook(
                aws_conn_id=conf.AWS_CONN_NAME,
                config=Config(retries={"max_attempts": 3, "mode": "standard"}),
            )

            dest_client = dest_hook.get_conn()
            dest_key = build_landing_key(source_key, source_name, dataset_name, ds)

            with obj["Body"] as body_stream:
                upload_stream_to_s3(
                    fileobj=body_stream,
                    s3_client=dest_client,
                    bucket=conf.LANDING_BUCKET,
                    key=dest_key,
                )

            validate_file_size(
                s3_client=dest_client,
                bucket=conf.LANDING_BUCKET,
                key=dest_key,
                expected_size=obj.get("ContentLength"),
            )

        wait_for_file >> stream_file(
            source_name, dataset_name, bucket, key, aws_conn_id
        )

    configs = load_sources_config()
    extract_files.expand_kwargs(configs)


ingestion_dag = ingest_from_s3()
