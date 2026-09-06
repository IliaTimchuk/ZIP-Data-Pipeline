import os
import boto3
import logging
import pyarrow as pa
import pyarrow.csv as pa_csv
from dotenv import load_dotenv
from datetime import datetime, timezone
from pyarrow.fs import S3FileSystem
from mypy_boto3_s3 import S3Client
from typing import Iterator

import scripts.bronze.unarchive_zip as unzip
from scripts.bronze.read_dataset_schema import read_dataset_schema_from_yaml
from settings.pipeline_config import DATASET_SCHEMAS_YAML_PATH

logger = logging.getLogger(__name__)


def get_context() -> dict:
    try:
        context = {
            "landing_bucket": os.environ["LANDING_BUCKET"],
            "landing_key": os.environ["LANDING_KEY"],
            "dataset_name": os.environ["DATASET_NAME"],
            "destination_bucket": os.environ["DESTINATION_BUCKET"],
            "aws_endpoint_url": os.getenv("AWS_ENDPOINT"),
        }
        logger.info("The bronze context was succesfully extracted. Values: %s", context)
        return context
    except KeyError as e:
        raise KeyError(
            f"The required environment variable(-s) was not provided: {e}"
        ) from e


def get_airflow_metadata_columns(landing_key: str):

    dag_run_id = os.environ["_DAG_RUN_ID"]

    time = datetime.now(timezone.utc)
    bronze_processed_at = int(time.timestamp() * 1000)

    zip_file_name = landing_key.rsplit("/", 1)[-1]

    airflow_metadata_columns = {
        "_bronze_processed_at": pa.scalar(
            bronze_processed_at, pa.timestamp("ms", tz="UTC")
        ),
        "_dag_run_id": pa.scalar(dag_run_id, pa.string()),
        "_zip_file_name": pa.scalar(zip_file_name, pa.string()),
    }

    logger.info(
        "The airflow_metadata_columns were succesfully built. Values: %s",
        airflow_metadata_columns,
    )

    return airflow_metadata_columns


def get_s3_object_iterator(
    s3_client: S3Client,
    source_bucket: str,
    source_key: str,
    chunk_size: int = 16 * 1024 * 1024,
) -> Iterator[bytes]:
    """
    Returns an iterable to the S3 object from S3 in chunks of specified size.
    """
    yield from s3_client.get_object(Bucket=source_bucket, Key=source_key)[
        "Body"
    ].iter_chunks(chunk_size)


def main():
    load_dotenv("/.env")
    context = get_context()
    airflow_metadata_columns = get_airflow_metadata_columns(context["landing_key"])

    source_s3_client = boto3.client("s3", endpoint_url=context["aws_endpoint_url"])
    upload_s3_client = S3FileSystem(endpoint_override=context["aws_endpoint_url"])

    source_stream = get_s3_object_iterator(
        s3_client=source_s3_client,
        source_bucket=context["landing_bucket"],
        source_key=context["landing_key"],
    )

    unarchived_stream = unzip.get_unarchived_stream(
        zip_iterator=source_stream,
        read_func=pa_csv.open_csv,
    )

    append_file_name = True
    enriched_stream = unzip.add_columns_to_unarchived_stream(
        unarchived_stream=unarchived_stream,
        columns_shape=airflow_metadata_columns,
        append_file_name=append_file_name,
    )

    expected_schema = read_dataset_schema_from_yaml(
        file_path=DATASET_SCHEMAS_YAML_PATH, dataset_name=context["dataset_name"]
    )

    metadata_column_names = list(airflow_metadata_columns.keys())

    if append_file_name:
        metadata_column_names += ["_source_file_name"]

    unzip.upload_unarchived_zip_stream_to_s3(
        unarchived_stream=enriched_stream,
        s3fs=upload_s3_client,
        bucket=context["destination_bucket"],
        source_key=context["landing_key"],
        expected_schema=expected_schema,
        metadata_columns=metadata_column_names,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
