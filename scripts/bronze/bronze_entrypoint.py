import os
import boto3
import pyarrow as pa
import pyarrow.csv as pa_csv
from pyarrow.fs import S3FileSystem
from mypy_boto3_s3 import S3Client
from typing import Iterator, Callable

import scripts.bronze.unarchive_zip as unzip
from scripts.bronze.read_dataset_schema import read_dataset_schema_from_yaml
from settings.pipeline_config import DATASET_SCHEMAS_YAML_PATH


def get_context() -> dict:
    try:
        return {
            "source_bucket": os.environ["SOURCE_BUCKET"],
            "source_key": os.environ["SOURCE_KEY"],
            "dataset_name": os.environ["DATASET_NAME"],
            "destination_bucket": os.environ["DESTINATION_BUCKET"],
            "endpoint_url": os.getenv("AWS_ENDPOINT"),
        }
    except KeyError as e:
        raise KeyError(
            f"The required environment variable(-s) was not provided: {e}"
        ) from e


def get_airflow_metadata_columns():
    bronze_processed_at = os.environ["_BRONZE_PROCESSED_AT"]
    dag_run_id = os.environ["_DAG_RUN_ID"]
    zip_file_name = os.environ["_ZIP_FILE_NAME"]

    airflow_metadata_columns = {
        "_bronze_processed_at": pa.scalar(int(bronze_processed_at), pa.timestamp("ms", tz="UTC")),
        "_dag_run_id": pa.scalar(dag_run_id, pa.string()),
        "_zip_file_name": pa.scalar(zip_file_name, pa.string()),
    }
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
    context = get_context()
    airflow_metadata_columns = get_airflow_metadata_columns()

    source_s3_client = boto3.client("s3", endpoint_url=context["endpoint_url"])
    upload_s3_client = S3FileSystem(endpoint_override=context["endpoint_url"])

    source_stream = get_s3_object_iterator(
        s3_client=source_s3_client,
        source_bucket=context["source_bucket"],
        source_key=context["source_key"],
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
        source_key=context["source_key"],
        expected_schema=expected_schema,
        metadata_columns=metadata_column_names,
    )
