import boto3
import logging
from dotenv import load_dotenv
from pyarrow.fs import S3FileSystem
from mypy_boto3_s3 import S3Client
from typing import Iterator

import scripts.bronze.unarchive_zip as unzip
import scripts.bronze.context as context
from settings.schemas.bronze_schemas import bronze_schemas

logger = logging.getLogger(__name__)


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
    context = context.get_context()

    source_s3_client = boto3.client("s3", endpoint_url=context["aws_endpoint_url"])
    upload_s3_client = S3FileSystem(endpoint_override=context["aws_endpoint_url"])

    source_stream = get_s3_object_iterator(
        s3_client=source_s3_client,
        source_bucket=context["landing_bucket"],
        source_key=context["landing_key"],
    )

    expected_schema = bronze_schemas[context["dataset_name"]]

    unarchived_stream = unzip.get_unarchived_stream(
        zip_iterator=source_stream, expected_schema=expected_schema
    )

    airflow_metadata_columns = context.get_airflow_metadata_columns(
        context["landing_key"]
    )
    append_file_name = True

    enriched_stream = unzip.add_columns_to_unarchived_stream(
        unarchived_stream=unarchived_stream,
        columns_shape=airflow_metadata_columns,
        append_file_name=append_file_name,
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
