import os
import boto3
import pyarrow as pa
import pyarrow.csv as pa_csv
import unarchive_zip as unzip
from pyarrow.fs import S3FileSystem
from mypy_boto3_s3 import S3Client
from typing import Iterator


def get_context() -> dict:
    try:
        return {
            "source_bucket": os.environ["SOURCE_BUCKET"],
            "source_key": os.environ["SOURCE_KEY"],
            "destination_bucket": os.environ["DESTINATION_BUCKET"],
            "expected_schema": os.environ["EXPECTED_SCHEMA"],
            "endpoint_url": os.getenv("AWS_ENDPOINT"),
        }
    except KeyError as e:
        raise KeyError(
            f"The required environment variable(-s) was not provided: {e}"
        ) from e


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


if __name__ == "__main__":
    context = get_context()

    source_s3_client = boto3.client("s3", endpoint_url=context["endpoint_url"])
    upload_s3_client = S3FileSystem(endpoint_override=context["endpoint_url"])

    source_stream = get_s3_object_iterator(
        s3_client=source_s3_client,
        source_bucket=context["source_bucket"],
        source_key=context["source_key"],
    )

    readers = {"csv": pa_csv.open_csv} 

    unarchived_stream = unzip.get_unarchived_stream(
        zip_iterator=source_stream,
        readers=readers,
    )

    unzip.upload_unarchived_zip_stream_to_s3(
        unarchived_stream=unarchived_stream,
        s3fs=upload_s3_client,
        bucket=context["destination_bucket"],
        source_key=context["source_key"],
        expected_schema=context["expected_schema"].split(","),
    )
