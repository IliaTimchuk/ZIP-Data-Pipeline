import os
import pyarrow as pa
from include.utils.io_utils import BytesIteratorIO
from logging import getLogger
from mypy_boto3_s3 import S3Client
from stream_unzip import stream_unzip
from typing import Iterable, Callable, Union

logger = getLogger(__name__)


def get_pyarrow_s3_filesystem() -> pa.fs.S3FileSystem:
    """
    Returns the pyarrow S3 filesystem configured with the AWS credentials and endpoint from environment variables.
    Parameters:
        None
    Returns:
        The configured instance of pyarrow.fs.S3FileSystem.
    Raises:
        Any exceptions raised by pyarrow.fs.S3FileSystem initialization."""
    return pa.fs.S3FileSystem(
        access_key=os.getenv("AWS_ACCESS_KEY_ID"),
        secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        endpoint_override=os.getenv("AWS_ENDPOINT"),
    )


def get_s3_object_iterable(
    s3_client: S3Client, source_bucket: str, object_key: str, chunk_size: int
) -> Iterable[bytes]:
    """
    Returns an iterable to the S3 object from S3 in chunks of specified size.
    Parameters:
        s3_client: The boto3 S3 client.
        source_bucket: The name of the S3 bucket containing the file.
        object_key: The key of the file in the S3 bucket.
        chunk_size: The size of each chunk that will be yielded.
    Returns:
        An iterable of bytes representing the file content in chunks.
    Raises:
        Any exceptions raised by the boto3 S3 client during the get_object call.
    """
    yield from s3_client.get_object(Bucket=source_bucket, Key=object_key)[
        "Body"
    ].iter_chunks(chunk_size)


def unarchive_zip_stream_to_s3(
    iterable_stream: Iterable[bytes],
    read_function: Callable[
        ..., Union[pa.Table, pa.RecordBatchReader, Iterable[pa.RecordBatch]]
    ],
    destination_bucket: str,
    max_row_per_group: int,
    max_row_per_file: int,
    decompressed_chunk_size: int = 65536,
    zip_files_password: bytes | None = None,
    **reader_kwargs,
) -> None:
    """
    Reads an iterable stream of zipped files, unarchives each chunk, and writes
    the contents to an S3 bucket in Parquet format.

    This function utilizes the `stream_unzip` package
    (https://stream-unzip.docs.trade.gov.uk/) to handle decompression on the fly.

    Args:
        iterable_stream: An iterable stream of bytes representing the zipped files.
        read_function: A callable that reads the unzipped data and returns a
            PyArrow Table, RecordBatchReader, or an iterable of RecordBatches.
            Keep in mind that some functions may read the entire file into memory;
            choose or configure a function that streams data if memory is limited.
        destination_bucket: The name of the S3 bucket (and optional prefix path)
            where the unarchived files will be stored (e.g., "my-bucket" or "my-bucket/data").
        max_row_per_group: The maximum number of rows per group in the Parquet files.
            This determines how much data PyArrow buffers in memory before writing a
            chunk to S3. It should be tuned according to available memory.
        max_row_per_file: The maximum number of rows per individual Parquet file.
        decompressed_chunk_size: The size of the chunks (in bytes) to read from the
            unzipped stream. Defaults to 65536 bytes (64 KB).
        zip_files_password: Password for the zipped files, if they are password-protected.
            Defaults to None.
        **reader_kwargs: Additional keyword arguments to pass directly to the `read_function`.

    Raises:
        Any exceptions raised by the `read_function`, `stream_unzip`, or PyArrow/S3
        write operations.

    Returns:
        None
    """

    s3_fs = get_pyarrow_s3_filesystem()
    base_path = f"{destination_bucket}/"

    unzipped_stream = stream_unzip(
        iterable_stream, password=zip_files_password, chunk_size=decompressed_chunk_size
    )

    files_processed = 0
    total_bytes_processed = 0

    for file_name, file_size, unzipped_chunks in unzipped_stream:

        chunk_file = BytesIteratorIO(unzipped_chunks)
        reader = read_function(chunk_file, **reader_kwargs)
        path = base_path + file_name.decode()

        pa.dataset.write_dataset(
            data=reader,
            base_dir=path,
            filesystem=s3_fs,
            format="parquet",
            max_rows_per_group=max_row_per_group,
            max_rows_per_file=max_row_per_file,
            existing_data_behavior="overwrite_or_ignore",
        )

        files_processed += 1
        total_bytes_processed += file_size

        logger.debug(
            f"File {file_name.decode()} saved to s3://{path}, the file size is {file_size}."
        )

    logger.info(
        f"Unarchiving complete. Processed {files_processed} files, totaling {total_bytes_processed}."
    )
