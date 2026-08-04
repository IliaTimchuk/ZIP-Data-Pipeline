import logging
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
from mypy_boto3_s3 import S3Client
from include.utils.logging_helpers import format_file_size

logger = logging.getLogger(__name__)

DEFAULT_TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=64 * 1024 * 1024,
    multipart_chunksize=64 * 1024 * 1024,
    max_concurrency=8,
)


def upload_stream_to_s3(
    fileobj,
    s3_client: S3Client,
    bucket: str,
    key: str,
    transfer_config: TransferConfig | None = None,
    extra_args: dict | None = None,
) -> None:
    """
    Streams a file-like object to S3.

    Args:
        fileobj: A readable file-like object to upload to S3.
        s3_client: An instance of boto3 S3 client to perform the upload.
        bucket: Name of the destination S3 bucket.
        key: Key (path) under which the file will be stored in the
            bucket.
        transfer_config: An optional boto3 TransferConfig controlling
            multipart upload behavior (part size, concurrency, and the
            size threshold above which a multipart upload is used).
            Defaults to 64 MB parts, a 64 MB multipart threshold, and
            8 concurrent threads.

            Note: S3 allows a maximum of 10,000 parts per multipart
            upload. The chunk size must be chosen so that
            file_size / multipart_chunksize stays under this limit --
            the default 64 MB chunk size supports files up to roughly
            640 GB. For larger files, pass a TransferConfig with a
            larger multipart_chunksize.
        extra_args: Optional dictionary of extra arguments to pass to the
            S3 upload method (metadata, ACLs, storage class, etc). See:
            https://boto3.amazonaws.com/v1/documentation/api/latest/guide

    Raises:
        IOError: If the upload fails.
    """

    config = transfer_config or DEFAULT_TRANSFER_CONFIG

    try:
        s3_client.upload_fileobj(
            Fileobj=fileobj,
            Bucket=bucket,
            Key=key,
            Config=config,
            ExtraArgs=extra_args or {},
        )
        logger.info("The object was successfully uploaded to s3://%s/%s", bucket, key)
    except ClientError as e:
        raise IOError(f"Failed to upload s3://{bucket}/{key}: {e}") from e


def validate_file_size(s3_client: S3Client, bucket: str, key: str, expected_size: int):
    """
    Checks the actual object size and deletes it if the size mismatches.

    Args:
        s3_client: An instance of boto3 S3 client to perform the check.
        bucket: Name of the S3 bucket where the file is stored.
        key: Key (path) under which the file is stored in the bucket.

    Raises:
        IOError: If the actual size does not match the expected size,
            or if the head_object call fails
    """
    try:
        head = s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError as e:

        raise IOError(
            f"s3://{bucket}/{key} could not be " f"verified: head_object failed: {e}"
        ) from e

    actual_size = head["ContentLength"]

    if actual_size != expected_size:

        error_msg = (
            f"Transfer size mismatch for {key}: "
            f"expected {format_file_size(expected_size)}, "
            f"got {format_file_size(actual_size)}."
        )

        try:
            s3_client.delete_object(Bucket=bucket, Key=key)

            logger.warning(
                "Deleted corrupt object s3://%s/%s after size mismatch "
                "(expected %s, got %s)",
                bucket,
                key,
                format_file_size(expected_size),
                format_file_size(actual_size),
            )

            error_msg += f" The file was deleted from {bucket}."

        except ClientError as e:

            logger.warning(
                "Failed to delete corrupt object s3://%s/%s: %s",
                bucket,
                key,
                e,
            )

            error_msg += " Attempted deletion also failed; object left in place."

        raise IOError(error_msg)
