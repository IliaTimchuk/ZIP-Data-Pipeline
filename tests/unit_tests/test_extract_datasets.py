import boto3
import botocore.exceptions
import os
import pytest
from unittest.mock import patch
from boto3.s3.transfer import TransferConfig
from io import BytesIO
from moto import mock_aws

from src.ingestion.upload_datasets import (
    upload_stream_to_s3,
    validate_file_size,
)

TEST_DATA = b"Hello, world!"
TEST_BUCKET = "test_bucket"
TEST_KEY = "test/key/file.txt"


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def s3_client(aws_credentials):
    """Returns a mocked S3 client and creates a dummy bucket."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=TEST_BUCKET)

        yield client


def _client_error(code: str, operation: str) -> botocore.exceptions.ClientError:
    return botocore.exceptions.ClientError(
        {"Error": {"Code": code, "Message": code}}, operation
    )


# uload_stream_to_s3


def test_upload_succeeds(s3_client, caplog):
    with caplog.at_level("INFO"):
        upload_stream_to_s3(
            fileobj=BytesIO(TEST_DATA),
            s3_client=s3_client,
            bucket=TEST_BUCKET,
            key=TEST_KEY,
        )

    obj = s3_client.get_object(Bucket=TEST_BUCKET, Key=TEST_KEY)
    assert obj["Body"].read() == TEST_DATA
    assert "successfully uploaded" in caplog.text


def test_extra_args_passed_through(s3_client):
    upload_stream_to_s3(
        fileobj=BytesIO(TEST_DATA),
        s3_client=s3_client,
        bucket=TEST_BUCKET,
        key=TEST_KEY,
        extra_args={"Metadata": {"source": "test"}},
    )

    obj = s3_client.head_object(Bucket=TEST_BUCKET, Key=TEST_KEY)
    assert obj["Metadata"] == {"source": "test"}


def test_custom_transfer_config_is_accepted(s3_client):
    data = b"A" * 50

    custom_config = TransferConfig(multipart_threshold=10, multipart_chunksize=10)

    upload_stream_to_s3(
        fileobj=BytesIO(data),
        s3_client=s3_client,
        bucket=TEST_BUCKET,
        key=TEST_KEY,
        transfer_config=custom_config,
    )

    obj = s3_client.get_object(Bucket=TEST_BUCKET, Key=TEST_KEY)
    assert obj["Body"].read() == data


def test_upload_zero_byte_file(s3_client):
    upload_stream_to_s3(
        fileobj=BytesIO(b""),
        s3_client=s3_client,
        bucket=TEST_BUCKET,
        key=TEST_KEY,
    )

    obj = s3_client.get_object(Bucket=TEST_BUCKET, Key=TEST_KEY)
    assert obj["Body"].read() == b""


def test_upload_fails_raises_ioerror(s3_client):
    non_existent_bucket = "bucket_that_does_not_exist"

    with pytest.raises(IOError) as exc_info:
        upload_stream_to_s3(
            fileobj=BytesIO(TEST_DATA),
            s3_client=s3_client,
            bucket=non_existent_bucket,
            key=TEST_KEY,
        )

    assert f"Failed to upload s3://{non_existent_bucket}/{TEST_KEY}" in str(
        exc_info.value
    )


# validate_file_size


def test_validate_file_size_matches(s3_client):
    s3_client.put_object(Bucket=TEST_BUCKET, Key=TEST_KEY, Body=TEST_DATA)

    validate_file_size(
        s3_client=s3_client,
        bucket=TEST_BUCKET,
        key=TEST_KEY,
        expected_size=len(TEST_DATA),
    )

    obj = s3_client.get_object(Bucket=TEST_BUCKET, Key=TEST_KEY)
    assert obj["Body"].read() == TEST_DATA


def test_validate_file_size_head_object_fails_raises_ioerror(s3_client):
    s3_client.put_object(Bucket=TEST_BUCKET, Key=TEST_KEY, Body=TEST_DATA)

    with patch.object(
        s3_client, "head_object", side_effect=_client_error("500", "HeadObject")
    ):
        with pytest.raises(IOError) as exc_info:
            validate_file_size(
                s3_client=s3_client,
                bucket=TEST_BUCKET,
                key=TEST_KEY,
                expected_size=len(TEST_DATA),
            )

    message = str(exc_info.value)
    assert f"s3://{TEST_BUCKET}/{TEST_KEY} could not be" in message
    assert "verified: head_object failed" in message

    obj = s3_client.get_object(Bucket=TEST_BUCKET, Key=TEST_KEY)
    assert obj["Body"].read() == TEST_DATA


def test_validate_file_size_mismatch_deletes_object_and_raises_ioerror(
    s3_client, caplog
):
    s3_client.put_object(Bucket=TEST_BUCKET, Key=TEST_KEY, Body=TEST_DATA)
    wrong_size = len(TEST_DATA) + 5

    with caplog.at_level("WARNING"):
        with pytest.raises(IOError) as exc_info:
            validate_file_size(
                s3_client=s3_client,
                bucket=TEST_BUCKET,
                key=TEST_KEY,
                expected_size=wrong_size,
            )

    message = str(exc_info.value)
    assert f"Transfer size mismatch for {TEST_KEY}" in message
    assert f"The file was deleted from {TEST_BUCKET}." in message

    with pytest.raises(botocore.exceptions.ClientError):
        s3_client.get_object(Bucket=TEST_BUCKET, Key=TEST_KEY)

    assert "Deleted corrupt object" in caplog.text


def test_validate_file_size_mismatch_when_delete_also_fails_raises_ioerror(
    s3_client, caplog
):
    s3_client.put_object(Bucket=TEST_BUCKET, Key=TEST_KEY, Body=TEST_DATA)
    wrong_size = len(TEST_DATA) + 5

    with patch.object(
        s3_client,
        "delete_object",
        side_effect=_client_error("AccessDenied", "DeleteObject"),
    ):
        with caplog.at_level("WARNING"):
            with pytest.raises(IOError) as exc_info:
                validate_file_size(
                    s3_client=s3_client,
                    bucket=TEST_BUCKET,
                    key=TEST_KEY,
                    expected_size=wrong_size,
                )

    message = str(exc_info.value)
    assert f"Transfer size mismatch for {TEST_KEY}" in message
    assert "Attempted deletion also failed; object left in place." in message

    obj = s3_client.get_object(Bucket=TEST_BUCKET, Key=TEST_KEY)
    assert obj["Body"].read() == TEST_DATA

    assert "Failed to delete corrupt object" in caplog.text
