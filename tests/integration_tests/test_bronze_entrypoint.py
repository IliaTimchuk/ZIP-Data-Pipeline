import io
import os
import zipfile
import pytest
import pyarrow as pa
import pyarrow.dataset as pa_dataset
import settings.pipeline_config as conf
import pyarrow.fs as fs
from unittest.mock import patch
from moto.server import ThreadedMotoServer

from src.bronze.entrypoint import main

SOURCE_BUCKET = "landing"
DATASET_NAME = "test-dataset"
DATASET_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string()),
        pa.field("name", pa.string()),
        pa.field("balance", pa.string()),
    ]
)
FILE_NAME = "file.zip"
DESTINATION_BUCKET = "bronze"
SOURCE_KEY = f"test_data/{DATASET_NAME}/ingest_date=2026-01-01/{FILE_NAME}"
BRONZE_KEY_BASE = f"{DESTINATION_BUCKET}/test_data/{DATASET_NAME}/ingest_date=2026-01-01/zip_name=file"


@pytest.fixture
def moto_server():
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0)
    server.start()
    host, port = server.get_host_and_port()
    endpoint_url = f"http://{host}:{port}"
    yield endpoint_url
    server.stop()


@pytest.fixture
def aws_credentials(monkeypatch, moto_server):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def s3fs(aws_credentials, moto_server):
    client = fs.S3FileSystem(endpoint_override=moto_server, allow_bucket_creation=True)
    client.create_dir(SOURCE_BUCKET)
    client.create_dir(DESTINATION_BUCKET)
    yield client


@pytest.fixture
def bronze_env(monkeypatch, moto_server):
    monkeypatch.setenv("LANDING_BUCKET", SOURCE_BUCKET)
    monkeypatch.setenv("LANDING_KEY", SOURCE_KEY)
    monkeypatch.setenv("DESTINATION_BUCKET", DESTINATION_BUCKET)
    monkeypatch.setenv("DATASET_NAME", DATASET_NAME)
    monkeypatch.setenv("AWS_ENDPOINT", moto_server)
    monkeypatch.setenv("_DAG_RUN_ID", "scheduled__2026-08-20T12:00:00+00:00")


@pytest.fixture
def make_zip():
    def _make_zip(files: dict[str, str | bytes]) -> bytes:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for name, data in files.items():
                zf.writestr(name, data)
        return buffer.getvalue()

    return _make_zip


@patch.dict(
    "settings.schemas.bronze_schemas.bronze_schemas", {DATASET_NAME: DATASET_SCHEMA}
)
def test_entrypoint_integration(s3fs, make_zip, bronze_env):
    test_file = make_zip(
        files={
            "test.csv": "id,name,balance\n1,Alice,10.5\n2,Bob,20.0\n",
            "test.json": '[{"id": 3, "name": "Carol"}]',
        }
    )

    with s3fs.open_output_stream(f"{SOURCE_BUCKET}/{SOURCE_KEY}") as f:
        f.write(test_file)

    main()

    expected_csv_key = f"{BRONZE_KEY_BASE}/schema_status={conf.VERIFIED_PREFIX}/test.csv_part-0.parquet"
    expected_json_key = f"{BRONZE_KEY_BASE}/schema_status={conf.UNVERIFIED_PREFIX}/test.json_part-0.parquet"
    expected_success_key = f"{BRONZE_KEY_BASE}/{conf.BRONZE_SUCCESS_MARKER}"
    
    assert s3fs.get_file_info(expected_csv_key).type == fs.FileType.File
    assert s3fs.get_file_info(expected_json_key).type == fs.FileType.File
    assert s3fs.get_file_info(expected_success_key).type == fs.FileType.File

    csv_table = pa_dataset.dataset(expected_csv_key, filesystem=s3fs).to_table()

    assert csv_table.column_names == [
        "id",
        "name",
        "balance",
        "_bronze_processed_at",
        "_dag_run_id",
        "_zip_file_name",
        "_source_file_name",
        "_validation_status",
    ]
    assert csv_table.column("_dag_run_id").to_pylist() == [
        "scheduled__2026-08-20T12:00:00+00:00",
        "scheduled__2026-08-20T12:00:00+00:00",
    ]
    assert csv_table.column("_zip_file_name").to_pylist() == ["file.zip", "file.zip"]