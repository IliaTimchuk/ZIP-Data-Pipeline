import io
import os
import yaml
import zipfile
import pytest
import pyarrow as pa
import pyarrow.dataset as pa_dataset
import settings.pipeline_config as conf
from unittest.mock import patch
from datetime import datetime, timezone
from pyarrow.fs import S3FileSystem
from moto.server import ThreadedMotoServer

from scripts.bronze.bronze_entrypoint import main
from scripts.utils.build_layer_key import build_bronze_key

SOURCE_BUCKET = "landing"
DATASET_NAME = "test-dataset"
DATASET_SCHEMA = ["id", "name", "score", "is_active"]
FILE_NAME = "file.zip"
SOURCE_KEY = f"test_data/{DATASET_NAME}/date=2026-01-01/{FILE_NAME}"
DESTINATION_BUCKET = "bronze"


@pytest.fixture
def test_zip_file():
    def _create_zip_file(data: str, zipped_file_name: str):
        file = io.BytesIO()
        with zipfile.ZipFile(file, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(zipped_file_name, data)
        file.seek(0)
        return file

    return _create_zip_file


@pytest.fixture
def test_dataset_schemas_yaml(tmp_path):
    path = tmp_path / "test_dataset_schemas.yaml"
    with open(path, "w") as f:
        yaml.safe_dump(
            data={DATASET_NAME: {"schema": DATASET_SCHEMA}},
            stream=f,
        )
    return str(path)


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
def bronze_processed_at():
    return str(int(datetime.now().timestamp() * 1000))


@pytest.fixture
def bronze_env(monkeypatch, moto_server, bronze_processed_at):
    monkeypatch.setenv("SOURCE_BUCKET", SOURCE_BUCKET)
    monkeypatch.setenv("SOURCE_KEY", SOURCE_KEY)
    monkeypatch.setenv("DESTINATION_BUCKET", DESTINATION_BUCKET)
    monkeypatch.setenv("DATASET_NAME", DATASET_NAME)
    monkeypatch.setenv("AWS_ENDPOINT", moto_server)
    monkeypatch.setenv("_BRONZE_PROCESSED_AT", bronze_processed_at)
    monkeypatch.setenv("_DAG_RUN_ID", "scheduled__2026-08-20T12:00:00+00:00")
    monkeypatch.setenv("_ZIP_FILE_NAME", "test_file.zip")


@pytest.fixture
def s3_client(aws_credentials, moto_server):
    client = S3FileSystem(endpoint_override=moto_server, allow_bucket_creation=True)
    client.create_dir(SOURCE_BUCKET)
    client.create_dir(DESTINATION_BUCKET)
    yield client


@pytest.mark.parametrize(
    "csv_data, expected_prefix, expected_columns, expected_data",
    [
        (
            f"{','.join(DATASET_SCHEMA)}\n1,Alice,85,true\n2,Bob,92,false\n",
            conf.VERIFIED_PREFIX,
            {
                *DATASET_SCHEMA,
                "_bronze_processed_at",
                "_dag_run_id",
                "_zip_file_name",
                "_source_file_name",
            },
            {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
                "score": [85, 92],
                "is_active": [True, False],
            },
        ),
        (
            "id,name,wrong_column\n1,Alice,bad\n2,Bob,bad\n",
            conf.UNVERIFIED_PREFIX,
            {
                "id",
                "name",
                "wrong_column",
                "_bronze_processed_at",
                "_dag_run_id",
                "_zip_file_name",
                "_source_file_name",
            },
            {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
                "wrong_column": ["bad", "bad"],
            },
        ),
    ],
    ids=["verified_schema", "unverified_schema"],
)
def test_bronze_entrypoint_run(
    s3_client,
    bronze_env,
    test_dataset_schemas_yaml,
    bronze_processed_at,
    test_zip_file,
    csv_data,
    expected_prefix,
    expected_columns,
    expected_data,
):
    zip_content = test_zip_file(data=csv_data, zipped_file_name="zipped_file.csv")
    source_path = f"{SOURCE_BUCKET}/{SOURCE_KEY}"

    with s3_client.open_output_stream(source_path) as stream:
        stream.write(zip_content.read())

    with patch(
        "scripts.bronze.bronze_entrypoint.DATASET_SCHEMAS_YAML_PATH",
        new=test_dataset_schemas_yaml,
    ):
        main()

    bronze_key = build_bronze_key(SOURCE_KEY, expected_prefix)

    unarchived_parquet = pa_dataset.dataset(
        source=f"{DESTINATION_BUCKET}/{bronze_key}",
        filesystem=s3_client,
        format="parquet",
    )
    result = unarchived_parquet.to_table().to_pydict()

    assert set(result.keys()) == expected_columns
    for key, values in expected_data.items():
        assert result[key] == values

    assert result["_source_file_name"] == ["zipped_file.csv", "zipped_file.csv"]
    assert result["_zip_file_name"] == ["test_file.zip", "test_file.zip"]
    assert result["_dag_run_id"] == ["scheduled__2026-08-20T12:00:00+00:00"] * 2

    expected_timestamp = datetime.fromtimestamp(
        int(bronze_processed_at) / 1000, tz=timezone.utc
    )
    assert result["_bronze_processed_at"] == [expected_timestamp, expected_timestamp]
