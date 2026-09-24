# import io
# import os
# import yaml
# import zipfile
# import pytest
# import pyarrow as pa
# import pyarrow.dataset as pa_dataset
# import settings.pipeline_config as conf
# from unittest.mock import patch
# from datetime import datetime, timezone
# from pyarrow.fs import S3FileSystem
# from moto.server import ThreadedMotoServer


# SOURCE_BUCKET = "landing"
# DATASET_NAME = "test-dataset"
# DATASET_SCHEMA = ["id", "name", "score", "is_active"]
# FILE_NAME = "file.zip"
# SOURCE_KEY = f"test_data/{DATASET_NAME}/ingest_date=2026-01-01/{FILE_NAME}"
# DESTINATION_BUCKET = "bronze"


# @pytest.fixture
# def moto_server():
#     server = ThreadedMotoServer(ip_address="127.0.0.1", port=0)
#     server.start()
#     host, port = server.get_host_and_port()
#     endpoint_url = f"http://{host}:{port}"
#     yield endpoint_url
#     server.stop()


# @pytest.fixture
# def aws_credentials(monkeypatch, moto_server):
#     monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
#     monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
#     monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


# @pytest.fixture
# def bronze_env(monkeypatch, moto_server):
#     monkeypatch.setenv("LANDING_BUCKET", SOURCE_BUCKET)
#     monkeypatch.setenv("LANDING_KEY", SOURCE_KEY)
#     monkeypatch.setenv("DESTINATION_BUCKET", DESTINATION_BUCKET)
#     monkeypatch.setenv("DATASET_NAME", DATASET_NAME)
#     monkeypatch.setenv("AWS_ENDPOINT", moto_server)
#     monkeypatch.setenv("_DAG_RUN_ID", "scheduled__2026-08-20T12:00:00+00:00")


# @pytest.fixture
# def s3_client(aws_credentials, moto_server):
#     client = S3FileSystem(endpoint_override=moto_server, allow_bucket_creation=True)
#     client.create_dir(SOURCE_BUCKET)
#     client.create_dir(DESTINATION_BUCKET)
#     yield client
