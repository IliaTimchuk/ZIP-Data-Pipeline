import pytest

import src.utils.build_layer_key as key

SOURCE_NAME = "test_source"
ARCHIVE_NAME = "test_file.zip"
ARCHIVE_STEM = "test_file"
DATASET_NAME = "test_dataset"
DATE = "2020-01-01"
VALIDATION_STATUS = "verified"

SOURCE_KEY = f"test/source/key/{ARCHIVE_NAME}"
LANDING_KEY = f"{SOURCE_NAME}/{DATASET_NAME}/ingest_date={DATE}/{ARCHIVE_NAME}"
BRONZE_KEY = f"{SOURCE_NAME}/{DATASET_NAME}/ingest_date={DATE}/zip_name={ARCHIVE_STEM}"
BRONZE_VALIDATION_STATUS_KEY = f"{BRONZE_KEY}/schema_status={VALIDATION_STATUS}"


def test_build_landing_key():
    test_landing_key = key.build_landing_key(
        source_name=SOURCE_NAME,
        source_key=SOURCE_KEY,
        dataset_name=DATASET_NAME,
        date=DATE,
    )
    assert test_landing_key == LANDING_KEY


def test_build_bronze_key():
    assert key.build_bronze_key(LANDING_KEY) == BRONZE_KEY


def test_build_bronze_validation_status_key():
    test_key = key.build_bronze_validation_status_key(LANDING_KEY, VALIDATION_STATUS)
    assert test_key == BRONZE_VALIDATION_STATUS_KEY
