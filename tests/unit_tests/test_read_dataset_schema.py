import pytest
import yaml
import settings.pipeline_config as conf

from scripts.bronze.read_dataset_schema import (
    resolve_prefix_by_schema,
    read_dataset_schema_from_yaml,
)

DATASET_NAME = "test-dataset"
DATASET_SCHEMA = ["id", "name", "score", "is_active"]


@pytest.fixture
def test_dataset_schemas_yaml(tmp_path):
    path = tmp_path / "test_dataset_schemas.yaml"
    with open(path, "w") as f:
        yaml.safe_dump(
            data={DATASET_NAME: {"schema": DATASET_SCHEMA}},
            stream=f,
        )
    return str(path)


def test_read_dataset_schema_from_yaml_reads_dataset_correctly(
    test_dataset_schemas_yaml,
):
    schema = read_dataset_schema_from_yaml(test_dataset_schemas_yaml, DATASET_NAME)

    assert schema == DATASET_SCHEMA


def test_read_dataset_schema_from_yaml_raises(test_dataset_schemas_yaml):
    missing_dataset = "non_existent_dataset"
    with pytest.raises(KeyError) as error:
        schema = read_dataset_schema_from_yaml(
            test_dataset_schemas_yaml, missing_dataset
        )

    error_message = str(error.value)
    assert f"No schema entry found for dataset '{missing_dataset}'" in error_message


@pytest.mark.parametrize(
    "expected, actual, expected_prefix",
    [
        (["id", "name"], ["id", "name"], conf.VERIFIED_PREFIX),
        (["id", "name"], ["id", "wrong_name"], conf.UNVERIFIED_PREFIX),
        (["id", "name"], ["name", "id"], conf.UNVERIFIED_PREFIX),
        (["id", "name"], ["id"], conf.UNVERIFIED_PREFIX),
    ],
)
def test_schema_validates_correctly(expected, actual, expected_prefix):
    prefix = resolve_prefix_by_schema(expected, actual)
    assert prefix == expected_prefix
