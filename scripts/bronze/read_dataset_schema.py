import logging
import yaml
import settings.pipeline_config as conf

logger = logging.getLogger(__name__)


def read_dataset_schema_from_yaml(file_path: str, dataset_name: str) -> list[str]:
    with open(file_path, "r") as f:
        schemas = yaml.safe_load(f) or {}

    dataset_data = schemas.get(dataset_name)
    if dataset_data is None:
        raise KeyError(
            f"No schema entry found for dataset '{dataset_name}' in {file_path}."
        )
    return dataset_data["schema"]


def resolve_prefix_by_schema(
    expected_schema: list[str], actual_schema: list[str]
) -> str:
    if expected_schema == actual_schema:
        return conf.VERIFIED_PREFIX

    logger.warning(
        "Schema mismatch: expected: %s, received: %s" "", expected_schema, actual_schema
    )

    return conf.UNVERIFIED_PREFIX