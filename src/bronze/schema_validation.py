import logging
import pyarrow as pa
import settings.pipeline_config as conf

logger = logging.getLogger(__name__)


def resolve_prefix_by_schema(
    expected_schema: pa.Schema, actual_schema: pa.Schema
) -> str:
    if expected_schema.names == actual_schema.names:
        return conf.VERIFIED_PREFIX

    logger.warning(
        "Schema mismatch: expected: %s, received: %s" "", expected_schema, actual_schema
    )

    return conf.UNVERIFIED_PREFIX