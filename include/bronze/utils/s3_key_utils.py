import logging
import posixpath

logger = logging.getLogger(__name__)


def resolve_prefix_by_schema(
    expected_schema: list[str], actual_schema: list[str]
) -> str:

    if expected_schema == actual_schema:
        return "_verified"

    logger.warning(
        "Schema mismatch: expected: %s, received: %s" "", expected_schema, actual_schema
    )

    return "_unverified"


def update_object_key(
    bucket: str,
    source_object_key: str,
    file_name: str,
    prefix: str,
) -> str:
    """
    Builds the destination key for a decompressed file:
    bucket/prefix/base_dir_of_source_key/file_stem
    """
    base_dir = posixpath.dirname(source_object_key)
    file_stem = posixpath.splitext(file_name)[0]
    return posixpath.join(bucket, prefix, base_dir, file_stem)




