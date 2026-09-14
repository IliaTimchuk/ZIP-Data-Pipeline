import os
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.json as pa_json
from typing import Iterator

import settings.pipeline_config as conf


class UnsupportedFileExtensionError(Exception):
    """Raised when a file inside the ZIP has an extension we don't know how to parse."""


def _cast_struct(col: pa.Array):
    """
    Casts all the data inside the struct type to strings. If nested structs are detected,
    they are processed with recursion
    """
    pass


def cast_batch_to_string(
    chunk: pa.RecordBatchReader, target_schema: pa.Schema
) -> pa.RecordBatch:
    """Casts all non-nested fields in a single batch to strings."""
    arrays = (
        _cast_struct(col) if pa.types.is_nested(col.type) else col.cast(pa.string())
        for col in chunk.columns
    )
    yield pa.RecordBatch.from_arrays(arrays, schema=target_schema)


def open_string_json(
    json_iterator: Iterator[bytes], expected_schema: pa.Schema
) -> tuple[pa.RecordBatchReader, str]:
    """
    Wraps pyarrow.open_json, casting all data to strings.
    pyarrow.open_json documentation:
        https://arrow.apache.org/docs/python/generated/pyarrow.json.open_json.html
    """

    options = pa_json.ParseOptions(
        explicit_schema=expected_schema,
        unexpected_field_behavior="infer",
    )

    record_batch_reader = pa_json.open_json(
        file_path=json_iterator, parse_options=options
    )

    if record_batch_reader.schema != expected_schema:
        unverified_batch_reader = cast_batch_to_string(record_batch_reader)
        return unverified_batch_reader, conf.UNVERIFIED_PREFIX

    return record_batch_reader, conf.VERIFIED_PREFIX


def open_string_csv(
    file_source: Iterator[bytes], expected_schema: pa.Schema
) -> tuple[pa.RecordBatchReader, str]:
    options = pa_csv.ConvertOptions(
        default_column_type=pa.string(),
        column_types=expected_schema,
    )

    record_batch_reader = pa_csv.open_csv(file_source, convert_options=options)

    if record_batch_reader.schema != expected_schema:
        return record_batch_reader, conf.UNVERIFIED_PREFIX

    return record_batch_reader, conf.VERIFIED_PREFIX


def open_file_like(
    file_iterator: Iterator[bytes], file_name: str, expected_schema: pa.Schema
):
    extension = os.path.splitext(file_name)[1].lstrip(".").lower()

    if extension == "csv":
        return open_string_csv(file_iterator, expected_schema)

    if extension == "json":
        return open_string_json(file_iterator, expected_schema)

    raise UnsupportedFileExtensionError(
        f"Unsupported file extension: {extension}, file: {file_name}"
    )
