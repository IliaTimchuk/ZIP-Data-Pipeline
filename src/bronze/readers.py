import os
import json
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.json as pa_json
from typing import IO
import settings.pipeline_config as conf


class UnsupportedFileExtensionError(Exception):
    """Raised when a file inside the ZIP has an extension that has no readers."""


class JsonTooLargeError(Exception):
    """The size of the JSON file exceeds the maximum allowed limit."""


def _read_iterator(iterator_file_like: IO[bytes], limit=1 * 1024 * 1024):
    """Buffers the whole iterator if it is under the limit."""
    buffer = iterator_file_like.read(limit + 1)

    if len(buffer) > limit:
        raise JsonTooLargeError(f"JSON member exceeds the {limit} byte in-memory limit")

    return buffer


def open_string_json(json_iterator: IO[bytes]) -> pa.RecordBatchReader:
    """
    Reads the full JSON file into memory (up to MAX_JSON_SIZE_BYTES) and converts
    it to pyarrow.RecordBatchReader. Parses all numbers (integers and floats) as
    text strings to avoid missing digits.
    """

    raw_json = _read_iterator(json_iterator, limit=conf.MAX_JSON_SIZE_BYTES)
    json_records = json.loads(raw_json, parse_float=str, parse_int=str)

    if isinstance(json_records, dict):
        json_records = [json_records]

    table = pa.Table.from_pylist(json_records)
    record_batch_reader = table.to_reader()
    return record_batch_reader


def open_string_csv(file_source: IO[bytes]) -> pa.RecordBatchReader:
    """
    Opens a streaming reader of CSV data using configured pyarrow.csv.open_csv
    fucntion. All unexpected columns default to strings.

    pyarrow.csv.open_csv: https://arrow.apache.org/docs/python/generated/pyarrow.csv.open_csv.html
    """
    options = pa_csv.ConvertOptions(default_column_type=pa.string())

    record_batch_reader = pa_csv.open_csv(file_source, convert_options=options)
    return record_batch_reader


def validate_schema(reader: pa.RecordBatchReader, expected_schema: pa.Schema) -> str:
    """Resolves the prefix based on the reader schema matching the expected schema."""
    actual_schema = reader.schema

    if expected_schema.equals(actual_schema):
        return conf.VERIFIED_PREFIX

    return conf.UNVERIFIED_PREFIX


_READERS = {
    "csv": open_string_csv,
    "json": open_string_json,
}


def open_file_like(file_iterator: IO[bytes], file_name: str) -> pa.RecordBatchReader:
    """
    Resolves a function to open a file by file extension. Unsupported extensions
    raise UnsupportedFileExtensionError.
    """
    extension = os.path.splitext(file_name)[1].lstrip(".").lower()

    open_reader = _READERS.get(extension)
    if not open_reader:
        raise UnsupportedFileExtensionError(
            f"Unsupported file extension: {extension}, file: {file_name}"
        )

    reader = open_reader(file_iterator)
    return reader
