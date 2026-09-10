import os
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.json as pa_json


def _build_csv_args(schema: pa.Schema) -> dict:
    return {
        "convert_options": pa_csv.ConvertOptions(
            default_column_type=pa.string(),
            column_types=schema,
        )
    }


def _build_json_args(schema: pa.Schema) -> dict:
    return {
        "parse_options": pa_json.ParseOptions(
            explicit_schema=schema,
            unexpected_field_behavior="infer",
        )
    }


READERS = {
    "csv": {"reader": pa_csv.open_csv, "args_builder": _build_csv_args},
    "json": {"reader": pa_json.open_json, "args_builder": _build_json_args},
}


def get_reader(file_name: str, expected_schema: pa.Schema, readers: dict):
    """Returns the read function and the args for it."""
    file_extension = os.path.splitext(file_name)[1].lstrip(".")

    if file_extension not in readers:
        raise ValueError(
            f"The file reader was not provided. File: {file_name}; Fetched extension: {file_extension}"
        )

    reader_data = readers[file_extension]
    reader_args = reader_data["args_builder"](expected_schema)

    return reader_data["reader"], reader_args
