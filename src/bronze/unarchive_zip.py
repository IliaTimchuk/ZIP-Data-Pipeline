import logging
import pyarrow as pa
import pyarrow.dataset as pa_dataset
import pyarrow.fs as fs
from stream_unzip import stream_unzip
from collections import deque
from typing import Iterator

from src.bronze.schema_validation import resolve_prefix_by_schema
from src.bronze.utils.readers import get_reader, READERS
from src.utils.build_layer_key import build_bronze_key
from src.bronze.utils.io_wrapper import BytesIteratorIO

logger = logging.getLogger(__name__)


def _get_reader_and_args(
    file_name: str, schema: pa.Schema
) -> tuple[callable, dict] | None:
    """Returns the PyArrow reader function and options for a given file name."""
    extension = os.path.splitext(file_name)[1].lstrip(".").lower()

    if extension == "csv":
        opts = pa_csv.ConvertOptions(
            default_column_type=pa.string(),
            column_types=schema,
        )
        return pa_csv.open_csv, {"convert_options": opts}

    if extension == "json":
        opts = pa_json.ParseOptions(
            explicit_schema=schema,
            unexpected_field_behavior="infer",
        )
        return pa_json.open_json, {"parse_options": opts}

    return None


def _target_schema(schema: pa.Schema) -> pa.Schema:
    return pa.schema([
        field if pa.types.is_nested(field.type) else pa.field(field.name, pa.string())
        for field in schema
    ])


def _cast_batch_to_string(chunk: pa.RecordBatch, target_schema: pa.Schema) -> pa.RecordBatch:
    """Casts all non-nested fields in a single batch to strings."""
    arrays = [
        col if pa.types.is_nested(col.type) else col.cast(pa.string())
        for col in chunk.columns
    ]
    return pa.RecordBatch.from_arrays(arrays, schema=target_schema)


def get_unarchived_stream(
    zip_iterator: Iterator[bytes],
    expected_schema: pa.Schema,
    unarchived_chunk_size: int = 16 * 1024 * 1024,
) -> Iterator[tuple[str, int, pa.RecordBatchReader]]:
    """
    Initializes an unarchived stream, creating a wrapper around a ZIP
    iterator that unarchives files inside the ZIP archive in chunks.

    This function uses the `stream_unzip` package to handle unarchivation
    in chunks: https://stream-unzip.docs.trade.gov.uk/

    Args:
        zip_iterator: the iterator over a ZIP file.
        expected_schema: the expected schema for files inside the ZIP archive.
        unarchived_chunk_size: how many bytes to fetch from zip_iterator before
            attempting to process them.
     Yields:
        tuple[str, int, pa.RecordBatchReader]: for each file inside the ZIP,
            a tuple of:
            - file_name: the name of the file inside the ZIP archive.
            - file_size: the uncompressed size of that file in bytes, as
                reported by the ZIP's local file header.
            - reader: a RecordBatchReader over that file's decompressed
                content, produced by read_func. Readers must be consumed before
                moving to the next yielded file.
    """
    unzipped_stream = stream_unzip(zip_iterator, chunk_size=unarchived_chunk_size)

    for file_name, file_size, unzipped_iterator in unzipped_stream:
        file_name = file_name.decode()
        read_func, read_func_args = _get_reader_and_args(
            file_name=file_name, expected_schema=expected_schema
        )
        if read_func:
            chunk = BytesIteratorIO(unzipped_iterator)

            if not read_func_args:
                logger.info("The read function's arguments weren not specified. Defult values are used.")

            with read_func(chunk, **read_func_args) as reader:
                string_reader = (_cast_batch_to_string(chunk) for chunk in reader)
                yield file_name, file_size,  pa.RecordBatchReader.from_batches(string_reader)

        else:
            logger.warning("The read function was not specified for %s, skipping.", file_name)
            deque(unzipped_iterator, maxlen=0)
            

def _add_columns(chunk: pa.RecordBatch, columns_template: dict[str, pa.Scalar]):
    """Creates columns from columns_shape and appends them to the batch"""
    n = chunk.num_rows
    for column_name, column_value in columns_template.items():
        column_data = pa.repeat(column_value, n)
        chunk = chunk.append_column(column_name, column_data)
    return chunk


def add_columns_to_unarchived_stream(
    unarchived_stream: Iterator[tuple[str, int, pa.RecordBatchReader]],
    columns_shape: dict[str, pa.Scalar],
    append_file_name: bool = False,
) -> Iterator[tuple[str, int, pa.RecordBatchReader]]:
    """
    Creates a wrapper to append columns to the unarchived stream building them
    based on columns_shape.

    Args:
        unarchived_stream: (file_name, file_size, reader) tuples.
        reader: RecordBatchReader instance that references to the data.
        columns_shape: the dictionary that is used to create columns.
        append_file_name: if True, appends a `_source_file_name` column
            containing the name of the file from the zip file
    """
    for file_name, file_size, reader in unarchived_stream:
        current_schema = reader.schema
        columns_shape_copy = columns_shape.copy()

        if append_file_name:
            columns_shape_copy["_source_file_name"] = pa.scalar(file_name, pa.string())

        new_schema = pa.schema(
            list(current_schema)
            + [
                pa.field(name, scalar.type)
                for name, scalar in columns_shape_copy.items()
            ]
        )

        batches_gen = (_add_columns(chunk, columns_shape_copy) for chunk in reader)

        reader_with_metadata = pa.RecordBatchReader.from_batches(
            schema=new_schema, batches=batches_gen
        )
        yield file_name, file_size, reader_with_metadata


def upload_unarchived_zip_stream_to_s3(
    unarchived_stream: Iterator[tuple[bytes, int, pa.RecordBatchReader]],
    s3fs: fs.S3FileSystem,
    bucket: str,
    source_key: str,
    expected_schema: list[str],
    metadata_columns: list[str] = None,
) -> None:
    """
    Consumes chunks from unarchived_stream and uploads them as Parquet to S3.
    It also checks the actual file schema against expected_schema. If the schemas
    don't match, the key will be built with the _UNVERIFIED (config/bronze/
    bronze_config.py) prefix, otherwise _VERIFIED.
    """

    metadata_columns = set(metadata_columns or [])

    for file_name, file_size, reader in unarchived_stream:
        actual_schema = pa.schema(
            [col for col in reader.schema if col not in metadata_columns]
        )

        validation_prefix = resolve_prefix_by_schema(expected_schema, actual_schema)

        key = build_bronze_key(
            landing_key=source_key,
            validation_prefix=validation_prefix,
        )

        pa_dataset.write_dataset(
            data=reader,
            base_dir=f"{bucket}/{key}",
            filesystem=s3fs,
            format="parquet",
        )

        logger.info(
            "File %s was successfully unarchived and uploaded to %s as Parquet."
            "Key: %s.",
            file_name,
            bucket,
            key,
        )
