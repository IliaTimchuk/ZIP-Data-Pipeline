import logging
import pyarrow as pa
import pyarrow.dataset as pa_dataset
import pyarrow.fs as fs
from stream_unzip import stream_unzip
from typing import Iterator, Callable

from scripts.bronze.read_dataset_schema import resolve_prefix_by_schema
from scripts.utils.build_layer_key import build_bronze_key
from scripts.bronze.io_wrapper import BytesIteratorIO

logger = logging.getLogger(__name__)


def get_unarchived_stream(
    zip_iterator: Iterator[bytes],
    read_func: Callable[..., pa.RecordBatchReader],
    read_func_args: dict = None,
    unarchived_chunk_size: int = 16 * 1024 * 1024,
) -> Iterator[tuple[str, int, pa.RecordBatchReader]]:
    """
    Initializes an unarchived stream, creating a wrapper around a zip
    iterator that unarchives a file in chunks.

    This function uses the `stream_unzip` package to handle unarchivation
    in chunks: https://stream-unzip.docs.trade.gov.uk/

    Args:
        zip_iterator: the iterator over a zip file.
        read_func: the pyarrow function that is used to read raw decompressed
            bytes (file-like). Consider, that some functions read the entire
            file to the buffer, that kills chunk-reading (e.g. read_csv).
        reader_args: the args for read_func (if args are not provided for the
            reader, the args will be set to defaults).
        unarchived_chunk_size: how many bytes to fetch from zip_iterator before
            attempting to process them.
     Yields:
        tuple[str, int, pa.RecordBatchReader]: for each file inside the zip,
            a tuple of:
            - file_name: the name of the file inside the zip archive.
            - file_size: the uncompressed size of that file in bytes, as
                reported by the zip's local file header.
            - reader: a RecordBatchReader over that file's decompressed
                content, produced by read_func. Readers must be consumed before
                moving to the next yielded file.
    """

    unzipped_stream = stream_unzip(zip_iterator, chunk_size=unarchived_chunk_size)

    if not read_func_args:
        logger.info("The read_func_args were not specified, setting to defaults.")
    read_func_args = read_func_args or {}

    for file_name, file_size, unzipped_chunks in unzipped_stream:
        chunk = BytesIteratorIO(unzipped_chunks)
        file_name = file_name.decode()

        with read_func(chunk, **read_func_args) as reader:
            yield file_name, file_size, reader


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
        actual_schema = [
            col for col in reader.schema.names if col not in metadata_columns
        ]

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
            "Schema: %s.",
            file_name,
            bucket,
            key,
        )
