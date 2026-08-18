import logging
import pyarrow as pa
import pyarrow.fs as fs
import posixpath
from stream_unzip import stream_unzip
from typing import Iterator, Callable
from include.bronze.utils.io_wrapper import BytesIteratorIO
from include.bronze.utils.s3_key_utils import (
    resolve_prefix_by_schema,
    update_object_key,
)

logger = logging.getLogger(__name__)


def _get_read_context(
    file_name: str,
    readers: dict[str, pa.RecordBatchReader],
    readers_args: dict[str, dict] | None,
) -> tuple[pa.RecordBatchReader, dict]:
    """
    Extracts the reader and reader args for the file_name extension, so
    file_name must contain the extension (e.g. .csv, .json)
    """

    file_extension = posixpath.splitext(file_name)[1].lstrip(".")

    if not file_extension:
        raise ValueError(f"The file {file_name} doesn't contain a file extension.")

    if file_extension not in readers:
        raise ValueError(f'The file reader for "{file_extension}" was not provided.')

    reader = readers[file_extension]
    reader_args = (readers_args or {}).get(file_extension, dict())

    if not reader_args:
        logger.warning(
            "The %s args were not specified, setting to defaults.", file_extension
        )

    return reader, reader_args


def get_unarchived_stream(
    zip_iterator: Iterator[bytes],
    readers: dict[str, Callable[..., pa.RecordBatchReader]],
    readers_args: dict[str, dict] = None,
    unarchived_chunk_size: int = 16 * 1024 * 1024,
    zip_file_password: bytes = None,
) -> Iterator[tuple[bytes, int, pa.RecordBatchReader]]:
    """
    Initializes an unarchived stream, creating a wrapper around a zip
    iterator that unarchives compressed files in chunks. It uses functions
    from the readers dictionary to read raw unarchived chunks, choosing a
    function based on the file extension.

    This function uses the `stream_unzip` package to handle unarchivation
    in chunks: https://stream-unzip.docs.trade.gov.uk/

    Args:
        readers: the dictionary of file readers for each file extension a
            zip file.
        readers_args: the dictionary of args for readers that are provided
            (if args are not provided for the reader, the args will be set
            to defaults).
    """

    unzipped_stream = stream_unzip(
        zip_iterator, password=zip_file_password, chunk_size=unarchived_chunk_size
    )

    for file_name, file_size, unzipped_chunks in unzipped_stream:
        chunk = BytesIteratorIO(unzipped_chunks)
        file_name = file_name.decode()

        read_func, reader_args = _get_read_context(file_name, readers, readers_args)

        with read_func(chunk, **reader_args) as reader:
            yield file_name, file_size, reader


def _add_columns(chunk: pa.RecordBatch, columns_shape: dict[str, pa.Scalar]):
    """Creates columns from columns_shape and appends them to the batch"""
    n = chunk.num_rows
    for column_name, column_value in columns_shape.items():
        column_data = pa.repeat(column_value, n)
        chunk = chunk.append_column(column_name, column_data)
    return chunk


def add_columns_to_unarchived_stream(
    unarchived_stream: Iterator[tuple[str, int, pa.RecordBatchReader]],
    columns_shape: dict[str, pa.Scalar],
) -> Iterator[tuple[str, int, pa.RecordBatchReader]]:
    """
    Creates a wrapper to append columns to the unarchived stream building them
    based on columns_shape.

    Args:
        unarchived_stream: (file_name, file_size, reader) tuples.
        reader: RecordBatchReader instance that references to the data.
        columns_shape: the dictionary that is used to create columns.
    """
    for file_name, file_size, reader in unarchived_stream:
        current_schema = reader.schema
        new_schema = pa.schema(
            list(current_schema)
            + [pa.field(name, scalar.type) for name, scalar in columns_shape.items()]
        )

        batches_gen = (_add_columns(chunk, columns_shape) for chunk in reader)

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
) -> None:
    """
    Consumes chunks from unarchived_stream and uploads them as Parquet to S3.
    While consuming, it checks schemas against
    """
    for file_name, file_size, reader in unarchived_stream:

        prefix = resolve_prefix_by_schema(expected_schema, reader.schema.names)
        destination_key = update_object_key(bucket, source_key, file_name, prefix)

        pa.dataset.write_dataset(
            data=reader,
            base_dir=destination_key,
            filesystem=s3fs,
            format="parquet",
        )

        logger.info(
            "File %s was successfully unarchived and uploaded to %s as Parquet.",
            file_name,
            bucket,
        )
