import logging
import pyarrow as pa
import pyarrow.dataset as pa_dataset
import pyarrow.fs as fs
from stream_unzip import stream_unzip
from collections import deque
from typing import Iterator

import settings.pipeline_config as conf
import src.bronze.readers as readers
from src.utils.build_layer_key import (
    build_bronze_key,
    build_bronze_validation_status_key,
)
from src.bronze.io_wrapper import BytesIteratorIO

logger = logging.getLogger(__name__)


def get_unarchived_stream(
    zip_iterator: Iterator[bytes],
    expected_schema: pa.Schema,
    unarchived_chunk_size: int = 16 * 1024 * 1024,
) -> Iterator[tuple[str, int, pa.RecordBatchReader]]:
    """
    Initializes an unarchived stream, creating a wrapper around a ZIP
    iterator that unarchives files inside the ZIP archive in chunks.

    Processes a ZIP archive in chunks without loading the full archive into
    memory. Each decompressed file is converted to a file-like stream, validated
    against the expected PyArrow schema, and converted into a RecordBatchReader.
    Files with unsupported extensions are skipped. Currently, supported files
    are CSV and JSON.

    Args:
        zip_iterator: the iterator over a ZIP file.
        expected_schema: the expected pyarrow.schema object to validate decompressed
            files against.
        unarchived_chunk_size: how many bytes to fetch from zip_iterator before
            attempting to process them.
     Yields:
        tuple[str, pa.RecordBatchReader, str]: for each file inside the ZIP,
            a tuple of:
            - file_name: the decoded name of the file inside the ZIP archive.
            - record_batch_reader: a RecordBatchReader over each decompressed file.
                Must be consumed.
            - validation_status: whether the expected_schema matches the actual file
                schema.
    """
    unzipped_stream = stream_unzip(zip_iterator, chunk_size=unarchived_chunk_size)

    for file_name, file_size, unzipped_iterator in unzipped_stream:
        file_name = file_name.decode()

        try:
            file_like_iterator = BytesIteratorIO(unzipped_iterator)

            with readers.open_file_like(
                file_like_iterator, file_name
            ) as record_batch_reader:

                validation_status = readers.validate_schema(
                    record_batch_reader, expected_schema
                )
                yield file_name, record_batch_reader, validation_status

        except (
            readers.UnsupportedFileExtensionError,
            readers.EmptyFileError,
        ) as e:
            logger.warning("Skipping the file inside the ZIP file: %s", e)
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
    append_file_validation_status: bool = False,
) -> Iterator[tuple[str, int, pa.RecordBatchReader]]:
    """
    Creates a wrapper to append columns to the unarchived stream, building them
    based on columns_shape.

    Args:
        unarchived_stream: (file_name, record_batch_reader, validation_status)
            tuples.
        columns_shape: the dictionary {column_name: pyarrow.scalar} that is used
            to build columns.
        append_file_name: wheter to append a _source_file_name column containing
            the name of the source file.
        append_file_validation_status: whether to append a schema validation status
            column.
    """
    for file_name, record_batch_reader, validation_status in unarchived_stream:
        current_schema = record_batch_reader.schema
        columns_shape_copy = columns_shape.copy()

        if append_file_name:
            columns_shape_copy["_source_file_name"] = pa.scalar(file_name, pa.string())

        if append_file_validation_status:
            columns_shape_copy["_validation_status"] = pa.scalar(
                validation_status, pa.string()
            )

        new_schema = pa.schema(
            list(current_schema)
            + [
                pa.field(name, scalar.type)
                for name, scalar in columns_shape_copy.items()
            ]
        )

        batches_gen = (
            _add_columns(chunk, columns_shape_copy) for chunk in record_batch_reader
        )

        reader_with_metadata = pa.RecordBatchReader.from_batches(
            schema=new_schema, batches=batches_gen
        )
        yield file_name, reader_with_metadata, validation_status


def _clean_bronze_key(s3fs: fs.S3FileSystem, bronze_key: str) -> None:
    """
    Deletes everything under the bronze key of a ZIP. The _SUCCESS marker 
    is deleted first. 
    """
    success_key = f"{bronze_key}/{conf.BRONZE_SUCCESS_MARKER}"
    if s3fs.get_file_info(success_key).type == fs.FileType.File:
        s3fs.delete_file(success_key)
    
    try:
        s3fs.delete_dir_contents(bronze_key)
    except FileNotFoundError:
        pass
    logger.info("Cleaned the bronze key before writing: %s", bronze_key)


def upload_unarchived_zip_stream_to_s3(
    unarchived_stream: Iterator[tuple[str, int, pa.RecordBatchReader]],
    s3fs: fs.S3FileSystem,
    bucket: str,
    source_key: str,
    max_rows_per_file: int = conf.PARQUET_MAX_ROWS_PER_FILE,
    max_rows_per_group: int = conf.PARQUET_MAX_ROWS_PER_GROUP,
    min_rows_per_group: int = conf.PARQUET_MIN_ROWS_PER_GROUP,
) -> None:
    """
    Consumes chunks from unarchived_stream and uploads them as Parquet to S3:
        {bronze_key}/schema_status={status}/{file_name}_part-{i}.parquet

    The bronze key is cleaned before writing, so re-runs never produce duplicates.
    Writes an empty {bronze_key}/_SUCCESS marker for the processed ZIP file if the
    transformation is fully completed.

    Args:
        max_rows_per_file: the maximum number of rows in a single Parquet file.
        max_rows_per_group: the maximum number of rows in a single row group.
        min_rows_per_group: the minimum number of rows buffered before a row group
            is written, which prevents tiny row groups.
    """
    bronze_key = f"{bucket}/{build_bronze_key(source_key)}"
    _clean_bronze_key(s3fs, bronze_key)

    uploaded_files_count = 0

    for file_name, record_batch_reader, validation_status in unarchived_stream:
        flat_file_name = file_name.replace("/", "__")
        key = f"{bucket}/{build_bronze_validation_status_key(source_key, validation_status)}"

        pa_dataset.write_dataset(
            data=record_batch_reader,
            base_dir=key,
            filesystem=s3fs,
            format="parquet",
            basename_template=flat_file_name + "_part-{i}.parquet",
            existing_data_behavior="overwrite_or_ignore",
            max_rows_per_file=max_rows_per_file,
            max_rows_per_group=max_rows_per_group,
            min_rows_per_group=min_rows_per_group,
        )

        logger.info(
            "File %s was successfully unarchived and uploaded as Parquet. Key: %s.",
            file_name,
            key,
        )
        uploaded_files_count += 1

    if uploaded_files_count == 0:
        logger.warning(
            "The ZIP %s has no files with data. Nothing was uploaded to %s.",
            source_key,
            bucket,
        )
        return

    success_key = f"{bronze_key}/{conf.BRONZE_SUCCESS_MARKER}"

    s3fs.open_output_stream(success_key).close()

    logger.info(
        "Marked the ZIP as fully processed: %s, uploaded files: %s",
        success_key,
        uploaded_files_count,
    )
