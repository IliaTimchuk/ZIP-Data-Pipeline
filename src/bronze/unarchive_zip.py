import logging
import pyarrow as pa
import pyarrow.dataset as pa_dataset
import pyarrow.fs as fs
from stream_unzip import stream_unzip
from collections import deque
from typing import Iterator

from bronze.readers import open_file_like, UnsupportedFileExtensionError
from src.utils.build_layer_key import build_bronze_key
from src.bronze.utils.io_wrapper import BytesIteratorIO

logger = logging.getLogger(__name__)


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
            - record_batch_reader: a RecordBatchReader over that file's decompressed
                content, produced by read_func. Readers must be consumed before
                moving to the next yielded file.
            - validation_status
    """
    unzipped_stream = stream_unzip(zip_iterator, chunk_size=unarchived_chunk_size)

    for file_name, _, unzipped_iterator in unzipped_stream:
        file_name = file_name.decode()

        try:
            file_like_iterator = BytesIteratorIO(unzipped_iterator)

            with open_file_like(
                file_like_iterator, file_name, expected_schema
            ) as reader_data:
                record_batch_reader, verification_status = reader_data
                yield file_name, record_batch_reader, verification_status

        except UnsupportedFileExtensionError as e:
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
    append_file_verification_status: bool = False 
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
    for file_name, record_batch_reader, verification_status in unarchived_stream:
        current_schema = record_batch_reader.schema
        columns_shape_copy = columns_shape.copy()

        if append_file_name:
            columns_shape_copy["_source_file_name"] = pa.scalar(file_name, pa.string())
        
        if append_file_name:
            columns_shape_copy["_verification_status"] = pa.scalar(verification_status, pa.string())

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
        yield file_name, reader_with_metadata, verification_status


def upload_unarchived_zip_stream_to_s3(
    unarchived_stream: Iterator[tuple[bytes, int, pa.RecordBatchReader]],
    s3fs: fs.S3FileSystem,
    bucket: str,
    source_key: str,
) -> None:
    """
    Consumes chunks from unarchived_stream and uploads them as Parquet to S3.
    It also checks the actual file schema against expected_schema. If the schemas
    don't match, the key will be built with the _UNVERIFIED (config/bronze/
    bronze_config.py) prefix, otherwise _VERIFIED.
    """

    metadata_columns = set(metadata_columns or [])

    for file_name, record_batch_reader, verification_status in unarchived_stream:

        key = build_bronze_key(
            landing_key=source_key,
            verification_status=verification_status,
        )

        pa_dataset.write_dataset(
            data=record_batch_reader,
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
