import io
import zipfile
import pytest
import pyarrow as pa
import pyarrow.dataset as pa_dataset
import pyarrow.fs as fs

import src.bronze.unarchive_zip as unzip
import settings.pipeline_config as conf


def build_zip_stream(files: dict[str, str | bytes], chunk_size: int = 16):
    """Yields chunks of an in-memory ZIP archive built from the given files."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in files.items():
            zf.writestr(name, data)

    zip_bytes = buffer.getvalue()
    return (zip_bytes[i : i + chunk_size] for i in range(0, len(zip_bytes), chunk_size))


def consume_stream(stream):
    """Consumes the stream and returns a dict {filename: (pyarrow.Table, status)}."""
    return {
        file_name: (reader.read_all(), validation_status)
        for file_name, reader, validation_status in stream
    }


def dummy_record_batch_reader(
    ids: list[str], schema: pa.Schema
) -> pa.RecordBatchReader:
    """Creates a dummy RecordBatchReader for testing wrappers."""
    batch = pa.record_batch({"id": ids}, schema=schema)
    return pa.RecordBatchReader.from_batches(schema, [batch])


SCHEMA = pa.schema([("id", pa.string())])


# get_unarchived_stream


def test_get_unarchived_stream_dispatches_and_skips():
    files = {
        "valid.csv": "id\n1\n",
        "valid.json": '[{"id": 1}]',
        "unsupported.txt": "irrelevant data",
        "empty.csv": "",
        "empty.json": "",
    }
    expected_result_files = ["valid.csv", "valid.json"]
    zip_iter = build_zip_stream(files)

    results = consume_stream(unzip.get_unarchived_stream(zip_iter, SCHEMA))

    assert list(results.keys()) == expected_result_files

    for file_name in expected_result_files:
        table, status = results[file_name]
        assert table.to_pydict() == {"id": ["1"]}
        assert status == conf.VERIFIED_PREFIX


def test_get_unarchived_stream_marks_schema_mismatch_as_unverified():
    files = {
        "test.json": '[{"id": 1, "new_col": "data"}]',
        "test.csv": "id,new_col\n1,data\n",
    }
    zip_iter = build_zip_stream(files)

    results = consume_stream(unzip.get_unarchived_stream(zip_iter, SCHEMA))
    for file_name in files:
        _, status = results[file_name]
        assert status == conf.UNVERIFIED_PREFIX


@pytest.mark.parametrize(
    "file_name, content, expected_error",
    [
        pytest.param("test.json", '[{"id": 1}', ValueError, id="json_truncated"),
        pytest.param("test.csv", "id\n1,2\n", pa.ArrowInvalid, id="csv_extra_column"),
    ],
)
def test_get_unarchived_stream_raises_on_malformed_file(
    file_name, content, expected_error
):
    zip_iter = build_zip_stream({file_name: content})
    with pytest.raises(expected_error):
        consume_stream(unzip.get_unarchived_stream(zip_iter, SCHEMA))


# _add_columns & add_columns_to_unarchived_stream


def test_add_columns_builds_columns_and_appends_them():
    batch = pa.RecordBatch.from_pydict({"id": ["1", "2"]}, schema=SCHEMA)

    scalars = {
        "file_name": pa.scalar("test.csv", pa.string()),
        "run_id": pa.scalar(1, pa.int16()),
    }

    result = unzip._add_columns(batch, scalars)

    assert result.schema.names == ["id", "file_name", "run_id"]
    assert result.num_rows == 2
    assert result.column("file_name").to_pylist() == ["test.csv", "test.csv"]


@pytest.mark.parametrize(
    "append_name, append_status, expected_cols",
    [
        (False, False, ["id", "run_id"]),
        (True, False, ["id", "run_id", "_source_file_name"]),
        (False, True, ["id", "run_id", "_validation_status"]),
        (True, True, ["id", "run_id", "_source_file_name", "_validation_status"]),
    ],
    ids=["none", "name_only", "status_only", "both"],
)
def test_add_columns_to_unarchived_stream_appends_columns(
    append_name, append_status, expected_cols
):
    stream = [
        ("a.csv", dummy_record_batch_reader(["1", "2"], SCHEMA), conf.VERIFIED_PREFIX)
    ]
    columns_shape = {"run_id": pa.scalar(1, pa.int16())}

    result_stream = unzip.add_columns_to_unarchived_stream(
        stream, columns_shape, append_name, append_status
    )
    results = consume_stream(result_stream)

    table, _ = results["a.csv"]
    assert table.column_names == expected_cols
    assert table.column("run_id").to_pylist() == [1, 1]


def test_add_columns_to_unarchived_stream_keeps_values_per_file():
    stream = [
        ("a.csv", dummy_record_batch_reader(["1"], SCHEMA), conf.VERIFIED_PREFIX),
        ("b.csv", dummy_record_batch_reader(["2"], SCHEMA), conf.UNVERIFIED_PREFIX),
    ]
    columns_shape = {"run_id": pa.scalar(1, pa.int16())}

    result_stream = unzip.add_columns_to_unarchived_stream(
        stream, columns_shape, True, True
    )
    results = consume_stream(result_stream)

    table_a, _ = results["a.csv"]
    assert table_a.column("_source_file_name").to_pylist() == ["a.csv"]
    assert table_a.column("_validation_status").to_pylist() == [conf.VERIFIED_PREFIX]

    table_b, _ = results["b.csv"]
    assert table_b.column("_source_file_name").to_pylist() == ["b.csv"]
    assert table_b.column("_validation_status").to_pylist() == [conf.UNVERIFIED_PREFIX]


def test_add_columns_to_unarchived_stream_does_not_read_ahead():
    pulled = []

    def batches():
        pulled.append("batch_read")
        yield pa.record_batch({"id": ["1"]}, schema=SCHEMA)

    source = pa.RecordBatchReader.from_batches(SCHEMA, batches())
    stream = [("a.csv", source, conf.VERIFIED_PREFIX)]

    result_stream = list(unzip.add_columns_to_unarchived_stream(stream, {}))
    assert pulled == []

    _, reader, _ = result_stream[0]
    reader.read_all()
    assert pulled == ["batch_read"]



# _clean_bronze_key


def test_clean_bronze_key(tmp_path):
    local_fs = fs.LocalFileSystem()
    bronze_dir = f"{tmp_path}/bronze_key"
    success_marker = f"{bronze_dir}/{conf.BRONZE_SUCCESS_MARKER}"
    old_parquet = f"{bronze_dir}/old_data.parquet"
    other_zip_parquet = f"{tmp_path}/other_bronze_key/data.parquet"

    local_fs.create_dir(bronze_dir)
    local_fs.create_dir(f"{tmp_path}/other_bronze_key")
    local_fs.open_output_stream(success_marker).close()
    local_fs.open_output_stream(old_parquet).close()
    local_fs.open_output_stream(other_zip_parquet).close()

    unzip._clean_bronze_key(local_fs, bronze_dir)

    assert local_fs.get_file_info(success_marker).type == fs.FileType.NotFound
    assert local_fs.get_file_info(old_parquet).type == fs.FileType.NotFound
    assert local_fs.get_file_info(other_zip_parquet).type == fs.FileType.File


def test_clean_bronze_key_ignores_missing_directory(tmp_path):
    """Passes if cleaning a directory that does not exist does not raise."""
    local_fs = fs.LocalFileSystem()
    missing_dir = f"{tmp_path}/does_not_exist"

    unzip._clean_bronze_key(local_fs, missing_dir)


# upload_unarchived_zip_stream_to_s3


LANDING_KEY = "source/dataset/ingest_date=2026-01-01/archive.zip"
BRONZE_KEY = "source/dataset/ingest_date=2026-01-01/zip_name=archive"


def test_upload_unarchived_zip_stream_writes_dataset(tmp_path):
    local_fs = fs.LocalFileSystem()
    bucket = str(tmp_path)
    stream = [
        (
            "folder/data.csv",
            dummy_record_batch_reader(["1", "2"], SCHEMA),
            conf.VERIFIED_PREFIX,
        )
    ]

    unzip.upload_unarchived_zip_stream_to_s3(
        unarchived_stream=stream,
        s3fs=local_fs,
        bucket=bucket,
        source_key=LANDING_KEY,
    )

    bronze_dir = tmp_path / BRONZE_KEY

    success_marker = bronze_dir / conf.BRONZE_SUCCESS_MARKER
    assert success_marker.exists()
    assert success_marker.stat().st_size == 0

    dataset_dir = bronze_dir / f"schema_status={conf.VERIFIED_PREFIX}"
    assert dataset_dir.exists()

    parquet_files = list(dataset_dir.glob("*.parquet"))
    assert len(parquet_files) == 1
    assert parquet_files[0].name.startswith("folder__data.csv_part-")

    saved_table = pa_dataset.dataset(str(dataset_dir)).to_table()
    assert saved_table.column("id").to_pylist() == ["1", "2"]


def test_upload_unarchived_zip_stream_rerun_leaves_no_old_files(tmp_path):
    local_fs = fs.LocalFileSystem()
    bucket = str(tmp_path)
    first_run = [
        ("a.csv", dummy_record_batch_reader(["1"], SCHEMA), conf.VERIFIED_PREFIX)
    ]
    second_run = [
        ("a.csv", dummy_record_batch_reader(["1"], SCHEMA), conf.UNVERIFIED_PREFIX)
    ]

    for stream in [first_run, second_run]:
        unzip.upload_unarchived_zip_stream_to_s3(
            unarchived_stream=stream,
            s3fs=local_fs,
            bucket=bucket,
            source_key=LANDING_KEY,
        )

    bronze_dir = tmp_path / BRONZE_KEY

    assert (bronze_dir / conf.BRONZE_SUCCESS_MARKER).exists()
    assert not (bronze_dir / f"schema_status={conf.VERIFIED_PREFIX}").exists()

    parquet_files = list(
        (bronze_dir / f"schema_status={conf.UNVERIFIED_PREFIX}").glob("*.parquet")
    )
    assert len(parquet_files) == 1


def test_upload_unarchived_zip_stream_handles_empty_stream(tmp_path):
    local_fs = fs.LocalFileSystem()
    bucket = str(tmp_path)

    unzip.upload_unarchived_zip_stream_to_s3(
        unarchived_stream=iter([]),
        s3fs=local_fs,
        bucket=bucket,
        source_key=LANDING_KEY,
    )

    bronze_dir = tmp_path / BRONZE_KEY

    assert not bronze_dir.exists()
