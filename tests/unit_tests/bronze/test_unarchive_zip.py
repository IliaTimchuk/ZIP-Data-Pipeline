import io
import zipfile
import pytest
import pyarrow as pa
import pyarrow.fs as fs
from typing import Iterator
from unittest.mock import MagicMock

import src.bronze.unarchive_zip as unzip
import settings.pipeline_config as conf


@pytest.fixture
def make_zip_file():
    """
    Returns an iterator of bytes chunks over an in-memory ZIP archive built
    from the given files.
    """

    def _create_zip_file(
        files: dict[str, str | bytes], chunk_size: int = 16
    ) -> Iterator[bytes]:
        """
        Args:
            files: {file name inside the archive: file content}, written in
                insertion order.
            chunk_size: the size of the yielded chunks.
        """
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for file_name, data in files.items():
                zf.writestr(file_name, data)

        zip_bytes = buffer.getvalue()
        return (
            zip_bytes[i : i + chunk_size] for i in range(0, len(zip_bytes), chunk_size)
        )

    return _create_zip_file


def _collect(stream):
    """
    Consumes an unarchived stream, reading each reader into a pyarrow.Table.
    """
    return [
        (file_name, reader.read_all(), validation_status)
        for file_name, reader, validation_status in stream
    ]


# get_unarchive_stream


SCHEMA = pa.schema([("id", pa.string())])

FILE_CONTENTS = {
    "valid.csv": "id\n1\n",
    "valid.json": '[{"id": 1}]',
    "unsupported.txt": "irrelevant",
    "empty.csv": "",
    "empty.json": "",
}


@pytest.mark.parametrize(
    "member_names, expected_names",
    [
        pytest.param(["valid.csv"], ["valid.csv"], id="csv_alone"),
        pytest.param(["valid.json"], ["valid.json"], id="json_alone"),
        pytest.param(["unsupported.txt"], [], id="unsupported_alone"),
        pytest.param(
            ["valid.csv", "valid.json"],
            ["valid.csv", "valid.json"],
            id="csv_and_json",
        ),
        pytest.param(
            ["valid.csv", "unsupported.txt", "valid.json"],
            ["valid.csv", "valid.json"],
            id="unsupported_between_csv_and_json",
        ),
        pytest.param(["empty.csv"], [], id="csv_empty"),
        pytest.param(["empty.json"], [], id="json_empty"),
        pytest.param(
            ["empty.csv", "valid.csv", "empty.json", "valid.json"],
            ["valid.csv", "valid.json"],
            id="empty_between_valid_files",
        ),
    ],
)
def test_get_unarchived_stream_dispatches_and_skips(
    member_names, expected_names, make_zip_file
):
    files = {name: FILE_CONTENTS[name] for name in member_names}
    zip_iter = make_zip_file(files=files)

    results = _collect(unzip.get_unarchived_stream(zip_iter, SCHEMA))

    assert [name for name, _, _ in results] == expected_names
    for name, table, validation_status in results:
        assert table.to_pydict() == {"id": ["1"]}
        assert validation_status == conf.VERIFIED_PREFIX


def test_get_unarchived_stream_marks_schema_mismatch_as_unverified(make_zip_file):
    zip_iter = make_zip_file(files={"test.csv": "other\n1\n"})

    results = _collect(unzip.get_unarchived_stream(zip_iter, SCHEMA))

    assert [status for _, _, status in results] == [conf.UNVERIFIED_PREFIX]


@pytest.mark.parametrize(
    "file_name, content, expected_error",
    [
        pytest.param("test.json", '[{"id": 1}', ValueError, id="json_truncated"),
        pytest.param("test.csv", "id\n1,2\n", pa.ArrowInvalid, id="csv_extra_column"),
    ],
)
def test_get_unarchived_stream_raises_on_malformed_file(
    file_name, content, expected_error, make_zip_file
):
    zip_iter = make_zip_file(files={file_name: content})

    with pytest.raises(expected_error):
        _collect(unzip.get_unarchived_stream(zip_iter, SCHEMA))


# upload_unarchived_zip_stream_to_s3


LANDING_KEY = "source/dataset/ingest_date=2026-01-01/archive.zip"
BRONZE_KEY = "source/dataset/ingest_date=2026-01-01/zip_name=archive"
SIBLING_FILE = "source/dataset/ingest_date=2026-01-01/zip_name=archive2/x.parquet"
UPLOAD_SCHEMA = pa.schema([("id", pa.string()), ("name", pa.string())])


@pytest.fixture
def bucket(tmp_path) -> str:
    """A local directory that plays the role of the bronze bucket."""
    return tmp_path.as_posix()


def _upload(files: dict[str, str], bucket: str, make_zip_file) -> None:
    stream = unzip.get_unarchived_stream(make_zip_file(files=files), UPLOAD_SCHEMA)
    unzip.upload_unarchived_zip_stream_to_s3(
        unarchived_stream=stream,
        s3fs=fs.LocalFileSystem(),
        bucket=bucket,
        source_key=LANDING_KEY,
        max_rows_per_file=2,
        max_rows_per_group=2,
        min_rows_per_group=1,
    )


def _list_files(bucket: str) -> list[str]:
    selector = fs.FileSelector(bucket, recursive=True)
    return sorted(
        info.path.removeprefix(f"{bucket}/")
        for info in fs.LocalFileSystem().get_file_info(selector)
        if info.type == fs.FileType.File
    )


def test_upload_writes_layout_and_success_marker(bucket, make_zip_file):
    _upload(
        {"a.csv": "id,name\n1,x\n2,y\n3,z\n", "b.csv": "id,other\n1,x\n"},
        bucket,
        make_zip_file,
    )

    assert _list_files(bucket) == [
        f"{BRONZE_KEY}/_SUCCESS",
        f"{BRONZE_KEY}/schema_status=unverified/b.csv_part-0.parquet",
        f"{BRONZE_KEY}/schema_status=verified/a.csv_part-0.parquet",
        f"{BRONZE_KEY}/schema_status=verified/a.csv_part-1.parquet",
    ]


def test_upload_rerun_leaves_no_files_from_previous_run(bucket, make_zip_file):
    """
    The re-run produces fewer parts and moves the file to another validation
    status. Nothing from the first run must survive.
    """
    _upload({"a.csv": "id,name\n1,x\n2,y\n3,z\n"}, bucket, make_zip_file)
    _upload({"a.csv": "id,other\n1,x\n"}, bucket, make_zip_file)

    assert _list_files(bucket) == [
        f"{BRONZE_KEY}/_SUCCESS",
        f"{BRONZE_KEY}/schema_status=unverified/a.csv_part-0.parquet",
    ]


def test_upload_rerun_of_zip_without_data_removes_previous_output(
    bucket, make_zip_file
):
    _upload({"a.csv": "id,name\n1,x\n"}, bucket, make_zip_file)
    _upload({"a.csv": ""}, bucket, make_zip_file)

    assert _list_files(bucket) == []


def test_upload_of_zip_with_only_files_without_data_writes_nothing(
    bucket, make_zip_file
):
    _upload({"a.json": "[]", "b.json": "{}"}, bucket, make_zip_file)

    assert _list_files(bucket) == []


def test_upload_does_not_touch_other_zips(bucket, make_zip_file):
    local_fs = fs.LocalFileSystem()
    local_fs.create_dir(f"{bucket}/{SIBLING_FILE.rsplit('/', 1)[0]}")
    local_fs.open_output_stream(f"{bucket}/{SIBLING_FILE}").close()

    _upload({"a.csv": "id,name\n1,x\n"}, bucket, make_zip_file)

    assert SIBLING_FILE in _list_files(bucket)
