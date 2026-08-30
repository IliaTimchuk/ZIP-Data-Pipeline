import pytest
import pyarrow as pa
import io
import zipfile

# import scripts.bronze.unarchive_zip as unzip


@pytest.fixture
def test_zip_file():
    file = io.BytesIO()
    with zipfile.ZipFile(file, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("file1.csv", "id name score is_active")
        zf.writestr()

    return file


@pytest.fixture
def test_stream(test_zip_file):
    test_zip_file.seek(0)
    while True:
        data = test_zip_file.read()
        if not data:
            break
        yield data


@pytest.fixture()
def batch():
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
            ("score", pa.float64()),
            ("is_active", pa.bool_()),
        ]
    )

    data = [
        pa.array([1, 2, 3]),
        pa.array(["Alice", "Bob", "Charlie"]),
        pa.array([92.5, 85.0, 78.3]),
        pa.array([True, False, True]),
    ]
    return pa.RecordBatch.from_arrays(data, schema=schema)


@pytest.fixture
def columns_shape():
    shape = {
        "test_int": pa.scalar(100, pa.int32()),
        "test_string": pa.scalar("test", pa.string()),
    }
    return shape