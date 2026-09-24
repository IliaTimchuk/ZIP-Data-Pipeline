import io
import pytest
import pyarrow as pa
from unittest.mock import MagicMock, patch

import src.bronze.readers as readers
import settings.pipeline_config as conf

# open_string_json


def test_open_string_json_parses_numbers_as_strings(make_json_file):
    json_content = [{"user_id": 1, "balance": 1222.33}, {"user_id": 2, "balance": 0.0}]
    expected_schema = pa.schema(
        [pa.field("user_id", pa.string()), pa.field("balance", pa.string())]
    )
    io_json = make_json_file(json_content)

    reader = readers.open_string_json(json_iterator=io_json)
    table = reader.read_all()

    assert table.to_pydict() == {"user_id": ["1", "2"], "balance": ["1222.33", "0.0"]}
    assert table.schema.equals(expected_schema)


def test_open_string_json_parses_single_json_object(make_json_file):
    json_content = {"user_id": 1, "status": "active"}
    expected_schema = pa.schema(
        [pa.field("user_id", pa.string()), pa.field("status", pa.string())]
    )

    io_json = make_json_file(json_content)

    reader = readers.open_string_json(json_iterator=io_json)
    table = reader.read_all()

    assert table.num_rows == 1
    assert table.to_pydict() == {"user_id": ["1"], "status": ["active"]}
    assert table.schema.equals(expected_schema)


def test_open_string_json_parse_nested_json(make_json_file):
    json_content = [{"user_id": 1, "trade": {"currency": "BTC", "total": 120.54}}]
    expected_schema = pa.schema(
        [
            pa.field("user_id", pa.string()),
            pa.field(
                "trade",
                pa.struct(
                    [pa.field("currency", pa.string()), pa.field("total", pa.string())]
                ),
            ),
        ]
    )

    io_json = make_json_file(json_content)

    reader = readers.open_string_json(json_iterator=io_json)
    table = reader.read_all()

    nested_structure = table.field("trade").type

    assert pa.types.is_struct(nested_structure)
    assert table.schema.equals(expected_schema)
    assert table.to_pydict() == {
        "user_id": ["1"],
        "trade": [{"currency": "BTC", "total": "120.54"}],
    }


def test_open_string_json_parses_nulls(make_json_file):
    json_content = [{"user_id": 1, "balance": None}, {"user_id": 1, "balance": 1222.33}]
    expected_schema = pa.schema(
        [pa.field("user_id", pa.string()), pa.field("balance", pa.string())]
    )

    io_json = make_json_file(json_content)

    reader = readers.open_string_json(json_iterator=io_json)
    table = reader.read_all()

    assert table.to_pydict() == {"user_id": ["1", "1"], "balance": [None, "1222.33"]}
    assert table.schema.equals(expected_schema)


@pytest.mark.parametrize(
    "content",
    [
        pytest.param(b"", id="zero_bytes"),
        pytest.param(b"  \n", id="whitespace_only"),
        pytest.param(b"{}", id="empty_object"),
        pytest.param(b"[]", id="empty_array"),
        pytest.param(b"[{}]", id="array_of_empty_object"),
    ],
)
def test_open_string_json_raises_on_file_without_data(content):
    io_json = io.BytesIO(content)
    with pytest.raises(readers.EmptyFileError):
        readers.open_string_json(io_json)


# _read_iterators


def test_read_iterator_returns_buffer_when_under_or_at_limit():
    limit = 10
    stream_under = io.BytesIO(b"12345")
    stream_exact = io.BytesIO(b"1234567890")

    assert readers._read_iterator(stream_under, limit=limit) == b"12345"
    assert readers._read_iterator(stream_exact, limit=limit) == b"1234567890"


def test_read_iterator_raises_error_when_over_limit():
    limit = 10
    stream_over = io.BytesIO(b"12345678901")

    with pytest.raises(
        readers.JsonTooLargeError,
        match=f"JSON member exceeds the {limit} byte in-memory limit",
    ):
        readers._read_iterator(stream_over, limit=limit)


# open_string_csv


def test_open_string_csv_unexpected_columns_are_strings(make_csv_file):
    csv_headers = ["user_id", "balance", "unexpected_metadata"]
    csv_content = [
        ["1", "1222.33", "12.09.2025"],
        ["2", "134.0", "12.01.2026"],
    ]
    expected_schema = pa.schema(
        [
            pa.field("user_id", pa.string()),
            pa.field("balance", pa.string()),
            pa.field("unexpected_metadata", pa.string()),
        ]
    )

    io_csv = make_csv_file(rows=csv_content, headers=csv_headers)

    reader = readers.open_string_csv(io_csv)
    table = reader.read_all()

    assert table.schema.equals(expected_schema)
    assert table.to_pydict() == {
        "user_id": ["1", "2"],
        "balance": ["1222.33", "134.0"],
        "unexpected_metadata": ["12.09.2025", "12.01.2026"],
    }

@pytest.mark.parametrize(
    "content",
    [
        pytest.param(b"", id="zero_bytes"),
        pytest.param(b"id", id="header_only_without_newline"),
    ],
)
def test_open_string_csv_raises_on_file_without_data(content):
    io_csv = io.BytesIO(content)
    with pytest.raises(readers.EmptyFileError):
        readers.open_string_csv(io_csv)


# open_file_like


@patch.dict("src.bronze.readers._READERS", clear=True)
def test_open_file_like_routes_to_correct_reader():
    mock_iterator = MagicMock()
    mock_csv_reader = MagicMock(return_value="fake_open_string_csv")

    readers._READERS["csv"] = mock_csv_reader

    result = readers.open_file_like(mock_iterator, "file.csv")

    mock_csv_reader.assert_called_once_with(mock_iterator)
    assert result == "fake_open_string_csv"


@pytest.mark.parametrize("file_name", ["data.CSV", "data.Csv"])
@patch.dict("src.bronze.readers._READERS", clear=True)
def test_open_file_like_handles_extension_casing(file_name):
    mock_iterator = MagicMock()
    mock_csv_reader = MagicMock()
    readers._READERS["csv"] = mock_csv_reader

    readers.open_file_like(mock_iterator, file_name)
    mock_csv_reader.assert_called_once_with(mock_iterator)


# validate_schema


def test_validate_schema_returns_verified_on_match():
    actual_schema = pa.schema([pa.field("user_id", pa.int64())])
    expected_schema = pa.schema([pa.field("user_id", pa.int64())])

    mock_reader = MagicMock()
    mock_reader.schema = actual_schema

    result = readers.validate_schema(mock_reader, expected_schema)

    assert result == conf.VERIFIED_PREFIX


def test_validate_schema_returns_unverified_on_mismatch():
    actual_schema = pa.schema([pa.field("user_id", pa.int64())])
    expected_schema = pa.schema([pa.field("user_id", pa.string())])

    mock_reader = MagicMock()
    mock_reader.schema = actual_schema

    result = readers.validate_schema(mock_reader, expected_schema)

    assert result == conf.UNVERIFIED_PREFIX
