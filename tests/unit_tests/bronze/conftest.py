import io
import csv
import json
import pytest
from typing import IO


@pytest.fixture
def make_json_file() -> IO[bytes]:
    """Creates a BytesIO object containing JSON data."""

    def _make_json_file(content: dict) -> io.BytesIO:
        raw_bytes = json.dumps(content).encode("utf-8")
        return io.BytesIO(raw_bytes)

    return _make_json_file


@pytest.fixture
def make_csv_file() -> IO[bytes]:
    """Creates a BytesIO containing CSV data."""

    def _make_csv_file(rows: list, headers: list = None) -> io.BytesIO:
        string_buffer = io.StringIO()
        writer = csv.writer(string_buffer)
        if headers:
            writer.writerow(headers)
        writer.writerows(rows)

        raw_bytes = string_buffer.getvalue().encode("utf-8")
        return io.BytesIO(raw_bytes)

    return _make_csv_file
