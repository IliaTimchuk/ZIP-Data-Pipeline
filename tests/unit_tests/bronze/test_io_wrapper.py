import io
import pytest

from src.bronze.io_wrapper import BytesIteratorIO


@pytest.fixture
def make_stream():
    """Builds a stream over the exact chunks the iterator will yield."""

    def _make(*chunks):
        return BytesIteratorIO(iter(chunks))

    return _make


# read


@pytest.mark.parametrize(
    "chunks",
    [
        (b"hello world",),
        (b"hello", b" ", b"world"),
        (b"h", b"e", b"l", b"l", b"o", b" world"),
        (b"", b"hello", b"", b" world", b""),
    ],
)
def test_read_reassembles_the_stream_regardless_of_chunking(make_stream, chunks):
    stream = make_stream(*chunks)

    assert stream.read() == b"hello world"


@pytest.mark.parametrize(
    "size, expected",
    [
        (2, b"ab"),
        (3, b"abc"),
        (4, b"abcd"),
        (9, b"abcdefghi"),
    ],
)
def test_read_size_is_independent_of_chunk_boundaries(make_stream, size, expected):
    stream = make_stream(b"abc", b"def", b"ghi")

    assert stream.read(size) == expected


@pytest.mark.parametrize(
    "first_size, second_size, first_expected, second_expected",
    [
        (1, 1, b"a", b"b"),
        (2, 1, b"ab", b"c"),
        (2, 4, b"ab", b"cdef"),
    ],
)
def test_successive_reads_continue_where_the_last_one_stopped(
    make_stream, first_size, second_size, first_expected, second_expected
):
    stream = make_stream(b"abc", b"def")

    assert stream.read(first_size) == first_expected
    assert stream.read(second_size) == second_expected


def test_reading_one_byte_at_a_time_walks_the_whole_stream(make_stream):
    stream = make_stream(b"abc", b"def")

    assert [stream.read(1) for _ in range(6)] == [b"a", b"b", b"c", b"d", b"e", b"f"]


@pytest.mark.parametrize("size", [None, -1])
def test_read_without_a_size_returns_the_rest_of_the_stream(make_stream, size):
    stream = make_stream(b"abc", b"def")

    assert stream.read(2) == b"ab"
    assert stream.read(size) == b"cdef"


def test_read_past_the_end_returns_what_is_left(make_stream):
    stream = make_stream(b"abc", b"def")

    assert stream.read(100) == b"abcdef"


def test_an_empty_stream_reads_as_empty(make_stream):
    stream = make_stream()

    assert stream.read() == b""
    assert stream.read(10) == b""


def test_reads_stay_empty_once_the_stream_is_exhausted(make_stream):
    stream = make_stream(b"abc")

    assert stream.read() == b"abc"
    assert stream.read() == b""
    assert stream.read(10) == b""


# readinto


def test_readinto_fills_the_buffer_and_reports_how_many_bytes(make_stream):
    stream = make_stream(b"abc", b"def")
    buffer = bytearray(4)

    assert stream.readinto(buffer) == 4
    assert bytes(buffer) == b"abcd"


def test_readinto_returns_zero_at_the_end_of_the_stream(make_stream):
    stream = make_stream()

    assert stream.readinto(bytearray(4)) == 0


# file-like interface


def test_stream_is_readable(make_stream):
    stream = make_stream(b"abc")

    assert stream.readable() is True
