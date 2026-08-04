import io


class BytesIteratorIO(io.RawIOBase):
    """Class that wraps an iterator of bytes and provides a file-like interface."""

    def __init__(self, iterator):
        self._iter = iterator
        self._left = b''

    def readable(self):
        return True

    def read(self, n=-1):
        if n is None or n < 0:
            res = self._left + b''.join(self._iter)
            self._left = b''
            return res

        chunks = [self._left]
        size = len(self._left)
        while size < n:
            try:
                chunk = next(self._iter)
                chunks.append(chunk)
                size += len(chunk)
            except StopIteration:
                break

        data = b''.join(chunks)
        self._left = data[n:]
        return data[:n]
