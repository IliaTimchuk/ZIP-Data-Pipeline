import io

class BytesIteratorIO(io.RawIOBase):
    """Class that wraps an iterator of bytes and provides a file-like interface."""

    def __init__(self, iterator):
        self._iter = iterator
        self._left = b''

    def readable(self):
        return True

    def readinto(self, b):
        n = len(b)
        view = memoryview(b).cast('B')
        bytes_read = 0

        if self._left:
            chunk_size = len(self._left)
            if chunk_size <= n:
                view[:chunk_size] = self._left
                self._left = b''
                bytes_read += chunk_size
                n -= chunk_size
            else:
                view[:n] = self._left[:n]
                self._left = self._left[n:]
                return bytes_read + n

        while n > 0:
            try:
                chunk = next(self._iter)
                chunk_size = len(chunk)
                
                if chunk_size <= n:
                    view[bytes_read : bytes_read + chunk_size] = chunk
                    bytes_read += chunk_size
                    n -= chunk_size
                else:
                    view[bytes_read : bytes_read + n] = chunk[:n]
                    self._left = chunk[n:]
                    bytes_read += n
                    n = 0 
            except StopIteration:
                break

        return bytes_read

    def read(self, n=-1):
        """Fallback for operations that don't support readinto."""
        if n is None or n < 0:
            res = self._left + b''.join(self._iter)
            self._left = b''
            return res

        b = bytearray(n)
        bytes_read = self.readinto(b)
        return bytes(b[:bytes_read])