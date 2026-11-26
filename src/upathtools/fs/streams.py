"""Async binary stream protocols and buffered file implementations for ``upathtools.fs``."""

import errno
import io
from typing import TYPE_CHECKING, Any, Literal, Protocol, Self, runtime_checkable


if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from upathtools.fs.base import AsyncFS


DEFAULT_BLOCK_SIZE: int = 5 * 1024 * 1024  # 5 MiB

type _WriteOpenMode = Literal["wb", "xb", "ab"]


@runtime_checkable
class AsyncReadStream(Protocol):
    """Protocol for an async binary readable stream."""

    async def read(self, size: int = -1) -> bytes: ...
    async def readline(self, size: int = -1) -> bytes: ...
    def __aiter__(self) -> AsyncIterator[bytes]: ...
    async def aclose(self) -> None: ...
    async def __aenter__(self) -> Self: ...
    async def __aexit__(self, *exc: object) -> None: ...


@runtime_checkable
class AsyncSeekableStream(AsyncReadStream, Protocol):
    """Protocol for an async binary readable stream that supports random-access seeking."""

    def tell(self) -> int: ...
    def seek(self, offset: int, whence: int = 0) -> int: ...


@runtime_checkable
class AsyncWriteStream(Protocol):
    """Protocol for an async binary writable stream."""

    async def write(self, data: bytes | bytearray | memoryview) -> int: ...
    async def flush(self) -> None: ...
    async def aclose(self) -> None: ...
    async def __aenter__(self) -> Self: ...
    async def __aexit__(self, *exc: object) -> None: ...


class AsyncBufferedReader:
    """Buffered, seekable async binary reader backed by :meth:`AsyncFS.read_bytes`.

    Reads fixed-size blocks of ``block_size`` bytes (default 5 MiB) on demand via
    byte-range ``read_bytes(path, start=..., end=...)`` calls. File metadata (and size)
    is fetched once via ``info()`` when entering ``async with`` (or lazily on the first
    read if used without a context manager).
    """

    __slots__ = (
        "_block_size",
        "_buf",
        "_buf_start",
        "_closed",
        "_fs",
        "_opened",
        "_path",
        "_pos",
        "_size",
    )

    def __init__(
        self,
        fs: AsyncFS[Any],
        path: str,
        *,
        block_size: int = DEFAULT_BLOCK_SIZE,
    ) -> None:
        if block_size <= 0:
            raise ValueError(f"block_size must be a positive integer, got {block_size}")
        self._fs = fs
        self._path = path
        self._block_size = block_size
        self._pos = 0
        self._size: int | None = None
        self._buf = b""
        self._buf_start = 0
        self._opened = False
        self._closed = False

    @property
    def closed(self) -> bool:
        """Return True if the stream has been closed."""
        return self._closed

    @property
    def size(self) -> int | None:
        """Total file size in bytes once opened, or ``None`` if unknown."""
        return self._size

    def _check_not_closed(self) -> None:
        if self._closed:
            raise ValueError("I/O operation on closed file")

    async def _ensure_open(self) -> None:
        self._check_not_closed()
        if not self._opened:
            info = await self._fs.info(self._path)
            if info.is_dir:
                raise IsADirectoryError(errno.EISDIR, "Is a directory", self._path)
            self._size = info.size
            self._opened = True

    async def _fill_buffer(self) -> bool:
        """Fetch a block starting at ``self._pos``. Return False at EOF."""
        if self._size is not None and self._pos >= self._size:
            return False
        end = self._pos + self._block_size
        if self._size is not None:
            end = min(end, self._size)
        chunk = await self._fs.read_bytes(self._path, start=self._pos, end=end)
        if not chunk:
            return False
        self._buf = chunk
        self._buf_start = self._pos
        return True

    def _buffer_has_pos(self) -> bool:
        return self._buf_start <= self._pos < self._buf_start + len(self._buf)

    async def read(self, size: int = -1) -> bytes:
        """Read up to ``size`` bytes, or until EOF when ``size < 0``."""
        await self._ensure_open()
        if size == 0:
            return b""
        if self._size is not None and self._pos >= self._size:
            return b""

        if size < 0:
            parts: list[bytes] = []
            while True:
                if self._size is not None and self._pos >= self._size:
                    break
                if not self._buffer_has_pos() and not await self._fill_buffer():
                    break
                offset = self._pos - self._buf_start
                piece = self._buf[offset:]
                parts.append(piece)
                self._pos += len(piece)
            return b"".join(parts)

        remaining = size
        parts = []
        while remaining > 0:
            if self._size is not None and self._pos >= self._size:
                break
            if not self._buffer_has_pos() and not await self._fill_buffer():
                break
            offset = self._pos - self._buf_start
            piece = self._buf[offset : offset + remaining]
            parts.append(piece)
            self._pos += len(piece)
            remaining -= len(piece)
        return b"".join(parts)

    async def readline(self, size: int = -1) -> bytes:
        r"""Read and return a single line (including trailing ``b'\n'``) up to ``size`` bytes."""
        await self._ensure_open()
        if size == 0:
            return b""

        limit = None if size < 0 else size
        parts: list[bytes] = []
        total = 0

        while True:
            if self._size is not None and self._pos >= self._size:
                break
            if not self._buffer_has_pos() and not await self._fill_buffer():
                break
            offset = self._pos - self._buf_start
            if limit is None:
                view = self._buf[offset:]
            else:
                view = self._buf[offset : offset + (limit - total)]
            nl_idx = view.find(b"\n")
            if nl_idx != -1:
                piece = view[: nl_idx + 1]
                parts.append(piece)
                self._pos += len(piece)
                break
            parts.append(view)
            self._pos += len(view)
            total += len(view)
            if limit is not None and total >= limit:
                break

        return b"".join(parts)

    async def _iter_lines(self) -> AsyncIterator[bytes]:
        while line := await self.readline():
            yield line

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self._iter_lines()

    def tell(self) -> int:
        """Return the current byte offset within the stream."""
        self._check_not_closed()
        return self._pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        """Move the stream position and return the new absolute byte offset.

        Args:
            offset: Byte offset relative to ``whence``.
            whence: ``io.SEEK_SET`` (0), ``io.SEEK_CUR`` (1), or ``io.SEEK_END`` (2).

        Raises:
            ValueError: If closed, ``whence`` is invalid, target position is negative,
                or ``SEEK_END`` is used before file size is known.
        """
        self._check_not_closed()
        if whence == io.SEEK_SET:
            new_pos = offset
        elif whence == io.SEEK_CUR:
            new_pos = self._pos + offset
        elif whence == io.SEEK_END:
            if self._size is None:
                raise ValueError(
                    "Cannot seek from end before stream size is known; "
                    "enter the stream with 'async with' first"
                )
            new_pos = self._size + offset
        else:
            raise ValueError(f"Invalid whence ({whence}, should be 0, 1 or 2)")

        if new_pos < 0:
            raise ValueError(f"Negative seek position {new_pos}")
        self._pos = new_pos
        return self._pos

    async def aclose(self) -> None:
        """Close the reader and release any buffered block. Idempotent."""
        if not self._closed:
            self._closed = True
            self._buf = b""

    async def __aenter__(self) -> Self:
        await self._ensure_open()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()


class AsyncBufferedWriter:
    """In-memory buffered async binary writer that commits once on close.

    Modes:
      - ``"wb"``: Overwrite target on close (``write_bytes(..., mode="overwrite")``).
      - ``"xb"``: Exclusive creation; raises :class:`FileExistsError` at open time if
        the target already exists, and again from ``write_bytes(..., mode="create")``
        at close time if created concurrently.
      - ``"ab"``: Append to existing file content (if present) and write combined bytes
        on close.

    If an exception escapes the ``async with`` block, the buffer is discarded and
    nothing is written to the filesystem.
    """

    __slots__ = ("_buf", "_closed", "_fs", "_initialized", "_mode", "_path")

    def __init__(
        self,
        fs: AsyncFS[Any],
        path: str,
        *,
        mode: _WriteOpenMode = "wb",
    ) -> None:
        if mode not in ("wb", "xb", "ab"):
            raise ValueError(f"Unsupported write mode {mode!r}; expected 'wb', 'xb', or 'ab'")
        self._fs = fs
        self._path = path
        self._mode: _WriteOpenMode = mode
        self._buf = bytearray()
        self._initialized = False
        self._closed = False

    @property
    def closed(self) -> bool:
        """Return True if the stream has been closed."""
        return self._closed

    def _check_not_closed(self) -> None:
        if self._closed:
            raise ValueError("I/O operation on closed file")

    async def _ensure_initialized(self) -> None:
        self._check_not_closed()
        if self._initialized:
            return
        self._fs._check_open()
        if self._mode == "xb":
            if await self._fs.exists(self._path):
                raise FileExistsError(errno.EEXIST, "File exists", self._path)
        elif self._mode == "ab":
            try:
                existing = await self._fs.read_bytes(self._path)
            except FileNotFoundError:
                existing = b""
            self._buf = bytearray(existing)
        self._initialized = True

    async def write(self, data: bytes | bytearray | memoryview) -> int:
        """Append ``data`` to the in-memory buffer and return the number of bytes written."""
        await self._ensure_initialized()
        view = memoryview(data)
        self._buf.extend(view)
        return view.nbytes

    async def flush(self) -> None:
        """Validate the stream is open. Buffered data is committed on :meth:`aclose`."""
        self._check_not_closed()

    async def aclose(self) -> None:
        """Commit buffered bytes via :meth:`AsyncFS.write_bytes` and close. Idempotent."""
        if self._closed:
            return
        try:
            await self._ensure_initialized()
            write_mode: Literal["overwrite", "create"] = (
                "create" if self._mode == "xb" else "overwrite"
            )
            payload = bytes(self._buf)
            await self._fs.write_bytes(self._path, payload, mode=write_mode)
        finally:
            self._buf.clear()
            self._closed = True

    async def __aenter__(self) -> Self:
        await self._ensure_initialized()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        if exc_type is not None:
            self._buf.clear()
            self._closed = True
            return
        await self.aclose()
