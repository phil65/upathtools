"""Base File objects."""

from __future__ import annotations

import io
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    Protocol,
    Required,
    Self,
    TypedDict,
    runtime_checkable,
)


if TYPE_CHECKING:
    from collections.abc import Buffer

    from fsspec.asyn import AsyncFileSystem
    from fsspec.spec import AbstractFileSystem


class FileInfo(TypedDict):
    """Info dict for Markdown filesystem paths."""

    name: Required[str]
    type: Required[Literal["file", "directory", "other", "link"]]


@runtime_checkable
class AsyncReadable(Protocol):
    """Protocol for async readable file-like objects."""

    async def read(self, size: int = -1) -> bytes: ...
    async def close(self) -> None: ...


@runtime_checkable
class AsyncWritable(Protocol):
    """Protocol for async writable file-like objects."""

    async def write(self, data: bytes) -> int: ...
    async def flush(self) -> None: ...
    async def close(self) -> None: ...


@runtime_checkable
class AsyncSeekable(Protocol):
    """Protocol for async seekable file-like objects."""

    def seek(self, offset: int, whence: int = 0) -> int: ...
    def tell(self) -> int: ...


class AsyncFile:
    """Simple asynchronous writer that buffers and writes on close.

    This is a write-only file object for filesystems that support `_pipe_file`.
    For read support, use `AsyncBufferedFile` instead.
    """

    def __init__(self, fs: AsyncFileSystem, path: str, **kwargs: Any) -> None:
        """Initialize the writer.

        Args:
            fs: AsyncFileSystem instance
            path: Path to write to
            **kwargs: Additional arguments to pass to _pipe_file
        """
        self.fs = fs
        self.path = path
        self.buffer = io.BytesIO()
        self.kwargs = kwargs
        self.closed = False

    async def write(self, data: bytes) -> int:
        """Write data to the buffer.

        Args:
            data: Data to write
        """
        return self.buffer.write(data)

    async def close(self) -> None:
        """Close the writer and write content to the file."""
        if not self.closed:
            self.closed = True
            content = self.buffer.getvalue()
            await self.fs._pipe_file(self.path, content, **self.kwargs)
            self.buffer.close()

    def __aenter__(self) -> AsyncFile:
        """Enter the context manager."""
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Exit the context manager and close the writer."""
        await self.close()


class AsyncBufferedFile:
    """Unified async file object for filesystems using _cat_file/_pipe_file.

    This class provides a complete async file interface that works with any
    AsyncFileSystem implementing `_cat_file` and `_pipe_file` methods.

    Note: This implementation buffers the entire file content in memory.
    For true streaming/partial read support, the underlying filesystem
    must provide native streaming APIs.
    """

    def __init__(
        self,
        fs: AsyncFileSystem,
        path: str,
        mode: str = "rb",
        **kwargs: Any,
    ) -> None:
        """Initialize async buffered file.

        Args:
            fs: AsyncFileSystem instance with _cat_file/_pipe_file methods
            path: File path
            mode: File open mode ('rb', 'wb', 'r+b', 'ab')
            **kwargs: Additional options passed to _cat_file/_pipe_file
        """
        self.fs = fs
        self.path = path
        self.mode = mode
        self.kwargs = kwargs
        self._buffer = io.BytesIO()
        self._closed = False
        self._loaded = False

    async def _ensure_loaded(self) -> None:
        """Load file content into buffer if not already loaded."""
        if self._loaded:
            return
        if "r" in self.mode or "a" in self.mode or "+" in self.mode:
            try:
                content = await self.fs._cat_file(self.path, **self.kwargs)
                self._buffer = io.BytesIO(content)
                if "a" in self.mode:
                    self._buffer.seek(0, 2)  # Seek to end for append
            except FileNotFoundError:
                if "w" not in self.mode and "a" not in self.mode:
                    raise
                # For write/append modes, start with empty buffer
        self._loaded = True

    def readable(self) -> bool:
        """Check if file is readable."""
        return "r" in self.mode or "+" in self.mode

    def writable(self) -> bool:
        """Check if file is writable."""
        return "w" in self.mode or "a" in self.mode or "+" in self.mode

    def seekable(self) -> bool:
        """Check if file is seekable."""
        return True

    @property
    def closed(self) -> bool:
        """Check if file is closed."""
        return self._closed

    def tell(self) -> int:
        """Get current buffer position."""
        if self._closed:
            msg = "I/O operation on closed file"
            raise ValueError(msg)
        return self._buffer.tell()

    def seek(self, offset: int, whence: int = 0) -> int:
        """Seek to position in buffer.

        Args:
            offset: Position offset
            whence: Reference point (0=start, 1=current, 2=end)
        """
        if self._closed:
            msg = "I/O operation on closed file"
            raise ValueError(msg)
        return self._buffer.seek(offset, whence)

    async def read(self, size: int = -1) -> bytes:
        """Read data from file.

        Args:
            size: Number of bytes to read (-1 for all)
        """
        if self._closed:
            msg = "I/O operation on closed file"
            raise ValueError(msg)
        if not self.readable():
            msg = "File not open for reading"
            raise io.UnsupportedOperation(msg)

        await self._ensure_loaded()
        return self._buffer.read(size)

    async def readline(self, size: int = -1) -> bytes:
        """Read a single line from file.

        Args:
            size: Maximum bytes to read (-1 for unlimited)
        """
        if self._closed:
            msg = "I/O operation on closed file"
            raise ValueError(msg)
        if not self.readable():
            msg = "File not open for reading"
            raise io.UnsupportedOperation(msg)

        await self._ensure_loaded()
        return self._buffer.readline(size)

    async def readlines(self, hint: int = -1) -> list[bytes]:
        """Read all lines from file.

        Args:
            hint: Approximate number of bytes to read (-1 for all)
        """
        if self._closed:
            msg = "I/O operation on closed file"
            raise ValueError(msg)
        if not self.readable():
            msg = "File not open for reading"
            raise io.UnsupportedOperation(msg)

        await self._ensure_loaded()
        return self._buffer.readlines(hint)

    async def write(self, data: bytes) -> int:
        """Write data to buffer.

        Args:
            data: Data to write
        """
        if self._closed:
            msg = "I/O operation on closed file"
            raise ValueError(msg)
        if not self.writable():
            msg = "File not open for writing"
            raise io.UnsupportedOperation(msg)

        # For modes that need existing content, ensure loaded first
        if "+" in self.mode or "a" in self.mode:
            await self._ensure_loaded()

        return self._buffer.write(data)

    async def writelines(self, lines: list[bytes]) -> None:
        """Write multiple lines to buffer.

        Args:
            lines: Lines to write
        """
        for line in lines:
            await self.write(line)

    async def flush(self) -> None:
        """Flush buffer contents to remote file."""
        if self._closed:
            return
        if self.writable():
            pos = self._buffer.tell()
            self._buffer.seek(0)
            content = self._buffer.read()
            self._buffer.seek(pos)
            await self.fs._pipe_file(self.path, content, **self.kwargs)

    async def close(self) -> None:
        """Close file, flushing if writable."""
        if not self._closed:
            if self.writable():
                await self.flush()
            self._buffer.close()
            self._closed = True

    async def __aenter__(self) -> Self:
        """Enter async context manager."""
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Exit async context manager."""
        await self.close()

    def __repr__(self) -> str:
        return f"<AsyncBufferedFile path={self.path!r} mode={self.mode!r} closed={self._closed}>"


class BufferedWriter(io.BufferedIOBase):
    """Buffered writer for filesystems that writes when closed.

    Generic implementation that can be used by any filesystem implementing
    a pipe_file method for writing content.
    """

    def __init__(
        self,
        buffer: io.BytesIO,
        fs: AbstractFileSystem,
        path: str,
        **kwargs: Any,
    ) -> None:
        """Initialize the writer.

        Args:
            buffer: Buffer to store content
            fs: Filesystem instance with pipe_file method
            path: Path to write to
            **kwargs: Additional arguments to pass to pipe_file
        """
        super().__init__()
        self.buffer = buffer
        self.fs = fs
        self.path = path
        self.kwargs = kwargs

    def write(self, data: Buffer) -> int:
        """Write data to the buffer.

        Args:
            data: Data to write
        """
        return self.buffer.write(data)

    def close(self) -> None:
        """Close the writer and write content to the filesystem."""
        if not self.closed:
            content = self.buffer.getvalue()
            self.fs.pipe_file(self.path, content, **self.kwargs)
            self.buffer.close()
            super().close()

    def readable(self) -> bool:
        """Whether the writer is readable."""
        return False

    def writable(self) -> bool:
        """Whether the writer is writable."""
        return True
