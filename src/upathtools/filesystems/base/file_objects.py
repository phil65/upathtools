"""Base File objects."""

from __future__ import annotations

import io
from typing import TYPE_CHECKING, Any, Literal, Required, TypedDict


if TYPE_CHECKING:
    from collections.abc import Buffer

    from fsspec.asyn import AsyncFileSystem
    from fsspec.spec import AbstractFileSystem


class FileInfo(TypedDict):
    """Info dict for Markdown filesystem paths."""

    name: Required[str]
    type: Required[Literal["file", "directory"]]


class AsyncFile:
    """Asynchronous writer for gist files."""

    def __init__(self, fs: AsyncFileSystem, path: str, **kwargs: Any) -> None:
        """Initialize the writer.

        Args:
            fs: GistFileSystem instance
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

        Returns:
            Number of bytes written
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

        Returns:
            Number of bytes written
        """
        return self.buffer.write(data)

    def close(self) -> None:
        """Close the writer and write content to the filesystem."""
        if not self.closed:
            # Get the buffer contents and write to the filesystem
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
