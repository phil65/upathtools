"""Base filesystem classes."""

from __future__ import annotations

from upathtools.filesystems.base.basefilesystem import (
    BaseAsyncFileSystem,
    BaseFileSystem,
    CreationMode,
    GrepMatch,
)
from upathtools.filesystems.base.baseupath import BaseUPath
from upathtools.filesystems.base.file_objects import (
    AsyncBufferedFile,
    AsyncFile,
    AsyncReadable,
    AsyncSeekable,
    AsyncWritable,
    BufferedWriter,
    FileInfo,
)
from upathtools.filesystems.base.filefilesystem import (
    BaseAsyncFileFileSystem,
    BaseFileFileSystem,
    FileFileSystemMixin,
    ProbeResult,
)
from upathtools.filesystems.base.wrapper import (
    ContentMount,
    FilesystemMount,
    WrapperFileSystem,
)

__all__ = [
    "AsyncBufferedFile",
    "AsyncFile",
    "AsyncReadable",
    "AsyncSeekable",
    "AsyncWritable",
    "BaseAsyncFileFileSystem",
    "BaseAsyncFileSystem",
    "BaseFileFileSystem",
    "BaseFileSystem",
    "BaseUPath",
    "BufferedWriter",
    "ContentMount",
    "CreationMode",
    "FileFileSystemMixin",
    "FileInfo",
    "FilesystemMount",
    "GrepMatch",
    "ProbeResult",
    "WrapperFileSystem",
]
