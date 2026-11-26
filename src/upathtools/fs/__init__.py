"""Typed, async-first core filesystem abstractions for ``upathtools``."""

from upathtools.fs._sync import FSSPEC_SYNC_VERSION, FsspecSyncNote
from upathtools.fs.errors import (
    FilesystemClosedError,
    FSError,
    FSTimeoutError,
    PathEscapeError,
    UnsupportedOperation,
)
from upathtools.fs.protocols import (
    SupportsGrep,
    SupportsMove,
    SupportsRangeRead,
    SupportsSign,
    SupportsWrite,
)
from upathtools.fs.types import (
    EntryType,
    FileInfo,
    GrepMatch,
    OpenMode,
    WriteMode,
)


__all__ = [
    "FSSPEC_SYNC_VERSION",
    "EntryType",
    "FSError",
    "FSTimeoutError",
    "FileInfo",
    "FilesystemClosedError",
    "FsspecSyncNote",
    "GrepMatch",
    "OpenMode",
    "PathEscapeError",
    "SupportsGrep",
    "SupportsMove",
    "SupportsRangeRead",
    "SupportsSign",
    "SupportsWrite",
    "UnsupportedOperation",
    "WriteMode",
]
