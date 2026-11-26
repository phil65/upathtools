"""Typed, async-first core filesystem abstractions for ``upathtools``."""

from upathtools.fs._sync import FSSPEC_SYNC_VERSION, FsspecSyncNote
from upathtools.fs.base import AsyncFS
from upathtools.fs.batch import run_bounded
from upathtools.fs.cache import DirCache
from upathtools.fs.errors import (
    FilesystemClosedError,
    FSError,
    FSTimeoutError,
    PathEscapeError,
    UnsupportedOperation,
)
from upathtools.fs.glob import (
    common_prefix,
    compile_glob,
    glob_translate,
    has_magic,
    other_paths,
)
from upathtools.fs.implementations import MemoryFS, MemoryStore
from upathtools.fs.paths import (
    ChainLink,
    check_contained,
    infer_storage_options,
    join_path,
    normalize_path,
    parent_path,
    split_protocol,
    strip_protocol,
    tokenize,
    unchain,
)
from upathtools.fs.protocols import (
    SupportsGrep,
    SupportsMove,
    SupportsRangeRead,
    SupportsSign,
    SupportsWrite,
)
from upathtools.fs.runner import LoopRunner, get_default_runner
from upathtools.fs.streams import (
    DEFAULT_BLOCK_SIZE,
    AsyncBufferedReader,
    AsyncBufferedWriter,
    AsyncReadStream,
    AsyncSeekableStream,
    AsyncWriteStream,
)
from upathtools.fs.sync import SyncFileBridge, SyncFS
from upathtools.fs.types import (
    EntryType,
    FileInfo,
    GrepMatch,
    OpenMode,
    WriteMode,
)


__all__ = [
    "DEFAULT_BLOCK_SIZE",
    "FSSPEC_SYNC_VERSION",
    "AsyncBufferedReader",
    "AsyncBufferedWriter",
    "AsyncFS",
    "AsyncReadStream",
    "AsyncSeekableStream",
    "AsyncWriteStream",
    "ChainLink",
    "DirCache",
    "EntryType",
    "FSError",
    "FSTimeoutError",
    "FileInfo",
    "FilesystemClosedError",
    "FsspecSyncNote",
    "GrepMatch",
    "LoopRunner",
    "MemoryFS",
    "MemoryStore",
    "OpenMode",
    "PathEscapeError",
    "SupportsGrep",
    "SupportsMove",
    "SupportsRangeRead",
    "SupportsSign",
    "SupportsWrite",
    "SyncFS",
    "SyncFileBridge",
    "UnsupportedOperation",
    "WriteMode",
    "check_contained",
    "common_prefix",
    "compile_glob",
    "get_default_runner",
    "glob_translate",
    "has_magic",
    "infer_storage_options",
    "join_path",
    "normalize_path",
    "other_paths",
    "parent_path",
    "run_bounded",
    "split_protocol",
    "strip_protocol",
    "tokenize",
    "unchain",
]
