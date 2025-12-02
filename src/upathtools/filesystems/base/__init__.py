"""Base filesystem classes."""

from __future__ import annotations

from upathtools.filesystems.base.basefilesystem import BaseAsyncFileSystem, BaseFileSystem
from upathtools.filesystems.base.baseupath import BaseUPath
from upathtools.filesystems.base.file_objects import AsyncFile, BufferedWriter, FileInfo
from upathtools.filesystems.base.wrapper import WrapperFileSystem

__all__ = [
    "AsyncFile",
    "BaseAsyncFileSystem",
    "BaseFileSystem",
    "BaseUPath",
    "BufferedWriter",
    "FileInfo",
    "WrapperFileSystem",
]
