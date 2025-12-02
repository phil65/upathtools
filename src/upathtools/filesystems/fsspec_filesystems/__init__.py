"""Fsspec filesystem classes with UPath integration."""

from __future__ import annotations

from fsspec.implementations.cached import SimpleCacheFileSystem as _SimpleCacheFileSystem
from fsspec.implementations.data import DataFileSystem as _DataFileSystem
from fsspec.implementations.github import GithubFileSystem as _GithubFileSystem

# from fsspec.implementations.http import HTTPFileSystem as _HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem as _LocalFileSystem
from fsspec.implementations.memory import MemoryFileSystem as _MemoryFileSystem
from fsspec.implementations.tar import TarFileSystem as _TarFileSystem
from fsspec.implementations.zip import ZipFileSystem as _ZipFileSystem
from upath.implementations.cached import SimpleCachePath
from upath.implementations.data import DataPath
from upath.implementations.github import GitHubPath

# from upath.implementations.http import HTTPPath
from upath.implementations.local import FilePath
from upath.implementations.memory import MemoryPath
from upath.implementations.tar import TarPath
from upath.implementations.zip import ZipPath

from upathtools.filesystems.base import BaseFileSystem


class LocalFileSystem(BaseFileSystem[FilePath], _LocalFileSystem):
    """Local filesystem with UPath integration."""

    upath_cls = FilePath


class MemoryFileSystem(BaseFileSystem[MemoryPath], _MemoryFileSystem):
    """In-memory filesystem with UPath integration."""

    upath_cls = MemoryPath


# class HTTPFileSystem(BaseFileSystem[HTTPPath], _HTTPFileSystem):
#     """HTTP/HTTPS filesystem with UPath integration."""

#     upath_cls = HTTPPath


class GithubFileSystem(BaseFileSystem[GitHubPath], _GithubFileSystem):
    """GitHub filesystem with UPath integration."""

    upath_cls = GitHubPath


class TarFileSystem(BaseFileSystem[TarPath], _TarFileSystem):
    """Tar archive filesystem with UPath integration."""

    upath_cls = TarPath


class ZipFileSystem(BaseFileSystem[ZipPath], _ZipFileSystem):
    """Zip archive filesystem with UPath integration."""

    upath_cls = ZipPath


class SimpleCacheFileSystem(BaseFileSystem[SimpleCachePath], _SimpleCacheFileSystem):
    """Simple cache filesystem with UPath integration."""

    upath_cls = SimpleCachePath


class DataFileSystem(BaseFileSystem[DataPath], _DataFileSystem):
    """Data URI filesystem with UPath integration."""

    upath_cls = DataPath
