"""UPathTools: main package.

UPath utilities.
"""

from __future__ import annotations

from importlib.metadata import version

__version__ = version("upathtools")
__title__ = "UPathTools"

__author__ = "Philipp Temminghoff"
__author_email__ = "philipptemminghoff@googlemail.com"
__copyright__ = "Copyright (c) 2024 Philipp Temminghoff"
__license__ = "MIT"
__url__ = "https://github.com/phil65/upathtools"

from fsspec import register_implementation
from upath import registry, UPath

from upathtools.core import url_to_fs
from upathtools.helpers import to_upath, upath_to_fs
from upathtools.async_ops import (
    read_path,
    read_folder,
    list_files,
    read_folder_as_text,
    is_directory_sync,
    is_directory,
    fsspec_grep,
)
from upathtools.async_upath import AsyncUPath

from upath.types import JoinablePathLike


def register_http_filesystems() -> None:
    """Register HTTP filesystems."""
    from upathtools.filesystems.httpx_fs import HttpPath, HTTPFileSystem

    register_implementation("http", HTTPFileSystem, clobber=True)
    registry.register_implementation("http", HttpPath, clobber=True)
    register_implementation("https", HTTPFileSystem, clobber=True)
    registry.register_implementation("https", HttpPath, clobber=True)

    HTTPFileSystem.register_fs(clobber=True)


def register_async_local_filesystem() -> None:
    """Register AsyncLocalFileSystem, replacing morefs implementation.

    This makes our optimized AsyncLocalFileSystem (with ripgrep-rs support)
    the default handler for 'asynclocal' protocol.
    """
    from upathtools.filesystems.async_local_fs import register_async_local_fs

    register_async_local_fs()


def register_all_filesystems() -> None:
    """Register all filesystem implementations provided by upathtools."""
    from upathtools.filesystems import DistributionFileSystem
    from upathtools.filesystems import FlatUnionFileSystem
    from upathtools.filesystems import MarkdownFileSystem
    from upathtools.filesystems import PackageFileSystem
    from upathtools.filesystems import SqliteFileSystem
    from upathtools.filesystems import UnionFileSystem
    from upathtools.filesystems import GistFileSystem
    from upathtools.filesystems import WikiFileSystem

    register_http_filesystems()
    register_async_local_filesystem()
    DistributionFileSystem.register_fs(clobber=True)
    FlatUnionFileSystem.register_fs(clobber=True)
    MarkdownFileSystem.register_fs(clobber=True)
    PackageFileSystem.register_fs(clobber=True)
    SqliteFileSystem.register_fs(clobber=True)
    UnionFileSystem.register_fs(clobber=True)
    GistFileSystem.register_fs(clobber=True)
    WikiFileSystem.register_fs(clobber=True)


__all__ = [
    "AsyncUPath",
    "JoinablePathLike",
    "UPath",
    "__version__",
    "fsspec_grep",
    "is_directory",
    "is_directory_sync",
    "list_files",
    "read_folder",
    "read_folder_as_text",
    "read_path",
    "register_all_filesystems",
    "register_async_local_filesystem",
    "register_http_filesystems",
    "to_upath",
    "upath_to_fs",
    "url_to_fs",
]
