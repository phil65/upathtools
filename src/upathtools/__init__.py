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

from upathtools.helpers import to_upath, upath_to_fs
from upathtools.async_ops import read_path, read_folder, list_files, read_folder_as_text
from upathtools.async_upath import AsyncUPath
from upathtools.filesystems.httpx_fs import HttpPath, HTTPFileSystem
from upathtools.filesystems import DistributionFileSystem, DistributionPath
from upathtools.filesystems import FlatUnionFileSystem, FlatUnionPath
from upathtools.filesystems import MarkdownFileSystem, MarkdownPath
from upathtools.filesystems import PackageFileSystem, PackagePath
from upathtools.filesystems import SqliteFileSystem, SqlitePath
from upathtools.filesystems import UnionFileSystem, UnionPath
from upathtools.filesystems import OverlayFileSystem, OverlayPath
from upathtools.filesystems import GistFileSystem, GistPath
from upathtools.filesystems import WikiFileSystem, WikiPath
from upath.types import JoinablePathLike


def register_http_filesystems() -> None:
    """Register HTTP filesystems."""
    register_implementation("http", HTTPFileSystem, clobber=True)
    registry.register_implementation("http", HttpPath, clobber=True)
    register_implementation("https", HTTPFileSystem, clobber=True)
    registry.register_implementation("https", HttpPath, clobber=True)


def register_all_filesystems() -> None:
    """Register all filesystem implementations provided by upathtools."""
    register_http_filesystems()

    register_implementation("distribution", DistributionFileSystem, clobber=True)
    registry.register_implementation("distribution", DistributionPath, clobber=True)

    register_implementation("flatunion", FlatUnionFileSystem, clobber=True)
    registry.register_implementation("flatunion", FlatUnionPath, clobber=True)

    register_implementation("md", MarkdownFileSystem, clobber=True)
    registry.register_implementation("md", MarkdownPath, clobber=True)

    register_implementation("pkg", PackageFileSystem, clobber=True)
    registry.register_implementation("pkg", PackagePath, clobber=True)

    register_implementation("sqlite", SqliteFileSystem, clobber=True)
    registry.register_implementation("sqlite", SqlitePath, clobber=True)

    register_implementation("union", UnionFileSystem, clobber=True)
    registry.register_implementation("union", UnionPath, clobber=True)

    register_implementation("gist", GistFileSystem, clobber=True)
    registry.register_implementation("gist", GistPath, clobber=True)

    register_implementation("wiki", WikiFileSystem, clobber=True)
    registry.register_implementation("wiki", WikiPath, clobber=True)


__all__ = [
    "AsyncUPath",
    "DistributionFileSystem",
    "DistributionPath",
    "FlatUnionFileSystem",
    "FlatUnionPath",
    "GistFileSystem",
    "GistPath",
    "HTTPFileSystem",
    "HttpPath",
    "JoinablePathLike",
    "MarkdownFileSystem",
    "MarkdownPath",
    "OverlayFileSystem",
    "OverlayPath",
    "PackageFileSystem",
    "PackagePath",
    "SqliteFileSystem",
    "SqlitePath",
    "UPath",
    "UnionFileSystem",
    "UnionPath",
    "WikiFileSystem",
    "WikiPath",
    "__version__",
    "list_files",
    "read_folder",
    "read_folder_as_text",
    "read_path",
    "register_all_filesystems",
    "register_http_filesystems",
    "to_upath",
    "upath_to_fs",
]
