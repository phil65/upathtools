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
from upath import registry

from upathtools.helpers import to_upath
from upathtools.async_ops import read_path, read_folder, list_files, read_folder_as_text
from upathtools.async_upath import AsyncUPath
from upathtools.filesystems.httpx_fs import HttpPath, HTTPFileSystem
from upathtools.filesystems.cli_fs import CliFS, CliPath
from upathtools.filesystems.distribution_fs import DistributionFS, DistributionPath
from upathtools.filesystems.flat_union_fs import FlatUnionFileSystem, FlatUnionPath
from upathtools.filesystems.markdown_fs import MarkdownFS, MarkdownPath
from upathtools.filesystems.module_fs import ModuleFS, ModulePath
from upathtools.filesystems.package_fs import PackageFS, PackagePath
from upathtools.filesystems.python_ast_fs import PythonAstPath, PythonAstFS
from upathtools.filesystems.union_fs import UnionFileSystem, UnionPath
from upathtools.filesystems.gist_fs import GistFileSystem, GistPath
from upathtools.filesystems.wiki_fs import WikiFileSystem, WikiPath


def register_http_filesystems():
    """Register HTTP filesystems."""
    register_implementation("http", HTTPFileSystem, clobber=True)
    registry.register_implementation("http", HttpPath, clobber=True)
    register_implementation("https", HTTPFileSystem, clobber=True)
    registry.register_implementation("https", HttpPath, clobber=True)


def register_all_filesystems():
    """Register all filesystem implementations provided by upathtools."""
    register_http_filesystems()
    register_implementation("cli", CliFS, clobber=True)
    registry.register_implementation("cli", CliPath, clobber=True)

    register_implementation("distribution", DistributionFS, clobber=True)
    registry.register_implementation("distribution", DistributionPath, clobber=True)

    register_implementation("flatunion", FlatUnionFileSystem, clobber=True)
    registry.register_implementation("flatunion", FlatUnionPath, clobber=True)

    register_implementation("md", MarkdownFS, clobber=True)
    registry.register_implementation("md", MarkdownPath, clobber=True)

    register_implementation("mod", ModuleFS, clobber=True)
    registry.register_implementation("mod", ModulePath, clobber=True)

    register_implementation("pkg", PackageFS, clobber=True)
    registry.register_implementation("pkg", PackagePath, clobber=True)

    register_implementation("ast", PythonAstFS, clobber=True)
    registry.register_implementation("ast", PythonAstPath, clobber=True)

    register_implementation("union", UnionFileSystem, clobber=True)
    registry.register_implementation("union", UnionPath, clobber=True)

    register_implementation("gist", GistFileSystem, clobber=True)
    registry.register_implementation("gist", GistPath, clobber=True)

    register_implementation("wiki", WikiFileSystem, clobber=True)
    registry.register_implementation("wiki", WikiPath, clobber=True)


__all__ = [
    "AsyncUPath",
    "CliFS",
    "CliPath",
    "DistributionFS",
    "DistributionPath",
    "FlatUnionFileSystem",
    "FlatUnionPath",
    "GistFileSystem",
    "GistPath",
    "HTTPFileSystem",
    "HttpPath",
    "MarkdownFS",
    "MarkdownPath",
    "ModuleFS",
    "ModulePath",
    "PackageFS",
    "PackagePath",
    "PythonAstFS",
    "PythonAstPath",
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
]
