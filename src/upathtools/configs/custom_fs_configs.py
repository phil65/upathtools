"""Configuration models for filesystem implementations."""

from __future__ import annotations

from typing import Any, ClassVar, Literal

from pydantic import Field
from upath import UPath  # noqa: TC002

from upathtools.configs.base import (
    FilesystemCategoryType,  # noqa: TC001
    FileSystemConfig,
)


class CliFilesystemConfig(FileSystemConfig):
    """Configuration for CLI filesystem."""

    fs_type: Literal["cli"] = Field("cli", init=False)
    """CLI filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"
    """Classification of the filesystem type"""

    shell: bool = False
    """Whether to use shell mode for command execution"""

    encoding: str = "utf-8"
    """Output encoding for command results"""


class DistributionFilesystemConfig(FileSystemConfig):
    """Configuration for Distribution filesystem."""

    fs_type: Literal["distribution"] = Field("distribution", init=False)
    """Distribution filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"
    """Classification of the filesystem type"""


class FlatUnionFilesystemConfig(FileSystemConfig):
    """Configuration for FlatUnion filesystem."""

    fs_type: Literal["flatunion"] = Field("flatunion", init=False)
    """FlatUnion filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "aggregation"
    """Classification of the filesystem type"""

    filesystems: list[str]
    """List of filesystem identifiers to include in the union"""


class GistFilesystemConfig(FileSystemConfig):
    """Configuration for GitHub Gist filesystem."""

    fs_type: Literal["gist"] = Field("gist", init=False)
    """Gist filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"
    """Classification of the filesystem type"""

    gist_id: str | None = None
    """Specific gist ID to access"""

    username: str | None = None
    """GitHub username for listing all gists"""

    token: str | None = None
    """GitHub personal access token for authentication"""

    sha: str | None = None
    """Specific revision of a gist"""

    timeout: int | None = None
    """Connection timeout in seconds"""


class HttpFilesystemConfig(FileSystemConfig):
    """Configuration for HTTP filesystem."""

    fs_type: Literal["http"] = Field("http", init=False)
    """HTTP filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"
    """Classification of the filesystem type"""

    simple_links: bool = True
    """Whether to extract links using simpler regex patterns"""

    block_size: int | None = None
    """Block size for reading files in chunks"""

    same_scheme: bool = True
    """Whether to keep the same scheme (http/https) when following links"""

    size_policy: str | None = None
    """Policy for determining file size ('head' or 'get')"""

    cache_type: str = "bytes"
    """Type of cache to use for file contents"""

    encoded: bool = False
    """Whether URLs are already encoded"""


class MarkdownFilesystemConfig(FileSystemConfig):
    """Configuration for Markdown filesystem."""

    fo: UPath
    """Path to markdown file"""

    fs_type: Literal["md"] = Field("md", init=False)
    """Markdown filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"
    """Classification of the filesystem type"""

    target_protocol: str | None = None
    """Protocol for source file"""

    target_options: dict[str, Any] | None = None
    """Options for target protocol"""


class ModuleFilesystemConfig(FileSystemConfig):
    """Configuration for Module filesystem."""

    fs_type: Literal["mod"] = Field("mod", init=False)
    """Module filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"
    """Classification of the filesystem type"""

    fo: UPath
    """Path to Python file"""

    target_protocol: str | None = None
    """Protocol for source file"""

    target_options: dict[str, Any] | None = None
    """Options for target protocol"""


class PackageFilesystemConfig(FileSystemConfig):
    """Configuration for Package filesystem."""

    fs_type: Literal["pkg"] = Field("pkg", init=False)
    """Package filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"
    """Classification of the filesystem type"""

    package: str
    """Name of the package to browse"""


class PythonAstFilesystemConfig(FileSystemConfig):
    """Configuration for Python AST filesystem."""

    fs_type: Literal["ast"] = Field("ast", init=False)
    """Python AST filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"
    """Classification of the filesystem type"""

    fo: UPath
    """Path to Python file"""

    target_protocol: str | None = None
    """Protocol for source file"""

    target_options: dict[str, Any] | None = None
    """Options for target protocol"""


class UnionFilesystemConfig(FileSystemConfig):
    """Configuration for Union filesystem."""

    fs_type: Literal["union"] = Field("union", init=False)
    """Union filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "aggregation"
    """Classification of the filesystem type"""

    filesystems: dict[str, Any]
    """Dictionary mapping protocol names to filesystem configurations"""


class WikiFilesystemConfig(FileSystemConfig):
    """Configuration for GitHub Wiki filesystem."""

    fs_type: Literal["wiki"] = Field("wiki", init=False)
    """Wiki filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"
    """Classification of the filesystem type"""

    owner: str
    """GitHub repository owner/organization"""

    repo: str
    """GitHub repository name"""

    token: str | None = None
    """GitHub personal access token for authentication"""

    timeout: int | None = None
    """Connection timeout in seconds"""
