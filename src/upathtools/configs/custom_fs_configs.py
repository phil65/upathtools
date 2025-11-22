"""Configuration models for filesystem implementations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Literal

from pydantic import Field, SecretStr
from upath import UPath  # noqa: TC002

from upathtools.configs.base import (
    FilesystemCategoryType,  # noqa: TC001
    FileSystemConfig,
)


if TYPE_CHECKING:
    from pydantic import SecretStr


class CliFilesystemConfig(FileSystemConfig):
    """Configuration for CLI filesystem."""

    fs_type: Literal["cli"] = Field("cli", init=False)
    """CLI filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    shell: bool = Field(default=False, title="Shell mode")
    """Whether to use shell mode for command execution"""

    encoding: str = Field(
        default="utf-8",
        title="Output encoding",
        examples=["utf-8"],
        pattern=r"^[a-zA-Z0-9]([a-zA-Z0-9\-_])*$",
    )
    """Output encoding for command results"""


class DistributionFilesystemConfig(FileSystemConfig):
    """Configuration for Distribution filesystem."""

    fs_type: Literal["distribution"] = Field("distribution", init=False)
    """Distribution filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"


class FlatUnionFilesystemConfig(FileSystemConfig):
    """Configuration for FlatUnion filesystem."""

    fs_type: Literal["flatunion"] = Field("flatunion", init=False)
    """FlatUnion filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "aggregation"

    filesystems: list[str] = Field(
        title="Filesystem Identifiers", examples=[["fs1", "fs2", "fs3"]], min_length=1
    )
    """List of filesystem identifiers to include in the union"""


class GistFilesystemConfig(FileSystemConfig):
    """Configuration for GitHub Gist filesystem."""

    fs_type: Literal["gist"] = Field("gist", init=False)
    """Gist filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    gist_id: str | None = Field(
        default=None,
        title="Gist ID",
        examples=["abc123"],
        pattern=r"^[a-f0-9]+$",
        min_length=1,
    )
    """Specific gist ID to access"""

    username: str | None = Field(
        default=None,
        title="GitHub Username",
        examples=["phil65"],
        pattern=r"^[a-zA-Z0-9]([a-zA-Z0-9\-])*[a-zA-Z0-9]$|^[a-zA-Z0-9]$",
        min_length=1,
        max_length=39,
    )
    """GitHub username for listing all gists"""

    token: SecretStr | None = Field(
        default=None, title="GitHub Token", examples=["abc123"]
    )
    """GitHub personal access token for authentication"""

    sha: str | None = Field(
        default=None,
        title="Gist Revision",
        examples=["abc123"],
        pattern=r"^[a-f0-9]+$",
        min_length=1,
    )
    """Specific revision of a gist"""

    timeout: int | None = Field(default=None, ge=0, title="Connection Timeout")
    """Connection timeout in seconds"""


class HttpFilesystemConfig(FileSystemConfig):
    """Configuration for HTTP filesystem."""

    fs_type: Literal["http"] = Field("http", init=False)
    """HTTP filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    simple_links: bool = True
    """Whether to extract links using simpler regex patterns"""

    block_size: int | None = Field(
        default=None, gt=0, title="Block Size", examples=[8192, 65536]
    )
    """Block size for reading files in chunks"""

    same_scheme: bool = True
    """Whether to keep the same scheme (http/https) when following links"""

    size_policy: str | None = Field(
        default=None, title="Size Policy", examples=["head", "get"]
    )
    """Policy for determining file size ('head' or 'get')"""

    cache_type: str = Field(
        default="bytes", title="Cache Type", examples=["bytes", "readahead", "blockcache"]
    )
    """Type of cache to use for file contents"""

    encoded: bool = False
    """Whether URLs are already encoded"""


class MarkdownFilesystemConfig(FileSystemConfig):
    """Configuration for Markdown filesystem."""

    fo: UPath = Field(title="Markdown File Path", examples=["/path/to/file.md"])
    """Path to markdown file"""

    fs_type: Literal["md"] = Field("md", init=False)
    """Markdown filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"

    target_protocol: str | None = Field(
        default=None, title="Target Protocol", examples=["file", "s3", "http"]
    )
    """Protocol for source file"""

    target_options: dict[str, Any] | None = Field(
        default=None, title="Target Protocol Options"
    )
    """Options for target protocol"""


class ModuleFilesystemConfig(FileSystemConfig):
    """Configuration for Module filesystem."""

    fs_type: Literal["mod"] = Field("mod", init=False)
    """Module filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"

    fo: UPath = Field(title="Python File Path", examples=["/path/to/module.py"])
    """Path to Python file"""

    target_protocol: str | None = Field(
        default=None, title="Target Protocol", examples=["file", "s3", "http"]
    )
    """Protocol for source file"""

    target_options: dict[str, Any] | None = Field(
        default=None, title="Target Protocol Options"
    )
    """Options for target protocol"""


class PackageFilesystemConfig(FileSystemConfig):
    """Configuration for Package filesystem."""

    fs_type: Literal["pkg"] = Field("pkg", init=False)
    """Package filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"

    package: str = Field(
        title="Package Name",
        examples=["upathtools"],
        pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$",
        min_length=1,
    )
    """Name of the package to browse"""


class PythonAstFilesystemConfig(FileSystemConfig):
    """Configuration for Python AST filesystem."""

    fs_type: Literal["ast"] = Field("ast", init=False)
    """Python AST filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"

    fo: UPath = Field(title="Python File Path", examples=["/path/to/script.py"])
    """Path to Python file"""

    target_protocol: str | None = Field(
        default=None, title="Target Protocol", examples=["file", "s3", "http"]
    )
    """Protocol for source file"""

    target_options: dict[str, Any] | None = Field(
        default=None, title="Target Protocol Options"
    )
    """Options for target protocol"""


class UnionFilesystemConfig(FileSystemConfig):
    """Configuration for Union filesystem."""

    fs_type: Literal["union"] = Field("union", init=False)
    """Union filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "aggregation"

    filesystems: dict[str, Any] = Field(title="Filesystem Configurations")
    """Dictionary mapping protocol names to filesystem configurations"""


class WikiFilesystemConfig(FileSystemConfig):
    """Configuration for GitHub Wiki filesystem."""

    fs_type: Literal["wiki"] = Field("wiki", init=False)
    """Wiki filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    owner: str = Field(
        title="Repository Owner",
        examples=["microsoft", "facebook", "phil65"],
        pattern=r"^[a-zA-Z0-9]([a-zA-Z0-9\-])*[a-zA-Z0-9]$|^[a-zA-Z0-9]$",
        min_length=1,
        max_length=39,
    )
    """GitHub repository owner/organization"""

    repo: str = Field(
        title="Repository Name",
        examples=["vscode", "react", "upathtools"],
        pattern=r"^[a-zA-Z0-9\._\-]+$",
        min_length=1,
        max_length=100,
    )
    """GitHub repository name"""

    token: SecretStr | None = Field(default=None, title="GitHub Token")
    """GitHub personal access token for authentication"""

    timeout: int | None = Field(
        default=None, ge=0, title="Connection Timeout", examples=[30, 60, 120]
    )
    """Connection timeout in seconds"""
