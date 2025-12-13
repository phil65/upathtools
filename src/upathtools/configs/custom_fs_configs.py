"""Configuration models for filesystem implementations."""

from __future__ import annotations

from typing import Any, ClassVar, Literal

from pydantic import ConfigDict, Field
from upath import UPath  # noqa: TC002

from upathtools.configs.base import (
    FilesystemCategoryType,  # noqa: TC001
    FileSystemConfig,
)


class CliFilesystemConfig(FileSystemConfig):
    """Configuration for CLI filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "CLI Configuration"})

    type: Literal["cli"] = Field("cli", init=False)
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

    model_config = ConfigDict(json_schema_extra={"title": "Distribution Configuration"})

    type: Literal["distribution"] = Field("distribution", init=False)
    """Distribution filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"


class FlatUnionFilesystemConfig(FileSystemConfig):
    """Configuration for FlatUnion filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "Flat Union Configuration"})

    type: Literal["flatunion"] = Field("flatunion", init=False)
    """FlatUnion filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "aggregation"

    filesystems: list[str] = Field(
        title="Filesystem Identifiers", examples=[["fs1", "fs2", "fs3"]], min_length=1
    )
    """List of filesystem identifiers to include in the union"""


class HttpFilesystemConfig(FileSystemConfig):
    """Configuration for HTTP filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "HTTP Configuration"})

    type: Literal["http"] = Field("http", init=False)
    """HTTP filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    simple_links: bool = Field(default=True, title="Simple Links")
    """Whether to extract links using simpler regex patterns"""

    block_size: int | None = Field(default=None, gt=0, title="Block Size", examples=[8192, 65536])
    """Block size for reading files in chunks"""

    same_scheme: bool = Field(default=True, title="Same Scheme")
    """Whether to keep the same scheme (http/https) when following links"""

    size_policy: str | None = Field(default=None, title="Size Policy", examples=["head", "get"])
    """Policy for determining file size ('head' or 'get')"""

    cache_type: str = Field(
        default="bytes", title="Cache Type", examples=["bytes", "readahead", "blockcache"]
    )
    """Type of cache to use for file contents"""

    encoded: bool = Field(default=False, title="Encoded URLs")
    """Whether URLs are already encoded"""


class PackageFilesystemConfig(FileSystemConfig):
    """Configuration for Package filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "Package Configuration"})

    type: Literal["pkg"] = Field("pkg", init=False)
    """Package filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"

    package: str = Field(
        title="Package Name",
        examples=["upathtools"],
        pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$",
        min_length=1,
    )
    """Name of the package to browse"""


class UnionFilesystemConfig(FileSystemConfig):
    """Configuration for Union filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "Union Configuration"})

    type: Literal["union"] = Field("union", init=False)
    """Union filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "aggregation"

    filesystems: dict[str, Any] = Field(title="Filesystem Configurations")
    """Dictionary mapping protocol names to filesystem configurations"""


class BaseModelFilesystemConfig(FileSystemConfig):
    """Configuration for Pydantic BaseModel schema filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "BaseModel Configuration"})

    type: Literal["basemodel"] = Field("basemodel", init=False)
    """BaseModel filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"

    model: str = Field(
        title="Model Import Path",
        examples=["mypackage.MyModel", "pydantic.BaseModel"],
        pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$",
        min_length=1,
    )
    """BaseModel class import path"""


class HttpxFilesystemConfig(FileSystemConfig):
    """Configuration for HTTPX-based HTTP filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "HTTPX Configuration"})

    type: Literal["httpx"] = Field("httpx", init=False)
    """HTTPX filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    simple_links: bool = Field(default=True, title="Simple Links")
    """Whether to extract links using simpler regex patterns"""

    block_size: int | None = Field(default=None, gt=0, title="Block Size", examples=[8192, 65536])
    """Block size for reading files in chunks"""

    same_scheme: bool = Field(default=True, title="Same Scheme")
    """Whether to keep the same scheme (http/https) when following links"""

    size_policy: str | None = Field(default=None, title="Size Policy", examples=["head", "get"])
    """Policy for determining file size ('head' or 'get')"""

    cache_type: str = Field(
        default="bytes", title="Cache Type", examples=["bytes", "readahead", "blockcache"]
    )
    """Type of cache to use for file contents"""

    encoded: bool = Field(default=False, title="Encoded URLs")
    """Whether URLs are already encoded"""

    timeout: int | None = Field(default=None, ge=0, title="Request Timeout", examples=[30, 60, 120])
    """HTTP request timeout in seconds"""


class SkillsFilesystemConfig(FileSystemConfig):
    """Configuration for Skills filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "Skills Configuration"})

    type: Literal["skills"] = Field("skills", init=False)
    """Skills filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"

    skills_dir: UPath | None = Field(
        default=None,
        title="Skills Directory",
        examples=["/path/to/skills", "~/my-skills"],
    )
    """Directory containing skill definitions"""


class BaseModelInstanceFilesystemConfig(FileSystemConfig):
    """Configuration for Pydantic BaseModel instance filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "BaseModel Instance Configuration"})

    type: Literal["basemodel_instance"] = Field("basemodel_instance", init=False)
    """BaseModel instance filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"

    instance: str = Field(
        title="Model Instance Path",
        examples=["mypackage.model_instance", "app.config.settings"],
        pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$",
        min_length=1,
    )
    """BaseModel instance import path"""


class AsyncLocalFilesystemConfig(FileSystemConfig):
    """Configuration for async local filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "Async Local Configuration"})

    type: Literal["asynclocal"] = Field("asynclocal", init=False)
    """Async local filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    auto_mkdir: bool = Field(default=False, title="Auto Create Directories")
    """Automatically create parent directories on write"""


class OverlayFilesystemConfig(FileSystemConfig):
    """Configuration for overlay filesystem with copy-on-write semantics."""

    model_config = ConfigDict(json_schema_extra={"title": "Overlay Configuration"})

    type: Literal["overlay"] = Field("overlay", init=False)
    """Overlay filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "aggregation"

    filesystems: list[str] = Field(
        title="Filesystem Identifiers",
        examples=[["writable_fs", "readonly_fs"]],
        min_length=1,
    )
    """List of filesystem identifiers, first is writable upper layer"""


# class SkillsFilesystemConfig(FileSystemConfig):
#     """Configuration for Skills filesystem."""

#     type_: Literal["skills"] = Field("skills", init=False)
#     """Skills filesystem type"""

#     _category: ClassVar[FilesystemCategoryType] = "wrapper"

#     wrapped_fs: str = Field(
#         title="Wrapped Filesystem",
#         examples=["file", "s3", "gcs"],
#         min_length=1,
#     )
#     """Type of filesystem to wrap"""

#     skills_dir: UPath | None = Field(
#         default=None,
#         title="Skills Directory",
#         examples=["/path/to/skills", "~/my-skills"],
#     )
#     """Directory containing skill definitions"""


CustomFilesystemConfig = (
    AsyncLocalFilesystemConfig
    | BaseModelFilesystemConfig
    | BaseModelInstanceFilesystemConfig
    | CliFilesystemConfig
    | DistributionFilesystemConfig
    | FlatUnionFilesystemConfig
    | HttpFilesystemConfig
    | HttpxFilesystemConfig
    | OverlayFilesystemConfig
    | PackageFilesystemConfig
    | SkillsFilesystemConfig
    | UnionFilesystemConfig
)
"""Union of all custom filesystem configurations."""
