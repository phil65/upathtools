"""Configuration models for filesystem implementations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Literal, overload
from urllib.parse import urlparse

import fsspec
from fsspec.asyn import AsyncFileSystem
from fsspec.implementations.asyn_wrapper import AsyncFileSystemWrapper
from fsspec.implementations.cached import WholeFileCacheFileSystem
from pydantic import AnyUrl, BaseModel, ConfigDict, Field, SecretStr
from upath import UPath


if TYPE_CHECKING:
    from fsspec import AbstractFileSystem


# Define filesystem categories as literals
FilesystemCategoryType = Literal[
    "base", "archive", "transform", "aggregation", "wrapper", "sandbox"
]


class FileSystemConfig(BaseModel):
    """Base configuration for filesystem implementations."""

    model_config = ConfigDict(extra="allow", use_attribute_docstrings=True)

    type: str
    """Type of filesystem"""

    root_path: str | None = Field(
        default=None,
        title="Root Path",
        examples=["/data", "/app/uploads", "C:\\Documents"],
    )
    """Root directory to restrict filesystem access to (wraps in DirFileSystem)."""

    cached: bool = Field(default=False, title="Enable Caching")
    """Whether to wrap in CachingFileSystem."""

    _category: ClassVar[FilesystemCategoryType] = "base"
    """Classification of the filesystem type"""

    @property
    def category(self) -> FilesystemCategoryType:
        """Get the category of this filesystem."""
        return self._category

    @overload
    def create_fs(self, ensure_async: Literal[False] = ...) -> AbstractFileSystem: ...

    @overload
    def create_fs(self, ensure_async: Literal[True]) -> AsyncFileSystem: ...

    def create_fs(self, ensure_async: bool = False) -> AbstractFileSystem:
        """Create a filesystem instance based on this configuration.

        Returns:
            Instantiated filesystem with the proper configuration.
        """
        fs_kwargs = self.model_dump(exclude={"type", "root_path", "cached"}, exclude_none=True)
        for key, value in fs_kwargs.items():
            if isinstance(value, SecretStr):
                fs_kwargs[key] = value.get_secret_value()
            elif isinstance(value, AnyUrl):
                fs_kwargs[key] = str(value)

        fs = fsspec.filesystem(self.type, **fs_kwargs)
        # Apply path prefix (DirFileSystem wrapper) - sandboxed, can't escape
        if self.root_path:
            fs = fsspec.filesystem("dir", path=self.root_path, fs=fs)
        # Apply caching wrapper

        if self.cached:
            fs = WholeFileCacheFileSystem(fs=fs)
        if not isinstance(fs, AsyncFileSystem) and ensure_async:
            fs = AsyncFileSystemWrapper(fs)
        return fs

    def create_upath(self, path: str | None = None) -> UPath:
        """Create a UPath object for the specified path on this filesystem.

        Args:
            path: Path within the filesystem (defaults to root)

        Returns:
            UPath object for the specified path
        """
        fs = self.create_fs()
        from upathtools.filesystems.base import BaseAsyncFileSystem, BaseFileSystem

        if isinstance(fs, BaseFileSystem | BaseAsyncFileSystem):
            return fs.get_upath(path)
        p = UPath(path or fs.root_marker)
        p._fs_cached = fs  # pyright: ignore[reportAttributeAccessIssue]
        return p


class URIFileSystemConfig(FileSystemConfig):
    """Generic filesystem config using URI and storage_options.

    This provides a simpler, more concise way to configure filesystems
    when you don't need the typed fields of specific configs.

    Example:
        ```python
        config = URIFileSystemConfig(
            uri="s3://my-bucket/data",
            storage_options={"key": "...", "secret": "..."},
        )
        fs = config.create_fs()
        ```
    """

    type: Literal["uri"] = Field("uri", init=False)
    """URI-based filesystem type."""

    uri: str = Field(
        title="Resource URI",
        examples=["file:///path/to/docs", "s3://bucket/data", "https://example.com"],
    )
    """URI defining the resource location and protocol."""

    storage_options: dict[str, Any] = Field(
        default_factory=dict,
        title="Storage Options",
        examples=[{"key": "access_key", "secret": "secret_key"}, {"token": "auth_token"}],
    )
    """Protocol-specific storage options passed to fsspec."""

    @overload
    def create_fs(self, ensure_async: Literal[False] = ...) -> AbstractFileSystem: ...

    @overload
    def create_fs(self, ensure_async: Literal[True]) -> AsyncFileSystem: ...

    def create_fs(self, ensure_async: bool = False) -> AbstractFileSystem:
        """Create filesystem from URI and storage options.

        ensure_async: If True, ensure the filesystem is async.
        """
        # Parse protocol from URI
        parsed = urlparse(self.uri)
        protocol = parsed.scheme or "file"

        # Build path from URI (handle file:// specially)
        if protocol == "file":
            path = parsed.path
        else:
            # For remote protocols, include netloc + path
            path = f"{parsed.netloc}{parsed.path}" if parsed.netloc else parsed.path

        fs = fsspec.filesystem(protocol, **self.storage_options)
        # Apply root_path restriction if set, otherwise use URI path
        effective_root = self.root_path or path
        if effective_root:
            fs = fsspec.filesystem("dir", path=effective_root, fs=fs)
        if self.cached:
            fs = WholeFileCacheFileSystem(fs=fs)
        if not isinstance(fs, AsyncFileSystem) and ensure_async:
            fs = AsyncFileSystemWrapper(fs)
        return fs


class PathConfig(BaseModel):
    """Configuration that combines a filesystem with a specific path."""

    model_config = ConfigDict(
        extra="forbid",
        use_attribute_docstrings=True,
        json_schema_extra={"x-doc-title": "Path Configuration"},
    )

    filesystem: FileSystemConfig = Field(title="Filesystem Configuration")
    """Configuration for the filesystem"""

    path: str = Field(
        default="/",
        title="Path",
        examples=["/", "/data", "subfolder/files"],
    )
    """Path within the filesystem"""

    def create_upath(self) -> UPath:
        """Create a UPath object for this path on its filesystem."""
        return self.filesystem.create_upath(self.path)


if __name__ == "__main__":
    from upathtools_config.fsspec_fs_configs import ZipFilesystemConfig

    zip_config = ZipFilesystemConfig(fo=UPath("C:/Users/phili/Downloads/tags.zip"))
    fs = zip_config.create_fs()
