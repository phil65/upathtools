"""Configuration models for filesystem implementations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Literal, overload
from urllib.parse import urlparse

import fsspec
from fsspec.asyn import AsyncFileSystem
from fsspec.implementations.asyn_wrapper import AsyncFileSystemWrapper
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

    root_path: str | None = None
    """Root directory to restrict filesystem access to (wraps in DirFileSystem)."""

    cwd: str | None = None
    """Working directory for relative path operations (uses fs.chdir)."""

    cached: bool = False
    """Whether to wrap in CachingFileSystem."""

    _category: ClassVar[FilesystemCategoryType] = "base"
    """Classification of the filesystem type"""

    @property
    def category(self) -> FilesystemCategoryType:
        """Get the category of this filesystem."""
        return self._category

    @property
    def is_typically_layered(self) -> bool:
        """Whether this filesystem type is typically used as a layer on top of another."""
        return self.category in {"archive", "transform", "wrapper"}

    @property
    def requires_target_fs(self) -> bool:
        """Whether this filesystem type typically requires a target filesystem."""
        return self.category in {"archive", "transform"}

    @classmethod
    def get_available_configs(cls) -> dict[str, type[FileSystemConfig]]:  # type: ignore[valid-type]
        """Return all available filesystem configurations.

        Returns:
            Dictionary mapping type values to configuration classes
        """
        result = {}
        for subclass in cls.__subclasses__():
            result.update(subclass.get_available_configs())
            if hasattr(subclass.type, "__args__"):
                fs_type = subclass.type.__args__[0]  # pyright: ignore
                result[fs_type] = subclass

        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FileSystemConfig:
        """Create appropriate config instance based on type.

        Args:
            data: Dictionary containing configuration data with type

        Returns:
            Instantiated configuration object of the appropriate type

        Raises:
            ValueError: If type is missing or unknown
        """
        fs_type = data.get("type")
        if not fs_type:
            msg = "type must be specified"
            raise ValueError(msg)

        configs = cls.get_available_configs()
        if fs_type in configs:
            return configs[fs_type](**data)  # type: ignore[misc]
        return cls(**data)

    @overload
    def create_fs(self, ensure_async: Literal[False] = ...) -> AbstractFileSystem: ...

    @overload
    def create_fs(self, ensure_async: Literal[True]) -> AsyncFileSystem: ...

    def create_fs(self, ensure_async: bool = False) -> AbstractFileSystem:
        """Create a filesystem instance based on this configuration.

        Returns:
            Instantiated filesystem with the proper configuration.
        """
        fs_kwargs = self.model_dump(exclude={"type", "root_path", "cwd", "cached"})
        fs_kwargs = {k: v for k, v in fs_kwargs.items() if v is not None}

        # Convert Pydantic types to plain Python types
        for key, value in fs_kwargs.items():
            if isinstance(value, SecretStr):
                fs_kwargs[key] = value.get_secret_value()
            elif isinstance(value, AnyUrl):
                fs_kwargs[key] = str(value)

        fs = fsspec.filesystem(self.type, **fs_kwargs)

        # Apply path prefix (DirFileSystem wrapper) - sandboxed, can't escape
        if self.root_path:
            fs = fsspec.filesystem("dir", path=self.root_path, fs=fs)

        # Apply cwd for relative path convenience
        if self.cwd:
            fs = fs.chdir(self.cwd)

        # Apply caching wrapper
        if self.cached:
            fs = fsspec.filesystem("filecache", fs=fs)
        if not isinstance(fs, AsyncFileSystem):
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
        return UPath(path or fs.root_marker, fs=fs)


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
        examples=["file:///path/to/docs", "s3://bucket/data", "https://example.com"],
    )
    """URI defining the resource location and protocol."""

    storage_options: dict[str, Any] = Field(default_factory=dict)
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

        if self.cwd:
            fs = fs.chdir(self.cwd)

        if self.cached:
            fs = fsspec.filesystem("filecache", fs=fs)
        if not isinstance(fs, AsyncFileSystem) and ensure_async:
            fs = AsyncFileSystemWrapper(fs)
        return fs


class PathConfig(BaseModel):
    """Configuration that combines a filesystem with a specific path."""

    model_config = ConfigDict(extra="forbid", use_attribute_docstrings=True)

    filesystem: FileSystemConfig
    """Configuration for the filesystem"""

    path: str = "/"
    """Path within the filesystem"""

    def create_upath(self) -> UPath:
        """Create a UPath object for this path on its filesystem."""
        return self.filesystem.create_upath(self.path)


if __name__ == "__main__":
    from upathtools.configs.fsspec_fs_configs import ZipFilesystemConfig

    zip_config = ZipFilesystemConfig(fo=UPath("C:/Users/phili/Downloads/tags.zip"))
    fs = zip_config.create_fs()
    print(fs.ls("tags/"))
