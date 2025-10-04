"""Configuration models for filesystem implementations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Literal

import fsspec
from pydantic import BaseModel, ConfigDict
from upath import UPath


if TYPE_CHECKING:
    from fsspec import AbstractFileSystem


# Define filesystem categories as literals
FilesystemCategoryType = Literal["base", "archive", "transform", "aggregation", "wrapper"]


class FileSystemConfig(BaseModel):
    """Base configuration for filesystem implementations."""

    model_config = ConfigDict(extra="allow", use_attribute_docstrings=True)

    fs_type: str
    """Type of filesystem"""

    root_path: str | None = None
    """Root directory to restrict filesystem access to (applies dir:: modifier)"""

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
    def get_available_configs(cls) -> dict[str, type[FileSystemConfig]]:
        """Return all available filesystem configurations.

        Returns:
            Dictionary mapping fs_type values to configuration classes
        """
        result = {}
        for subclass in cls.__subclasses__():
            result.update(subclass.get_available_configs())
            if hasattr(subclass, "fs_type") and hasattr(subclass.fs_type, "__args__"):
                fs_type = subclass.fs_type.__args__[0]  # pyright: ignore
                result[fs_type] = subclass

        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FileSystemConfig:
        """Create appropriate config instance based on fs_type.

        Args:
            data: Dictionary containing configuration data with fs_type

        Returns:
            Instantiated configuration object of the appropriate type

        Raises:
            ValueError: If fs_type is missing or unknown
        """
        fs_type = data.get("fs_type")
        if not fs_type:
            msg = "fs_type must be specified"
            raise ValueError(msg)

        configs = cls.get_available_configs()
        if fs_type in configs:
            return configs[fs_type](**data)
        return cls(**data)

    def create_fs(self) -> AbstractFileSystem:
        """Create a filesystem instance based on this configuration.

        Returns:
            Instantiated filesystem with the proper configuration
        """
        fs_kwargs = self.model_dump(exclude={"fs_type", "root_path"})
        fs_kwargs = {k: v for k, v in fs_kwargs.items() if v is not None}
        fs = fsspec.filesystem(self.fs_type, **fs_kwargs)
        if self.root_path:
            return fs.chdir(self.root_path)

        return fs

    def create_upath(self, path: str = "/") -> UPath:
        """Create a UPath object for the specified path on this filesystem.

        Args:
            path: Path within the filesystem (defaults to root)

        Returns:
            UPath object for the specified path
        """
        fs = self.create_fs()
        return UPath(path, fs=fs)


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
