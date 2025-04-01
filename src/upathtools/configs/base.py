"""Configuration models for filesystem implementations."""

from __future__ import annotations

from typing import Any

import fsspec
from fsspec import AbstractFileSystem
from pydantic import BaseModel, ConfigDict


class FileSystemConfig(BaseModel):
    """Base configuration for filesystem implementations."""

    model_config = ConfigDict(extra="allow", use_attribute_docstrings=True)

    fs_type: str
    """Type of filesystem"""

    @classmethod
    def get_available_configs(cls) -> dict[str, type[FileSystemConfig]]:
        """Return all available filesystem configurations.

        Returns:
            Dictionary mapping fs_type values to configuration classes
        """
        result = {}
        for subclass in cls.__subclasses__():
            # Recursively collect subclasses of subclasses too
            result.update(subclass.get_available_configs())

            # Add this subclass if it has a literal fs_type
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

    def create_fs(self, path: str | None = None) -> AbstractFileSystem:
        """Create a filesystem instance based on this configuration.

        Args:
            path: Optional path to pass to the filesystem

        Returns:
            Instantiated filesystem with the proper configuration

        Raises:
            ImportError: If fsspec is not installed
            ValueError: If the filesystem type is not found
        """
        fs_kwargs = self.model_dump(exclude={"fs_type"})
        fs_kwargs = {k: v for k, v in fs_kwargs.items() if v is not None}
        return fsspec.filesystem(self.fs_type, **fs_kwargs)
