"""Configuration models for filesystem implementations and utilities."""

from __future__ import annotations

from typing import Annotated

from pydantic import Field

from upathtools.configs.base import FileSystemConfig, PathConfig, URIFileSystemConfig
from upathtools.configs.custom_fs_configs import CustomFilesystemConfig
from upathtools.configs.fsspec_fs_configs import FsspecFilesystemConfig

# Combined union of all filesystem config types
FilesystemConfigType = Annotated[
    CustomFilesystemConfig | FsspecFilesystemConfig | URIFileSystemConfig,
    Field(discriminator="type"),
]

__all__ = [
    "CustomFilesystemConfig",
    "FileSystemConfig",
    "FilesystemConfigType",
    "FsspecFilesystemConfig",
    "PathConfig",
    "URIFileSystemConfig",
]
