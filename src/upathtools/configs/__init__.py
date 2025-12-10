"""Configuration models for filesystem implementations and utilities."""

from __future__ import annotations

from typing import Annotated

from pydantic import Field

from upathtools.configs.base import FileSystemConfig, PathConfig, URIFileSystemConfig
from upathtools.configs.custom_fs_configs import CustomFilesystemConfig
from upathtools.configs.sandbox_fs_configs import SandboxFilesystemConfig
from upathtools.configs.fsspec_fs_configs import FsspecFilesystemConfig
from upathtools.configs.file_based_fs_configs import FileBasedFilesystemConfig
from upathtools.configs.remote_fs_configs import RemoteFilesystemConfig

# Combined union of all filesystem config types
FilesystemConfigType = Annotated[
    CustomFilesystemConfig
    | FsspecFilesystemConfig
    | URIFileSystemConfig
    | SandboxFilesystemConfig
    | RemoteFilesystemConfig
    | FileBasedFilesystemConfig,
    Field(discriminator="type"),
]

__all__ = [
    "CustomFilesystemConfig",
    "FileBasedFilesystemConfig",
    "FileSystemConfig",
    "FilesystemConfigType",
    "FsspecFilesystemConfig",
    "PathConfig",
    "RemoteFilesystemConfig",
    "SandboxFilesystemConfig",
    "URIFileSystemConfig",
]
