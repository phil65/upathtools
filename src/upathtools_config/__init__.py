"""Filesystem configuration.

This is a lightweight config-only package for fast imports.
For the actual filesystems, use `from upathtools import ...`.
"""

from typing import Annotated

from pydantic import Field

from upathtools_config.base import (
    FileSystemConfig,
    FilesystemCategoryType,
    PathConfig,
    URIFileSystemConfig,
)
from upathtools_config.custom_fs_configs import (
    AsyncLocalFilesystemConfig,
    BaseModelFilesystemConfig,
    BaseModelInstanceFilesystemConfig,
    CliFilesystemConfig,
    CustomFilesystemConfig,
    DistributionFilesystemConfig,
    FlatUnionFilesystemConfig,
    HttpFilesystemConfig,
    HttpxFilesystemConfig,
    MountsFilesystemConfig,
    OverlayFilesystemConfig,
    PackageFilesystemConfig,
    SkillsFilesystemConfig,
    UnionFilesystemConfig,
)
from upathtools_config.file_based_fs_configs import (
    FileBasedConfig,
    FileBasedFilesystemConfig,
    JsonSchemaFilesystemConfig,
    MarkdownFilesystemConfig,
    OpenApiFilesystemConfig,
    SqliteFilesystemConfig,
    TreeSitterFilesystemConfig,
)
from upathtools_config.fsspec_fs_configs import (
    ArrowFilesystemConfig,
    AzureBlobFilesystemConfig,
    DaskWorkerFilesystemConfig,
    DataFilesystemConfig,
    FTPFilesystemConfig,
    FsspecFilesystemConfig,
    GCSFilesystemConfig,
    GitFilesystemConfig,
    GithubFilesystemConfig,
    HadoopFilesystemConfig,
    HuggingFaceFilesystemConfig,
    JupyterFilesystemConfig,
    LibArchiveFilesystemConfig,
    LocalFilesystemConfig,
    MemoryFilesystemConfig,
    S3FilesystemConfig,
    SFTPFilesystemConfig,
    SMBFilesystemConfig,
    TarFilesystemConfig,
    WebdavFilesystemConfig,
    WebHDFSFilesystemConfig,
    ZipFilesystemConfig,
)
from upathtools_config.remote_fs_configs import (
    AppwriteFilesystemConfig,
    GistFilesystemConfig,
    GitLabFilesystemConfig,
    LinearFilesystemConfig,
    McpFilesystemConfig,
    McpToolsFilesystemConfig,
    NotionFilesystemConfig,
    RemoteFilesystemConfig,
    WikiFilesystemConfig,
)
from upathtools_config.sandbox_fs_configs import (
    BeamFilesystemConfig,
    DaytonaFilesystemConfig,
    E2BFilesystemConfig,
    MicrosandboxFilesystemConfig,
    ModalFilesystemConfig,
    SandboxFilesystemConfig,
    SRTFilesystemConfig,
    VercelFilesystemConfig,
)


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
    # Remote
    "AppwriteFilesystemConfig",
    # Fsspec
    "ArrowFilesystemConfig",
    # Custom
    "AsyncLocalFilesystemConfig",
    "AzureBlobFilesystemConfig",
    "BaseModelFilesystemConfig",
    "BaseModelInstanceFilesystemConfig",
    # Sandbox
    "BeamFilesystemConfig",
    "CliFilesystemConfig",
    "CustomFilesystemConfig",
    "DaskWorkerFilesystemConfig",
    "DataFilesystemConfig",
    "DaytonaFilesystemConfig",
    "DistributionFilesystemConfig",
    "E2BFilesystemConfig",
    "FTPFilesystemConfig",
    # File-based
    "FileBasedConfig",
    "FileBasedFilesystemConfig",
    # Base
    "FileSystemConfig",
    "FilesystemCategoryType",
    "FilesystemConfigType",
    "FlatUnionFilesystemConfig",
    "FsspecFilesystemConfig",
    "GCSFilesystemConfig",
    "GistFilesystemConfig",
    "GitFilesystemConfig",
    "GitLabFilesystemConfig",
    "GithubFilesystemConfig",
    "HadoopFilesystemConfig",
    "HttpFilesystemConfig",
    "HttpxFilesystemConfig",
    "HuggingFaceFilesystemConfig",
    "JsonSchemaFilesystemConfig",
    "JupyterFilesystemConfig",
    "LibArchiveFilesystemConfig",
    "LinearFilesystemConfig",
    "LocalFilesystemConfig",
    "MarkdownFilesystemConfig",
    "McpFilesystemConfig",
    "McpToolsFilesystemConfig",
    "MemoryFilesystemConfig",
    "MicrosandboxFilesystemConfig",
    "ModalFilesystemConfig",
    "MountsFilesystemConfig",
    "NotionFilesystemConfig",
    "OpenApiFilesystemConfig",
    "OverlayFilesystemConfig",
    "PackageFilesystemConfig",
    "PathConfig",
    "RemoteFilesystemConfig",
    "S3FilesystemConfig",
    "SFTPFilesystemConfig",
    "SMBFilesystemConfig",
    "SRTFilesystemConfig",
    "SandboxFilesystemConfig",
    "SkillsFilesystemConfig",
    "SqliteFilesystemConfig",
    "TarFilesystemConfig",
    "TreeSitterFilesystemConfig",
    "URIFileSystemConfig",
    "UnionFilesystemConfig",
    "VercelFilesystemConfig",
    "WebHDFSFilesystemConfig",
    "WebdavFilesystemConfig",
    "WikiFilesystemConfig",
    "ZipFilesystemConfig",
]
