"""Configuration models for filesystem implementations and utilities."""

from __future__ import annotations

from typing import Annotated

from pydantic import Field

from upathtools.configs.base import FileSystemConfig, PathConfig
from upathtools.configs.custom_fs_configs import (
    CliFilesystemConfig,
    DistributionFilesystemConfig,
    FlatUnionFilesystemConfig,
    GistFilesystemConfig,
    HttpFilesystemConfig,
    MarkdownFilesystemConfig,
    ModuleFilesystemConfig,
    PackageFilesystemConfig,
    PythonAstFilesystemConfig,
    UnionFilesystemConfig,
    WikiFilesystemConfig,
)
from upathtools.configs.fsspec_fs_configs import (
    ArrowFilesystemConfig,
    DataFilesystemConfig,
    DaskWorkerFilesystemConfig,
    FTPFilesystemConfig,
    GitFilesystemConfig,
    GithubFilesystemConfig,
    HadoopFilesystemConfig,
    JupyterFilesystemConfig,
    LibArchiveFilesystemConfig,
    LocalFilesystemConfig,
    MemoryFilesystemConfig,
    SFTPFilesystemConfig,
    SMBFilesystemConfig,
    TarFilesystemConfig,
    WebHDFSFilesystemConfig,
    ZipFilesystemConfig,
)

FilesystemConfigType = Annotated[
    CliFilesystemConfig
    | DistributionFilesystemConfig
    | FlatUnionFilesystemConfig
    | GistFilesystemConfig
    | HttpFilesystemConfig
    | MarkdownFilesystemConfig
    | ModuleFilesystemConfig
    | PackageFilesystemConfig
    | PythonAstFilesystemConfig
    | UnionFilesystemConfig
    | WikiFilesystemConfig
    | ArrowFilesystemConfig
    | DataFilesystemConfig
    | DaskWorkerFilesystemConfig
    | FTPFilesystemConfig
    | GitFilesystemConfig
    | GithubFilesystemConfig
    | HadoopFilesystemConfig
    | JupyterFilesystemConfig
    | LibArchiveFilesystemConfig
    | LocalFilesystemConfig
    | MemoryFilesystemConfig
    | SFTPFilesystemConfig
    | SMBFilesystemConfig
    | TarFilesystemConfig
    | WebHDFSFilesystemConfig
    | ZipFilesystemConfig,
    Field(discriminator="fs_type"),
]

__all__ = [
    "ArrowFilesystemConfig",
    "CliFilesystemConfig",
    "DaskWorkerFilesystemConfig",
    "DataFilesystemConfig",
    "DistributionFilesystemConfig",
    "FTPFilesystemConfig",
    "FileSystemConfig",
    "FilesystemConfigType",
    "FlatUnionFilesystemConfig",
    "GistFilesystemConfig",
    "GitFilesystemConfig",
    "GithubFilesystemConfig",
    "HadoopFilesystemConfig",
    "HttpFilesystemConfig",
    "JupyterFilesystemConfig",
    "LibArchiveFilesystemConfig",
    "LocalFilesystemConfig",
    "MarkdownFilesystemConfig",
    "MemoryFilesystemConfig",
    "ModuleFilesystemConfig",
    "PackageFilesystemConfig",
    "PathConfig",
    "PythonAstFilesystemConfig",
    "SFTPFilesystemConfig",
    "SMBFilesystemConfig",
    "TarFilesystemConfig",
    "UnionFilesystemConfig",
    "WebHDFSFilesystemConfig",
    "WikiFilesystemConfig",
    "ZipFilesystemConfig",
]
