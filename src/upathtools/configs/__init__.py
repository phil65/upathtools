"""Configuration models for filesystem implementations and utilities."""

from __future__ import annotations

from typing import Annotated

from pydantic import Field

from upathtools.configs.base import FileSystemConfig, PathConfig
from upathtools.configs.custom_fs_configs import (
    AppwriteFilesystemConfig,
    BaseModelFilesystemConfig,
    BaseModelInstanceFilesystemConfig,
    BeamFilesystemConfig,
    CliFilesystemConfig,
    DaytonaFilesystemConfig,
    DistributionFilesystemConfig,
    E2BFilesystemConfig,
    FlatUnionFilesystemConfig,
    GistFilesystemConfig,
    HttpFilesystemConfig,
    HttpxFilesystemConfig,
    MarkdownFilesystemConfig,
    McpFilesystemConfig,
    MicrosandboxFilesystemConfig,
    ModalFilesystemConfig,
    ModuleFilesystemConfig,
    NotionFilesystemConfig,
    OpenApiFilesystemConfig,
    PackageFilesystemConfig,
    PythonAstFilesystemConfig,
    SkillsFilesystemConfig,
    TypeAdapterFilesystemConfig,
    UnionFilesystemConfig,
    VercelFilesystemConfig,
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
    AppwriteFilesystemConfig
    | BaseModelFilesystemConfig
    | BaseModelInstanceFilesystemConfig
    | BeamFilesystemConfig
    | CliFilesystemConfig
    | DaytonaFilesystemConfig
    | DistributionFilesystemConfig
    | E2BFilesystemConfig
    | FlatUnionFilesystemConfig
    | GistFilesystemConfig
    | HttpFilesystemConfig
    | HttpxFilesystemConfig
    | MarkdownFilesystemConfig
    | McpFilesystemConfig
    | MicrosandboxFilesystemConfig
    | ModalFilesystemConfig
    | ModuleFilesystemConfig
    | NotionFilesystemConfig
    | OpenApiFilesystemConfig
    | PackageFilesystemConfig
    | PythonAstFilesystemConfig
    | SkillsFilesystemConfig
    | TypeAdapterFilesystemConfig
    | UnionFilesystemConfig
    | VercelFilesystemConfig
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
    "AppwriteFilesystemConfig",
    "ArrowFilesystemConfig",
    "BaseModelFilesystemConfig",
    "BaseModelInstanceFilesystemConfig",
    "BeamFilesystemConfig",
    "CliFilesystemConfig",
    "DaskWorkerFilesystemConfig",
    "DataFilesystemConfig",
    "DaytonaFilesystemConfig",
    "DistributionFilesystemConfig",
    "E2BFilesystemConfig",
    "FTPFilesystemConfig",
    "FileSystemConfig",
    "FilesystemConfigType",
    "FlatUnionFilesystemConfig",
    "GistFilesystemConfig",
    "GitFilesystemConfig",
    "GithubFilesystemConfig",
    "HadoopFilesystemConfig",
    "HttpFilesystemConfig",
    "HttpxFilesystemConfig",
    "JupyterFilesystemConfig",
    "LibArchiveFilesystemConfig",
    "LocalFilesystemConfig",
    "MarkdownFilesystemConfig",
    "McpFilesystemConfig",
    "MemoryFilesystemConfig",
    "MicrosandboxFilesystemConfig",
    "ModalFilesystemConfig",
    "ModuleFilesystemConfig",
    "NotionFilesystemConfig",
    "OpenApiFilesystemConfig",
    "PackageFilesystemConfig",
    "PathConfig",
    "PythonAstFilesystemConfig",
    "SFTPFilesystemConfig",
    "SMBFilesystemConfig",
    "SkillsFilesystemConfig",
    "TarFilesystemConfig",
    "TypeAdapterFilesystemConfig",
    "UnionFilesystemConfig",
    "VercelFilesystemConfig",
    "WebHDFSFilesystemConfig",
    "WikiFilesystemConfig",
    "ZipFilesystemConfig",
]
