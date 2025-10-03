"""Configuration models for fsspec core filesystem implementations."""

from __future__ import annotations

from typing import Any, ClassVar, Literal

from pydantic import Field
from upath import UPath  # noqa: TC002

from upathtools.configs.base import (
    FilesystemCategoryType,  # noqa: TC001
    FileSystemConfig,
)


class ArrowFilesystemConfig(FileSystemConfig):
    """Configuration for Arrow filesystem wrapper."""

    fs_type: Literal["arrow"] = Field("arrow", init=False)
    """Arrow filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "wrapper"
    """Classification of the filesystem type"""


class DataFilesystemConfig(FileSystemConfig):
    """Configuration for Data URL filesystem."""

    fs_type: Literal["data"] = Field("data", init=False)
    """Data URL filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"
    """Classification of the filesystem type"""


class DaskWorkerFilesystemConfig(FileSystemConfig):
    """Configuration for Dask worker filesystem."""

    fs_type: Literal["dask"] = Field("dask", init=False)
    """Dask worker filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "wrapper"
    """Classification of the filesystem type"""

    target_protocol: str | None = None
    """Target protocol to use when running on workers"""

    target_options: dict[str, Any] | None = None
    """Options for target protocol"""

    client: Any | str | None = None
    """Dask client instance or connection string"""


class FTPFilesystemConfig(FileSystemConfig):
    """Configuration for FTP filesystem."""

    fs_type: Literal["ftp"] = Field("ftp", init=False)
    """FTP filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"
    """Classification of the filesystem type"""

    host: str
    """FTP server hostname or IP"""

    port: int = 21
    """FTP server port"""

    username: str | None = None
    """Username for authentication"""

    password: str | None = None
    """Password for authentication"""

    acct: str | None = None
    """Account string some servers need for auth"""

    block_size: int | None = None
    """Block size for file operations"""

    tempdir: str | None = None
    """Directory for temporary files during transactions"""

    timeout: int = 30
    """Connection timeout in seconds"""

    encoding: str = "utf-8"
    """Encoding for filenames and directories"""

    tls: bool = False
    """Whether to use FTP over TLS"""


class GitFilesystemConfig(FileSystemConfig):
    """Configuration for Git filesystem."""

    fs_type: Literal["git"] = Field("git", init=False)
    """Git filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"
    """Classification of the filesystem type"""

    path: str | None = None
    """Path to git repository"""

    fo: UPath | None = None
    """Alternative to path, passed as part of URL"""

    ref: str | None = None
    """Reference to work with (hash, branch, tag)"""


class GithubFilesystemConfig(FileSystemConfig):
    """Configuration for GitHub filesystem."""

    fs_type: Literal["github"] = Field("github", init=False)
    """GitHub filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"
    """Classification of the filesystem type"""

    org: str
    """GitHub organization or user name"""

    repo: str
    """Repository name"""

    sha: str | None = None
    """Commit hash, branch or tag name to use"""

    username: str | None = None
    """GitHub username for authentication"""

    token: str | None = None
    """GitHub token for authentication"""

    timeout: tuple[int, int] | int | None = None
    """Connection timeout in seconds (connect, read)"""


class HadoopFilesystemConfig(FileSystemConfig):
    """Configuration for Hadoop filesystem."""

    fs_type: Literal["hdfs"] = Field("hdfs", init=False)
    """Hadoop filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"
    """Classification of the filesystem type"""

    host: str = "default"
    """Hostname, IP or 'default' to use Hadoop config"""

    port: int = 0
    """Port number or 0 to use default from Hadoop config"""

    user: str | None = None
    """Username to connect as"""

    kerb_ticket: str | None = None
    """Kerberos ticket for authentication"""

    replication: int = 3
    """Replication factor for write operations"""

    extra_conf: dict[str, Any] | None = None
    """Additional configuration parameters"""


class JupyterFilesystemConfig(FileSystemConfig):
    """Configuration for Jupyter notebook/lab filesystem."""

    fs_type: Literal["jupyter"] = Field("jupyter", init=False)
    """Jupyter filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"
    """Classification of the filesystem type"""

    url: str
    """Base URL of the Jupyter server"""

    tok: str | None = None
    """Jupyter authentication token"""


class LibArchiveFilesystemConfig(FileSystemConfig):
    """Configuration for LibArchive filesystem."""

    fs_type: Literal["libarchive"] = Field("libarchive", init=False)
    """LibArchive filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "archive"
    """Classification of the filesystem type"""

    fo: UPath
    """Path to archive file"""

    target_protocol: str | None = None
    """Protocol for source file"""

    target_options: dict[str, Any] | None = None
    """Options for target protocol"""

    block_size: int | None = None
    """Block size for read operations"""


class LocalFilesystemConfig(FileSystemConfig):
    """Configuration for Local filesystem."""

    fs_type: Literal["file"] = Field("file", init=False)
    """Local filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"
    """Classification of the filesystem type"""

    auto_mkdir: bool = False
    """Whether to automatically make directories"""

    dir_policy: Literal["auto", "try_then_fail", "try_then_noop"] = "auto"
    """Policy for handling directories that may exist"""


class MemoryFilesystemConfig(FileSystemConfig):
    """Configuration for Memory filesystem."""

    fs_type: Literal["memory"] = Field("memory", init=False)
    """Memory filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"
    """Classification of the filesystem type"""


class SFTPFilesystemConfig(FileSystemConfig):
    """Configuration for SFTP filesystem."""

    fs_type: Literal["sftp"] = Field("sftp", init=False)
    """SFTP filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"
    """Classification of the filesystem type"""

    host: str
    """Hostname or IP to connect to"""

    port: int = 22
    """Port to connect to"""

    username: str | None = None
    """Username for authentication"""

    password: str | None = None
    """Password for authentication"""

    temppath: str = "/tmp"
    """Path for temporary files during transactions"""

    timeout: int = 30
    """Connection timeout in seconds"""


class SMBFilesystemConfig(FileSystemConfig):
    """Configuration for SMB/CIFS filesystem."""

    fs_type: Literal["smb"] = Field("smb", init=False)
    """SMB filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"
    """Classification of the filesystem type"""

    host: str
    """Hostname or IP of the SMB server"""

    port: int | None = None
    """Port to connect to"""

    username: str | None = None
    """Username for authentication"""

    password: str | None = None
    """Password for authentication"""

    auto_mkdir: bool = False
    """Whether to automatically make directories"""

    register_session_retries: int | None = None
    """Number of retries for session registration"""


class TarFilesystemConfig(FileSystemConfig):
    """Configuration for Tar archive filesystem."""

    fs_type: Literal["tar"] = Field("tar", init=False)
    """Tar filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "archive"
    """Classification of the filesystem type"""

    fo: UPath
    """Path to tar file"""

    index_store: Any | None = None
    """Where to store the index"""

    target_options: dict[str, Any] | None = None
    """Options for target protocol"""

    target_protocol: str | None = None
    """Protocol for source file"""

    compression: str | None = None
    """Compression type (None, 'gz', 'bz2', 'xz')"""


class WebHDFSFilesystemConfig(FileSystemConfig):
    """Configuration for WebHDFS filesystem."""

    fs_type: Literal["webhdfs"] = Field("webhdfs", init=False)
    """WebHDFS filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"
    """Classification of the filesystem type"""

    host: str
    """Hostname or IP of the HDFS namenode"""

    port: int = 50070
    """WebHDFS REST API port"""

    user: str | None = None
    """Username for authentication"""

    kerb: bool = False
    """Whether to use Kerberos authentication"""

    proxy_to: str | None = None
    """Host to proxy to (instead of real host)"""

    data_proxy: dict[str, str] | None = None
    """Map of data nodes to proxies"""

    ssl_verify: bool = True
    """Verify SSL certificates"""


class ZipFilesystemConfig(FileSystemConfig):
    """Configuration for Zip archive filesystem."""

    fs_type: Literal["zip"] = Field("zip", init=False)
    """Zip filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "archive"
    """Classification of the filesystem type"""

    fo: UPath
    """Path to zip file"""

    mode: str = "r"
    """Open mode ('r', 'w', 'a')"""

    target_protocol: str | None = None
    """Protocol for source file"""

    target_options: dict[str, Any] | None = None
    """Options for target protocol"""

    compression: int = 0  # ZipFile.ZIP_STORED
    """Compression method"""

    compresslevel: int | None = None
    """Compression level"""
