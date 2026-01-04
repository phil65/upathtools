"""Configuration models for fsspec core filesystem implementations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Literal

from pydantic import AnyUrl, ConfigDict, Field, SecretStr
from upath import UPath  # noqa: TC002

from upathtools_config.base import FilesystemCategoryType, FileSystemConfig  # noqa: TC001


if TYPE_CHECKING:
    from pydantic import AnyUrl, SecretStr


class ArrowFilesystemConfig(FileSystemConfig):
    """Configuration for Arrow filesystem wrapper."""

    model_config = ConfigDict(json_schema_extra={"title": "Arrow Configuration"})

    type: Literal["arrow"] = Field("arrow", init=False)
    """Arrow filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "wrapper"


class DataFilesystemConfig(FileSystemConfig):
    """Configuration for Data URL filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "Data URL Configuration"})

    type: Literal["data"] = Field("data", init=False)
    """Data URL filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"


class DaskWorkerFilesystemConfig(FileSystemConfig):
    """Configuration for Dask worker filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "Dask Worker Configuration"})

    type: Literal["dask"] = Field("dask", init=False)
    """Dask worker filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "wrapper"

    target_protocol: str | None = Field(
        default=None, title="Target Protocol", examples=["file", "s3", "gcs"]
    )
    """Target protocol to use when running on workers"""

    target_options: dict[str, Any] | None = Field(default=None, title="Target Protocol Options")
    """Options for target protocol"""

    client: Any | str | None = Field(
        default=None,
        title="Dask Client",
        examples=["localhost:8786", "tcp://scheduler:8786"],
    )
    """Dask client instance or connection string"""


class FTPFilesystemConfig(FileSystemConfig):
    """Configuration for FTP filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "FTP Configuration"})

    type: Literal["ftp"] = Field("ftp", init=False)
    """FTP filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    host: str = Field(title="FTP Host", examples=["ftp.example.com", "192.168.1.100"])
    """FTP server hostname or IP"""

    port: int = Field(default=21, ge=1, le=65535, title="FTP port", examples=[21, 1234])
    """FTP server port"""

    username: str | None = Field(
        default=None,
        title="Username",
        examples=["user123", "admin"],
        pattern=r"^[a-zA-Z0-9._-]+$",
        min_length=1,
        max_length=32,
    )
    """Username for authentication"""

    password: SecretStr | None = Field(default=None, title="Password")
    """Password for authentication"""

    acct: str | None = Field(default=None, title="Account String", examples=["account123"])
    """Account string some servers need for auth"""

    block_size: int | None = Field(default=None, gt=0, title="Block Size", examples=[8192, 65536])
    """Block size for file operations"""

    tempdir: str | None = Field(default=None, title="Temp Directory", examples=["/tmp", "/var/tmp"])
    """Directory for temporary files during transactions"""

    timeout: int = Field(default=30, ge=0, title="Timeout", examples=[30, 60, 120])
    """Connection timeout in seconds"""

    encoding: str = Field(
        default="utf-8",
        title="Encoding",
        examples=["utf-8", "latin-1"],
        pattern=r"^[a-zA-Z0-9]([a-zA-Z0-9\-_])*$",
    )
    """Encoding for filenames and directories"""

    tls: bool = Field(default=False, title="FTP over TLS")
    """Whether to use FTP over TLS"""


class GitFilesystemConfig(FileSystemConfig):
    """Configuration for Git filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "Git Configuration"})

    type: Literal["git"] = Field("git", init=False)
    """Git filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"

    path: str | None = Field(
        default=None,
        title="Repository Path",
        examples=["/path/to/repo", "~/projects/myrepo"],
    )
    """Path to git repository"""

    fo: UPath | None = Field(default=None, title="File Object Path")
    """Alternative to path, passed as part of URL"""

    ref: str | None = Field(
        default=None,
        title="Git Reference",
        examples=["main", "v1.0.0", "abc1234567890"],
        min_length=1,
        max_length=255,
    )
    """Reference to work with (hash, branch, tag)"""


class GithubFilesystemConfig(FileSystemConfig):
    """Configuration for GitHub filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "GitHub Configuration"})

    type: Literal["github"] = Field("github", init=False)
    """GitHub filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    org: str = Field(
        title="Organization",
        examples=["microsoft", "facebook", "phil65"],
        pattern=r"^[a-zA-Z0-9]([a-zA-Z0-9\-])*[a-zA-Z0-9]$|^[a-zA-Z0-9]$",
        min_length=1,
        max_length=39,
    )
    """GitHub organization or user name"""

    repo: str = Field(
        title="Repository",
        examples=["vscode", "react", "upathtools"],
        pattern=r"^[a-zA-Z0-9\._\-]+$",
        min_length=1,
        max_length=100,
    )
    """Repository name"""

    sha: str | None = Field(
        default=None,
        title="Commit/Branch/Tag",
        examples=["main", "v2.1.0", "abc1234567890"],
        min_length=1,
        max_length=255,
    )
    """Commit hash, branch or tag name to use"""

    username: str | None = Field(
        default=None,
        title="GitHub Username",
        examples=["octocat", "phil65"],
        pattern=r"^[a-zA-Z0-9]([a-zA-Z0-9\-])*[a-zA-Z0-9]$|^[a-zA-Z0-9]$",
        min_length=1,
        max_length=39,
    )
    """GitHub username for authentication"""

    token: SecretStr | None = Field(default=None, title="GitHub Token")
    """GitHub token for authentication"""

    timeout: tuple[int, int] | int | None = Field(
        default=None, title="Connection Timeout", examples=[30, 60, (10, 60)]
    )
    """Connection timeout in seconds (connect, read)"""


class HadoopFilesystemConfig(FileSystemConfig):
    """Configuration for Hadoop filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "Hadoop Configuration"})

    type: Literal["hdfs"] = Field("hdfs", init=False)
    """Hadoop filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    host: str = Field(
        default="default",
        title="HDFS Host",
        examples=["default", "namenode.example.com", "192.168.1.100"],
    )
    """Hostname, IP or 'default' to use Hadoop config"""

    port: int = Field(default=0, ge=0, le=65535, title="HDFS Port", examples=[0, 8020, 9000])
    """Port number or 0 to use default from Hadoop config"""

    user: str | None = Field(
        default=None,
        title="HDFS User",
        examples=["hdfs", "hadoop", "admin"],
        pattern=r"^[a-zA-Z0-9._-]+$",
        min_length=1,
        max_length=64,
    )
    """Username to connect as"""

    kerb_ticket: str | None = Field(
        default=None, title="Kerberos Ticket", examples=["/tmp/krb5cc_1000"]
    )
    """Kerberos ticket for authentication"""

    replication: int = Field(default=3, ge=1, title="Replication Factor", examples=[1, 3, 5])
    """Replication factor for write operations"""

    extra_conf: dict[str, Any] | None = Field(default=None, title="Extra Configuration")
    """Additional configuration parameters"""


class JupyterFilesystemConfig(FileSystemConfig):
    """Configuration for Jupyter notebook/lab filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "Jupyter Configuration"})

    type: Literal["jupyter"] = Field("jupyter", init=False)
    """Jupyter filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    url: AnyUrl = Field(
        title="Jupyter URL",
        examples=["http://localhost:8888", "https://jupyter.example.com"],
    )
    """Base URL of the Jupyter server"""

    tok: SecretStr | None = Field(default=None, title="Auth Token")
    """Jupyter authentication token"""


class LibArchiveFilesystemConfig(FileSystemConfig):
    """Configuration for LibArchive filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "LibArchive Configuration"})

    type: Literal["libarchive"] = Field("libarchive", init=False)
    """LibArchive filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "archive"

    fo: UPath = Field(title="Archive Path", examples=["/path/to/archive.tar.gz"])
    """Path to archive file"""

    target_protocol: str | None = Field(
        default=None, title="Target Protocol", examples=["file", "s3", "http"]
    )
    """Protocol for source file"""

    target_options: dict[str, Any] | None = Field(default=None, title="Target Protocol Options")
    """Options for target protocol"""

    block_size: int | None = Field(default=None, gt=0, title="Block Size", examples=[8192, 65536])
    """Block size for read operations"""


class LocalFilesystemConfig(FileSystemConfig):
    """Configuration for Local filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "Local Filesystem Configuration"})

    type: Literal["file"] = Field("file", init=False)
    """Local filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    auto_mkdir: bool = Field(default=False, title="Auto Make Directories")
    """Whether to automatically make directories"""

    dir_policy: Literal["auto", "try_then_fail", "try_then_noop"] = Field(
        default="auto", title="Directory Policy"
    )
    """Policy for handling directories that may exist"""


class MemoryFilesystemConfig(FileSystemConfig):
    """Configuration for Memory filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "Memory Filesystem Configuration"})

    type: Literal["memory"] = Field("memory", init=False)
    """Memory filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"


class SFTPFilesystemConfig(FileSystemConfig):
    """Configuration for SFTP filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "SFTP Configuration"})

    type: Literal["sftp"] = Field("sftp", init=False)
    """SFTP filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    host: str = Field(title="SFTP Host", examples=["sftp.example.com", "192.168.1.100"])
    """Hostname or IP to connect to"""

    port: int = Field(default=22, ge=1, le=65535, title="SFTP Port", examples=[22, 2222])
    """Port to connect to"""

    username: str | None = Field(
        default=None,
        title="Username",
        examples=["user", "admin"],
        pattern=r"^[a-zA-Z0-9._-]+$",
        min_length=1,
        max_length=32,
    )
    """Username for authentication"""

    password: SecretStr | None = Field(default=None, title="Password")
    """Password for authentication"""

    temppath: str = Field(default="/tmp", title="Temp Path", examples=["/tmp", "/var/tmp"])
    """Path for temporary files during transactions"""

    timeout: int = Field(default=30, ge=0, title="Timeout", examples=[30, 60, 120])
    """Connection timeout in seconds"""


class SMBFilesystemConfig(FileSystemConfig):
    """Configuration for SMB/CIFS filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "SMB Configuration"})

    type: Literal["smb"] = Field("smb", init=False)
    """SMB filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    host: str = Field(title="SMB Host", examples=["smb.example.com", "192.168.1.100"])
    """Hostname or IP of the SMB server"""

    port: int | None = Field(default=None, ge=1, le=65535, title="SMB Port", examples=[445, 139])
    """Port to connect to"""

    username: str | None = Field(
        default=None,
        title="Username",
        examples=["user", "admin"],
        pattern=r"^[a-zA-Z0-9._-]+$",
        min_length=1,
        max_length=32,
    )
    """Username for authentication"""

    password: SecretStr | None = Field(default=None, title="Password")
    """Password for authentication"""

    auto_mkdir: bool = Field(default=False, title="Auto Make Directories")
    """Whether to automatically make directories"""

    register_session_retries: int | None = Field(
        default=None, ge=0, title="Session Retries", examples=[3, 5, 10]
    )
    """Number of retries for session registration"""


class TarFilesystemConfig(FileSystemConfig):
    """Configuration for Tar archive filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "TAR Archive Configuration"})

    type: Literal["tar"] = Field("tar", init=False)
    """Tar filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "archive"

    fo: UPath = Field(title="Tar File Path", examples=["/path/to/archive.tar.gz"])
    """Path to tar file"""

    index_store: Any | None = Field(default=None, title="Index Store")
    """Where to store the index"""

    target_options: dict[str, Any] | None = Field(default=None, title="Target Protocol Options")
    """Options for target protocol"""

    target_protocol: str | None = Field(
        default=None, title="Target Protocol", examples=["file", "s3", "http"]
    )
    """Protocol for source file"""

    compression: str | None = Field(
        default=None,
        title="Compression Type",
        examples=["gz", "bz2", "xz"],
        pattern=r"^(gz|bz2|xz|lzma)$",
    )
    """Compression type (None, 'gz', 'bz2', 'xz')"""


class WebHDFSFilesystemConfig(FileSystemConfig):
    """Configuration for WebHDFS filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "WebHDFS Configuration"})

    type: Literal["webhdfs"] = Field("webhdfs", init=False)
    """WebHDFS filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    host: str = Field(title="WebHDFS Host", examples=["namenode.example.com", "192.168.1.100"])
    """Hostname or IP of the HDFS namenode"""

    port: int = Field(
        default=50070, ge=1, le=65535, title="WebHDFS Port", examples=[50070, 9870, 14000]
    )
    """WebHDFS REST API port"""

    user: str | None = Field(
        default=None,
        title="HDFS User",
        examples=["hdfs", "hadoop", "admin"],
        pattern=r"^[a-zA-Z0-9._-]+$",
        min_length=1,
        max_length=64,
    )
    """Username for authentication"""

    kerb: bool = Field(default=False, title="Kerberos Authentication")
    """Whether to use Kerberos authentication"""

    proxy_to: str | None = Field(
        default=None, title="Proxy Host", examples=["proxy.example.com", "192.168.1.200"]
    )
    """Host to proxy to (instead of real host)"""

    data_proxy: dict[str, str] | None = Field(default=None, title="Data Proxy Map")
    """Map of data nodes to proxies"""

    ssl_verify: bool = Field(default=False, title="Verify SSL Certificates")
    """Verify SSL certificates"""


class ZipFilesystemConfig(FileSystemConfig):
    """Configuration for Zip archive filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "ZIP Archive Configuration"})

    type: Literal["zip"] = Field("zip", init=False)
    """Zip filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "archive"

    fo: UPath = Field(title="Zip File Path", examples=["/path/to/archive.zip"])
    """Path to zip file"""

    mode: str = Field(default="r", title="Open Mode", examples=["r", "w", "a"], pattern=r"^[rwa]$")
    """Open mode ('r', 'w', 'a')"""

    target_protocol: str | None = Field(
        default=None, title="Target Protocol", examples=["file", "s3", "http"]
    )
    """Protocol for source file"""

    target_options: dict[str, Any] | None = Field(default=None, title="Target Protocol Options")
    """Options for target protocol"""

    compression: int = Field(
        default=0, ge=0, le=99, title="Compression Method", examples=[0, 8]
    )  # ZipFile.ZIP_STORED
    """Compression method"""

    compresslevel: int | None = Field(
        default=None, ge=1, le=9, title="Compression Level", examples=[1, 6, 9]
    )
    """Compression level"""


class AzureBlobFilesystemConfig(FileSystemConfig):
    """Configuration for Azure Blob Storage filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "Azure Blob Storage Configuration"})

    type: Literal["abfs"] = Field("abfs", init=False)
    """Azure Blob filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    account_name: str = Field(
        title="Account Name",
        examples=["mystorageaccount"],
        pattern=r"^[a-z0-9]{3,24}$",
    )
    """Azure storage account name"""

    account_key: SecretStr | None = Field(default=None, title="Account Key")
    """Azure storage account key"""

    connection_string: SecretStr | None = Field(default=None, title="Connection String")
    """Azure storage connection string"""

    sas_token: SecretStr | None = Field(default=None, title="SAS Token")
    """Shared Access Signature token"""

    container_name: str | None = Field(
        default=None,
        title="Container Name",
        examples=["mycontainer"],
    )
    """Default container name"""

    credential: Any | None = Field(default=None, title="Azure Credential")
    """Azure credential object (DefaultAzureCredential, etc.)"""


class GCSFilesystemConfig(FileSystemConfig):
    """Configuration for Google Cloud Storage filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "Google Cloud Storage Configuration"})

    type: Literal["gcs"] = Field("gcs", init=False)
    """GCS filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    project: str | None = Field(
        default=None,
        title="GCP Project",
        examples=["my-gcp-project"],
    )
    """Google Cloud project ID"""

    token: str | dict[str, Any] | None = Field(
        default=None,
        title="Authentication Token",
        examples=["anon", "browser", "cloud", "cache"],
    )
    """Authentication method or token dict"""

    access: Literal["read_only", "read_write", "full_control"] | None = Field(
        default=None,
        title="Access Level",
    )
    """Access level for the filesystem"""

    consistency: Literal["none", "size", "md5"] | None = Field(
        default=None,
        title="Consistency Check",
    )
    """Consistency check method after writes"""

    requester_pays: bool = Field(default=False, title="Requester Pays")
    """Whether to use requester-pays buckets"""


class HuggingFaceFilesystemConfig(FileSystemConfig):
    """Configuration for HuggingFace Hub filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "HuggingFace Hub Configuration"})

    type: Literal["hf"] = Field("hf", init=False)
    """HuggingFace filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    token: SecretStr | None = Field(default=None, title="HF Token")
    """HuggingFace API token (or set HF_TOKEN env var)"""

    repo_type: Literal["model", "dataset", "space"] | None = Field(
        default=None,
        title="Repository Type",
    )
    """Type of HuggingFace repository"""

    revision: str | None = Field(
        default=None,
        title="Revision",
        examples=["main", "v1.0.0"],
    )
    """Git revision (branch, tag, or commit)"""


class S3FilesystemConfig(FileSystemConfig):
    """Configuration for AWS S3 filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "Amazon S3 Configuration"})

    type: Literal["s3"] = Field("s3", init=False)
    """S3 filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    key: SecretStr | None = Field(default=None, title="AWS Access Key ID")
    """AWS access key ID"""

    secret: SecretStr | None = Field(default=None, title="AWS Secret Access Key")
    """AWS secret access key"""

    token: SecretStr | None = Field(default=None, title="AWS Session Token")
    """AWS session token for temporary credentials"""

    endpoint_url: str | None = Field(
        default=None,
        title="Endpoint URL",
        examples=["https://s3.amazonaws.com", "http://localhost:9000"],
    )
    """Custom S3 endpoint URL (for MinIO, LocalStack, etc.)"""

    region_name: str | None = Field(
        default=None,
        title="AWS Region",
        examples=["us-east-1", "eu-west-1"],
    )
    """AWS region name"""

    anon: bool = Field(default=False, title="Anonymous Access")
    """Use anonymous access (no credentials)"""

    use_ssl: bool = Field(default=True, title="Use SSL")
    """Whether to use SSL for connections"""

    requester_pays: bool = Field(default=False, title="Requester Pays")
    """Access requester-pays buckets"""

    default_block_size: int | None = Field(
        default=None, gt=0, title="Block Size", examples=[5242880]
    )
    """Default block size for multipart uploads"""


class WebdavFilesystemConfig(FileSystemConfig):
    """Configuration for WebDAV filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "WebDAV Configuration"})

    type: Literal["webdav"] = Field("webdav", init=False)
    """WebDAV filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    base_url: str = Field(
        title="WebDAV URL",
        examples=["https://webdav.example.com/files", "http://localhost:8080/dav"],
    )
    """Base URL of the WebDAV server"""

    auth: tuple[str, str] | None = Field(default=None, title="Basic Auth")
    """Basic authentication (username, password) tuple"""

    token: SecretStr | None = Field(default=None, title="Bearer Token")
    """Bearer token for authentication"""

    cert: str | tuple[str, str] | None = Field(default=None, title="Client Certificate")
    """Client certificate path or (cert, key) tuple"""

    verify: bool | str = Field(default=True, title="Verify SSL")
    """Verify SSL certificates (True, False, or CA bundle path)"""

    timeout: int | None = Field(default=None, ge=0, title="Timeout", examples=[30, 60])
    """Connection timeout in seconds"""


FsspecFilesystemConfig = (
    ArrowFilesystemConfig
    | AzureBlobFilesystemConfig
    | DataFilesystemConfig
    | DaskWorkerFilesystemConfig
    | FTPFilesystemConfig
    | GCSFilesystemConfig
    | GitFilesystemConfig
    | GithubFilesystemConfig
    | HadoopFilesystemConfig
    | HuggingFaceFilesystemConfig
    | JupyterFilesystemConfig
    | LibArchiveFilesystemConfig
    | LocalFilesystemConfig
    | MemoryFilesystemConfig
    | S3FilesystemConfig
    | SFTPFilesystemConfig
    | SMBFilesystemConfig
    | TarFilesystemConfig
    | WebdavFilesystemConfig
    | WebHDFSFilesystemConfig
    | ZipFilesystemConfig
)
"""Union of all fsspec-based filesystem configurations."""
