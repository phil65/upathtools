"""Configuration models for filesystem implementations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Literal

from pydantic import Field, SecretStr
from upath import UPath  # noqa: TC002

from upathtools.configs.base import (
    FilesystemCategoryType,  # noqa: TC001
    FileSystemConfig,
)


if TYPE_CHECKING:
    from pydantic import SecretStr


class CliFilesystemConfig(FileSystemConfig):
    """Configuration for CLI filesystem."""

    fs_type: Literal["cli"] = Field("cli", init=False)
    """CLI filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    shell: bool = Field(default=False, title="Shell mode")
    """Whether to use shell mode for command execution"""

    encoding: str = Field(
        default="utf-8",
        title="Output encoding",
        examples=["utf-8"],
        pattern=r"^[a-zA-Z0-9]([a-zA-Z0-9\-_])*$",
    )
    """Output encoding for command results"""


class DistributionFilesystemConfig(FileSystemConfig):
    """Configuration for Distribution filesystem."""

    fs_type: Literal["distribution"] = Field("distribution", init=False)
    """Distribution filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"


class FlatUnionFilesystemConfig(FileSystemConfig):
    """Configuration for FlatUnion filesystem."""

    fs_type: Literal["flatunion"] = Field("flatunion", init=False)
    """FlatUnion filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "aggregation"

    filesystems: list[str] = Field(
        title="Filesystem Identifiers", examples=[["fs1", "fs2", "fs3"]], min_length=1
    )
    """List of filesystem identifiers to include in the union"""


class GistFilesystemConfig(FileSystemConfig):
    """Configuration for GitHub Gist filesystem."""

    fs_type: Literal["gist"] = Field("gist", init=False)
    """Gist filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    gist_id: str | None = Field(
        default=None,
        title="Gist ID",
        examples=["abc123"],
        pattern=r"^[a-f0-9]+$",
        min_length=1,
    )
    """Specific gist ID to access"""

    username: str | None = Field(
        default=None,
        title="GitHub Username",
        examples=["phil65"],
        pattern=r"^[a-zA-Z0-9]([a-zA-Z0-9\-])*[a-zA-Z0-9]$|^[a-zA-Z0-9]$",
        min_length=1,
        max_length=39,
    )
    """GitHub username for listing all gists"""

    token: SecretStr | None = Field(default=None, title="GitHub Token", examples=["abc123"])
    """GitHub personal access token for authentication"""

    sha: str | None = Field(
        default=None,
        title="Gist Revision",
        examples=["abc123"],
        pattern=r"^[a-f0-9]+$",
        min_length=1,
    )
    """Specific revision of a gist"""

    timeout: int | None = Field(default=None, ge=0, title="Connection Timeout")
    """Connection timeout in seconds"""


class HttpFilesystemConfig(FileSystemConfig):
    """Configuration for HTTP filesystem."""

    fs_type: Literal["http"] = Field("http", init=False)
    """HTTP filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    simple_links: bool = Field(default=True, title="Simple Links")
    """Whether to extract links using simpler regex patterns"""

    block_size: int | None = Field(default=None, gt=0, title="Block Size", examples=[8192, 65536])
    """Block size for reading files in chunks"""

    same_scheme: bool = Field(default=True, title="Same Scheme")
    """Whether to keep the same scheme (http/https) when following links"""

    size_policy: str | None = Field(default=None, title="Size Policy", examples=["head", "get"])
    """Policy for determining file size ('head' or 'get')"""

    cache_type: str = Field(
        default="bytes", title="Cache Type", examples=["bytes", "readahead", "blockcache"]
    )
    """Type of cache to use for file contents"""

    encoded: bool = Field(default=False, title="Encoded URLs")
    """Whether URLs are already encoded"""


class MarkdownFilesystemConfig(FileSystemConfig):
    """Configuration for Markdown filesystem."""

    fo: UPath = Field(title="Markdown File Path", examples=["/path/to/file.md"])
    """Path to markdown file"""

    fs_type: Literal["md"] = Field("md", init=False)
    """Markdown filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"

    target_protocol: str | None = Field(
        default=None, title="Target Protocol", examples=["file", "s3", "http"]
    )
    """Protocol for source file"""

    target_options: dict[str, Any] | None = Field(default=None, title="Target Protocol Options")
    """Options for target protocol"""


class ModuleFilesystemConfig(FileSystemConfig):
    """Configuration for Module filesystem."""

    fs_type: Literal["mod"] = Field("mod", init=False)
    """Module filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"

    fo: UPath = Field(title="Python File Path", examples=["/path/to/module.py"])
    """Path to Python file"""

    target_protocol: str | None = Field(
        default=None, title="Target Protocol", examples=["file", "s3", "http"]
    )
    """Protocol for source file"""

    target_options: dict[str, Any] | None = Field(default=None, title="Target Protocol Options")
    """Options for target protocol"""


class PackageFilesystemConfig(FileSystemConfig):
    """Configuration for Package filesystem."""

    fs_type: Literal["pkg"] = Field("pkg", init=False)
    """Package filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"

    package: str = Field(
        title="Package Name",
        examples=["upathtools"],
        pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$",
        min_length=1,
    )
    """Name of the package to browse"""


class PythonAstFilesystemConfig(FileSystemConfig):
    """Configuration for Python AST filesystem."""

    fs_type: Literal["ast"] = Field("ast", init=False)
    """Python AST filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"

    fo: UPath = Field(title="Python File Path", examples=["/path/to/script.py"])
    """Path to Python file"""

    target_protocol: str | None = Field(
        default=None, title="Target Protocol", examples=["file", "s3", "http"]
    )
    """Protocol for source file"""

    target_options: dict[str, Any] | None = Field(default=None, title="Target Protocol Options")
    """Options for target protocol"""


class UnionFilesystemConfig(FileSystemConfig):
    """Configuration for Union filesystem."""

    fs_type: Literal["union"] = Field("union", init=False)
    """Union filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "aggregation"

    filesystems: dict[str, Any] = Field(title="Filesystem Configurations")
    """Dictionary mapping protocol names to filesystem configurations"""


class WikiFilesystemConfig(FileSystemConfig):
    """Configuration for GitHub Wiki filesystem."""

    fs_type: Literal["wiki"] = Field("wiki", init=False)
    """Wiki filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    owner: str = Field(
        title="Repository Owner",
        examples=["microsoft", "facebook", "phil65"],
        pattern=r"^[a-zA-Z0-9]([a-zA-Z0-9\-])*[a-zA-Z0-9]$|^[a-zA-Z0-9]$",
        min_length=1,
        max_length=39,
    )
    """GitHub repository owner/organization"""

    repo: str = Field(
        title="Repository Name",
        examples=["vscode", "react", "upathtools"],
        pattern=r"^[a-zA-Z0-9\._\-]+$",
        min_length=1,
        max_length=100,
    )
    """GitHub repository name"""

    token: SecretStr | None = Field(default=None, title="GitHub Token")
    """GitHub personal access token for authentication"""

    timeout: int | None = Field(
        default=None, ge=0, title="Connection Timeout", examples=[30, 60, 120]
    )
    """Connection timeout in seconds"""


class AppwriteFilesystemConfig(FileSystemConfig):
    """Configuration for Appwrite storage filesystem."""

    fs_type: Literal["appwrite"] = Field("appwrite", init=False)
    """Appwrite filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    endpoint: str | None = Field(
        default=None,
        title="Appwrite Endpoint",
        examples=["https://cloud.appwrite.io/v1"],
    )
    """Appwrite API endpoint"""

    project: str | None = Field(
        default=None,
        title="Project ID",
        examples=["64b1f2c8e8c9a"],
        min_length=1,
    )
    """Appwrite project ID"""

    key: SecretStr | None = Field(default=None, title="API Key")
    """Appwrite API key"""

    bucket_id: str | None = Field(
        default=None,
        title="Bucket ID",
        examples=["default", "images", "documents"],
        min_length=1,
    )
    """Default bucket ID for operations"""

    self_signed: bool = Field(default=False, title="Allow Self-Signed Certificates")
    """Whether to allow self-signed certificates"""


class BaseModelFilesystemConfig(FileSystemConfig):
    """Configuration for Pydantic BaseModel schema filesystem."""

    fs_type: Literal["basemodel"] = Field("basemodel", init=False)
    """BaseModel filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"

    model: str = Field(
        title="Model Import Path",
        examples=["mypackage.MyModel", "pydantic.BaseModel"],
        pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$",
        min_length=1,
    )
    """BaseModel class import path"""


class HttpxFilesystemConfig(FileSystemConfig):
    """Configuration for HTTPX-based HTTP filesystem."""

    fs_type: Literal["httpx"] = Field("httpx", init=False)
    """HTTPX filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    simple_links: bool = Field(default=True, title="Simple Links")
    """Whether to extract links using simpler regex patterns"""

    block_size: int | None = Field(default=None, gt=0, title="Block Size", examples=[8192, 65536])
    """Block size for reading files in chunks"""

    same_scheme: bool = Field(default=True, title="Same Scheme")
    """Whether to keep the same scheme (http/https) when following links"""

    size_policy: str | None = Field(default=None, title="Size Policy", examples=["head", "get"])
    """Policy for determining file size ('head' or 'get')"""

    cache_type: str = Field(
        default="bytes", title="Cache Type", examples=["bytes", "readahead", "blockcache"]
    )
    """Type of cache to use for file contents"""

    encoded: bool = Field(default=False, title="Encoded URLs")
    """Whether URLs are already encoded"""

    timeout: int | None = Field(default=None, ge=0, title="Request Timeout", examples=[30, 60, 120])
    """HTTP request timeout in seconds"""


class McpFilesystemConfig(FileSystemConfig):
    """Configuration for MCP (Model Context Protocol) filesystem."""

    fs_type: Literal["mcp"] = Field("mcp", init=False)
    """MCP filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    url: str | None = Field(
        default=None,
        title="MCP Server URL",
        examples=["ws://localhost:8000", "wss://mcp.example.com"],
    )
    """MCP server URL"""

    server_cmd: list[str] | None = Field(
        default=None,
        title="Server Command",
        examples=[["python", "-m", "my_mcp_server"]],
        min_length=1,
    )
    """Command to start MCP server"""


class NotionFilesystemConfig(FileSystemConfig):
    """Configuration for Notion filesystem."""

    fs_type: Literal["notion"] = Field("notion", init=False)
    """Notion filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    token: SecretStr = Field(title="Integration Token")
    """Notion integration token"""

    parent_page_id: str = Field(
        title="Parent Page ID",
        examples=["64b1f2c8e8c9a1234567890"],
        pattern=r"^[a-f0-9\-]+$",
        min_length=32,
        max_length=36,
    )
    """ID of the parent page where new pages will be created"""


class OpenApiFilesystemConfig(FileSystemConfig):
    """Configuration for OpenAPI schema filesystem."""

    fs_type: Literal["openapi"] = Field("openapi", init=False)
    """OpenAPI filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"

    fo: UPath = Field(
        title="OpenAPI Spec Path",
        examples=["/path/to/openapi.yaml", "/path/to/spec.json"],
    )
    """Path to OpenAPI specification file"""

    target_protocol: str | None = Field(
        default=None, title="Target Protocol", examples=["file", "http", "s3"]
    )
    """Protocol for source file"""

    target_options: dict[str, Any] | None = Field(default=None, title="Target Protocol Options")
    """Options for target protocol"""


class SkillsFilesystemConfig(FileSystemConfig):
    """Configuration for Skills filesystem."""

    fs_type: Literal["skills"] = Field("skills", init=False)
    """Skills filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"

    skills_dir: UPath | None = Field(
        default=None,
        title="Skills Directory",
        examples=["/path/to/skills", "~/my-skills"],
    )
    """Directory containing skill definitions"""


class TypeAdapterFilesystemConfig(FileSystemConfig):
    """Configuration for TypeAdapter filesystem."""

    fs_type: Literal["typeadapter"] = Field("typeadapter", init=False)
    """TypeAdapter filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"

    type_adapter: str = Field(
        title="TypeAdapter Import Path",
        examples=["mypackage.MyTypeAdapter", "pydantic.TypeAdapter"],
        pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$",
        min_length=1,
    )
    """TypeAdapter class import path"""


# Sandbox Filesystem Configurations


class BeamFilesystemConfig(FileSystemConfig):
    """Configuration for Beam sandbox filesystem."""

    fs_type: Literal["beam"] = Field("beam", init=False)
    """Beam filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "sandbox"

    sandbox_id: str | None = Field(
        default=None,
        title="Sandbox ID",
        examples=["sb-abc123456789"],
        min_length=1,
    )
    """Existing sandbox ID to connect to"""

    cpu: float = Field(default=1.0, gt=0, title="CPU Allocation", examples=[0.5, 1.0, 2.0])
    """CPU allocation for new sandboxes"""

    memory: int = Field(default=128, gt=0, title="Memory (MB)", examples=[128, 512, 1024])
    """Memory allocation for new sandboxes in MB"""

    gpu_count: int = Field(default=0, ge=0, title="GPU Count", examples=[0, 1, 2])
    """Number of GPUs for new sandboxes"""

    keep_warm_seconds: int = Field(
        default=600, ge=0, title="Keep Warm Duration", examples=[300, 600, 1800]
    )
    """How long to keep sandbox alive in seconds"""

    timeout: float = Field(default=300, gt=0, title="Timeout", examples=[60, 300, 600])
    """Default timeout for operations in seconds"""

    env_variables: dict[str, str] | None = Field(default=None, title="Environment Variables")
    """Environment variables for new sandboxes"""


class DaytonaFilesystemConfig(FileSystemConfig):
    """Configuration for Daytona sandbox filesystem."""

    fs_type: Literal["daytona"] = Field("daytona", init=False)
    """Daytona filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "sandbox"

    sandbox_id: str | None = Field(
        default=None,
        title="Sandbox ID",
        examples=["daytona-workspace-123"],
        min_length=1,
    )
    """Existing sandbox ID to connect to"""

    timeout: float = Field(default=600, gt=0, title="Timeout", examples=[300, 600, 1200])
    """Default timeout for operations in seconds"""


class E2BFilesystemConfig(FileSystemConfig):
    """Configuration for E2B sandbox filesystem."""

    fs_type: Literal["e2b"] = Field("e2b", init=False)
    """E2B filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "sandbox"

    api_key: SecretStr = Field(title="E2B API Key")
    """E2B API key for authentication"""

    template: str = Field(
        default="code-interpreter-v1",
        title="Template",
        examples=["code-interpreter-v1", "base", "python"],
        min_length=1,
    )
    """E2B template to use for sandboxes"""

    sandbox_id: str | None = Field(
        default=None,
        title="Sandbox ID",
        examples=["e2b-sb-abc123456789"],
        min_length=1,
    )
    """Existing sandbox ID to connect to"""

    timeout: float = Field(default=60, gt=0, title="Timeout", examples=[30, 60, 120])
    """Default timeout for operations in seconds"""


class MicrosandboxFilesystemConfig(FileSystemConfig):
    """Configuration for Microsandbox filesystem."""

    fs_type: Literal["microsandbox"] = Field("microsandbox", init=False)
    """Microsandbox filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "sandbox"

    server_url: str | None = Field(
        default=None,
        title="Server URL",
        examples=["http://localhost:8080", "https://microsandbox.example.com"],
    )
    """Microsandbox server URL"""

    namespace: str = Field(
        default="default",
        title="Namespace",
        examples=["default", "production", "staging"],
        min_length=1,
    )
    """Sandbox namespace"""

    name: str | None = Field(
        default=None,
        title="Sandbox Name",
        examples=["my-sandbox", "data-processor"],
        min_length=1,
    )
    """Sandbox name"""

    api_key: SecretStr | None = Field(default=None, title="API Key")
    """API key for authentication"""

    image: str | None = Field(
        default=None,
        title="Docker Image",
        examples=["python:3.11", "ubuntu:22.04", "node:18"],
        min_length=1,
    )
    """Docker image to use"""

    memory: int = Field(default=512, gt=0, title="Memory Limit (MB)", examples=[256, 512, 1024])
    """Memory limit in MB"""

    cpus: float = Field(default=1.0, gt=0, title="CPU Limit", examples=[0.5, 1.0, 2.0])
    """CPU limit"""


class ModalFilesystemConfig(FileSystemConfig):
    """Configuration for Modal sandbox filesystem."""

    fs_type: Literal["modal"] = Field("modal", init=False)
    """Modal filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "sandbox"

    app_name: str = Field(
        title="Modal App Name",
        examples=["my-app", "data-processing", "ml-pipeline"],
        pattern=r"^[a-zA-Z0-9\-_]+$",
        min_length=1,
        max_length=64,
    )
    """Modal application name"""

    sandbox_id: str | None = Field(
        default=None,
        title="Sandbox ID",
        examples=["sb-abc123456789"],
        min_length=1,
    )
    """Existing sandbox ID to connect to"""

    timeout: float = Field(default=600, gt=0, title="Timeout", examples=[300, 600, 1200])
    """Default timeout for operations in seconds"""

    idle_timeout: float = Field(default=300, gt=0, title="Idle Timeout", examples=[60, 300, 600])
    """Sandbox idle timeout in seconds"""


class VercelFilesystemConfig(FileSystemConfig):
    """Configuration for Vercel sandbox filesystem."""

    fs_type: Literal["vercel"] = Field("vercel", init=False)
    """Vercel filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "sandbox"

    template: str = Field(
        default="code-interpreter-v1",
        title="Template",
        examples=["code-interpreter-v1", "node", "python"],
        min_length=1,
    )
    """Vercel template to use for sandboxes"""

    sandbox_id: str | None = Field(
        default=None,
        title="Sandbox ID",
        examples=["vercel-sb-123"],
        min_length=1,
    )
    """Existing sandbox ID to connect to"""

    api_key: SecretStr | None = Field(default=None, title="API Key")
    """Vercel API key for authentication"""

    timeout: float = Field(default=60, gt=0, title="Timeout", examples=[30, 60, 120])
    """Default timeout for operations in seconds"""


class SRTFilesystemConfig(FileSystemConfig):
    """Configuration for SRT (Sandbox Runtime) filesystem.

    Uses Anthropic's sandbox-runtime for sandboxed local filesystem access
    with configurable network and filesystem restrictions.
    """

    fs_type: Literal["srt"] = Field("srt", init=False)
    """SRT filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "sandbox"

    allowed_domains: list[str] = Field(
        default_factory=list,
        title="Allowed Domains",
        examples=[["github.com", "*.github.com", "pypi.org"]],
    )
    """Domains that can be accessed. Empty = no network access."""

    denied_domains: list[str] = Field(
        default_factory=list,
        title="Denied Domains",
        examples=[["malicious.com"]],
    )
    """Domains explicitly blocked."""

    allow_unix_sockets: list[str] = Field(
        default_factory=list,
        title="Allowed Unix Sockets",
        examples=[["/var/run/docker.sock"]],
    )
    """Unix socket paths to allow."""

    allow_all_unix_sockets: bool = Field(default=False, title="Allow All Unix Sockets")
    """Allow all Unix sockets (less secure)."""

    allow_local_binding: bool = Field(default=False, title="Allow Local Binding")
    """Allow binding to localhost ports."""

    deny_read: list[str] = Field(
        default_factory=lambda: ["~/.ssh", "~/.aws", "~/.gnupg"],
        title="Deny Read Paths",
        examples=[["~/.ssh", "~/.aws"]],
    )
    """Paths blocked from reading."""

    allow_write: list[str] = Field(
        default_factory=lambda: ["."],
        title="Allow Write Paths",
        examples=[["."], [".", "/tmp"]],
    )
    """Paths where writes are permitted."""

    deny_write: list[str] = Field(
        default_factory=list,
        title="Deny Write Paths",
        examples=[[".env", "secrets/"]],
    )
    """Paths denied within allowed write paths."""

    timeout: float = Field(default=30, gt=0, title="Timeout", examples=[30, 60, 120])
    """Default timeout for operations in seconds."""


class BaseModelInstanceFilesystemConfig(FileSystemConfig):
    """Configuration for Pydantic BaseModel instance filesystem."""

    fs_type: Literal["basemodel_instance"] = Field("basemodel_instance", init=False)
    """BaseModel instance filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"

    instance: str = Field(
        title="Model Instance Path",
        examples=["mypackage.model_instance", "app.config.settings"],
        pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$",
        min_length=1,
    )
    """BaseModel instance import path"""


class AsyncLocalFilesystemConfig(FileSystemConfig):
    """Configuration for async local filesystem."""

    fs_type: Literal["asynclocal"] = Field("asynclocal", init=False)
    """Async local filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    auto_mkdir: bool = Field(default=False, title="Auto Create Directories")
    """Automatically create parent directories on write"""


class OverlayFilesystemConfig(FileSystemConfig):
    """Configuration for overlay filesystem with copy-on-write semantics."""

    fs_type: Literal["overlay"] = Field("overlay", init=False)
    """Overlay filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "aggregation"

    filesystems: list[str] = Field(
        title="Filesystem Identifiers",
        examples=[["writable_fs", "readonly_fs"]],
        min_length=1,
    )
    """List of filesystem identifiers, first is writable upper layer"""


class SqliteFilesystemConfig(FileSystemConfig):
    """Configuration for SQLite database filesystem."""

    fs_type: Literal["sqlite"] = Field("sqlite", init=False)
    """SQLite filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"

    db_path: str = Field(title="Database Path", examples=["/path/to/database.db"])
    """Path to SQLite database file"""

    target_protocol: str | None = Field(
        default=None, title="Target Protocol", examples=["file", "s3", "http"]
    )
    """Protocol for source database file"""

    target_options: dict[str, Any] | None = Field(default=None, title="Target Protocol Options")
    """Options for target protocol"""


class TreeSitterFilesystemConfig(FileSystemConfig):
    """Configuration for tree-sitter code structure filesystem."""

    fs_type: Literal["treesitter"] = Field("treesitter", init=False)
    """Tree-sitter filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"

    source_file: str = Field(title="Source File Path", examples=["/path/to/code.py"])
    """Path to source code file"""

    language: str | None = Field(
        default=None,
        title="Language",
        examples=["python", "javascript", "rust"],
    )
    """Programming language (auto-detected from extension if not specified)"""

    target_protocol: str | None = Field(
        default=None, title="Target Protocol", examples=["file", "s3", "http"]
    )
    """Protocol for source file"""

    target_options: dict[str, Any] | None = Field(default=None, title="Target Protocol Options")
    """Options for target protocol"""


class GitLabFilesystemConfig(FileSystemConfig):
    """Configuration for GitLab repository filesystem."""

    fs_type: Literal["gitlab"] = Field("gitlab", init=False)
    """GitLab filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    project_id: str | int = Field(
        title="Project ID",
        examples=["mygroup/myproject", 12345],
    )
    """GitLab project ID or path (e.g., 'namespace/project')"""

    ref: str | None = Field(
        default=None,
        title="Git Reference",
        examples=["main", "v1.0.0", "abc123"],
    )
    """Git ref (branch, tag, commit SHA). Uses default branch if not specified"""

    url: str = Field(
        default="https://gitlab.com",
        title="GitLab URL",
        examples=["https://gitlab.com", "https://gitlab.example.com"],
    )
    """GitLab instance URL"""

    private_token: SecretStr | None = Field(
        default=None,
        title="Private Token",
        description="GitLab private/personal access token (or set GITLAB_TOKEN env var)",
    )
    """GitLab private access token"""


class McpToolsFilesystemConfig(FileSystemConfig):
    """Configuration for MCP tools filesystem."""

    fs_type: Literal["mcptools"] = Field("mcptools", init=False)
    """MCP tools filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    url: str | None = Field(
        default=None,
        title="MCP Server URL",
        examples=["http://localhost:8000/mcp"],
    )
    """URL of MCP server"""

    server_cmd: list[str] | None = Field(
        default=None,
        title="Server Command",
        examples=[["uvx", "mcp-server-fetch"]],
    )
    """Command to start MCP server"""

    stubs_only: bool = Field(
        default=False,
        title="Stubs Only",
    )
    """If True, generate type stubs without implementation"""


# class SkillsFilesystemConfig(FileSystemConfig):
#     """Configuration for Skills filesystem."""

#     fs_type: Literal["skills"] = Field("skills", init=False)
#     """Skills filesystem type"""

#     _category: ClassVar[FilesystemCategoryType] = "wrapper"

#     wrapped_fs: str = Field(
#         title="Wrapped Filesystem",
#         examples=["file", "s3", "gcs"],
#         min_length=1,
#     )
#     """Type of filesystem to wrap"""

#     skills_dir: UPath | None = Field(
#         default=None,
#         title="Skills Directory",
#         examples=["/path/to/skills", "~/my-skills"],
#     )
#     """Directory containing skill definitions"""


CustomFilesystemConfig = (
    AppwriteFilesystemConfig
    | AsyncLocalFilesystemConfig
    | BaseModelFilesystemConfig
    | BaseModelInstanceFilesystemConfig
    | BeamFilesystemConfig
    | CliFilesystemConfig
    | DaytonaFilesystemConfig
    | DistributionFilesystemConfig
    | E2BFilesystemConfig
    | FlatUnionFilesystemConfig
    | GistFilesystemConfig
    | GitLabFilesystemConfig
    | HttpFilesystemConfig
    | HttpxFilesystemConfig
    | MarkdownFilesystemConfig
    | McpFilesystemConfig
    | McpToolsFilesystemConfig
    | MicrosandboxFilesystemConfig
    | ModalFilesystemConfig
    | ModuleFilesystemConfig
    | NotionFilesystemConfig
    | OpenApiFilesystemConfig
    | OverlayFilesystemConfig
    | PackageFilesystemConfig
    | PythonAstFilesystemConfig
    | SkillsFilesystemConfig
    | SqliteFilesystemConfig
    | SRTFilesystemConfig
    | TreeSitterFilesystemConfig
    | TypeAdapterFilesystemConfig
    | UnionFilesystemConfig
    | VercelFilesystemConfig
    | WikiFilesystemConfig
)
"""Union of all custom filesystem configurations."""
