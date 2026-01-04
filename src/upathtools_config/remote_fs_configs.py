"""Configuration models for filesystem implementations."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Literal

from pydantic import ConfigDict, Field, SecretStr

from upathtools_config.base import (
    FilesystemCategoryType,  # noqa: TC001
    FileSystemConfig,
)


if TYPE_CHECKING:
    from pydantic import SecretStr


class GistFilesystemConfig(FileSystemConfig):
    """Configuration for GitHub Gist filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "GitHub Gist Configuration"})

    type: Literal["gist"] = Field("gist", init=False)
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


class WikiFilesystemConfig(FileSystemConfig):
    """Configuration for GitHub Wiki filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "GitHub Wiki Configuration"})

    type: Literal["wiki"] = Field("wiki", init=False)
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

    model_config = ConfigDict(json_schema_extra={"title": "Appwrite Configuration"})

    type: Literal["appwrite"] = Field("appwrite", init=False)
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


class McpFilesystemConfig(FileSystemConfig):
    """Configuration for MCP (Model Context Protocol) filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "MCP Configuration"})

    type: Literal["mcp"] = Field("mcp", init=False)
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

    model_config = ConfigDict(json_schema_extra={"title": "Notion Configuration"})

    type: Literal["notion"] = Field("notion", init=False)
    """Notion filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    token: SecretStr = Field(title="Integration Token")
    """Notion integration token"""

    parent_page_id: str = Field(
        title="Parent Page ID",
        examples=["64b1f2c8e8c9a123456789012345"],
        pattern=r"^[a-f0-9\-]+$",
        min_length=32,
        max_length=36,
    )
    """ID of the parent page where new pages will be created"""


class GitLabFilesystemConfig(FileSystemConfig):
    """Configuration for GitLab repository filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "GitLab Configuration"})

    type: Literal["gitlab"] = Field("gitlab", init=False)
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

    private_token: SecretStr | None = Field(default=None, title="Private Token")
    """GitLab private/personal access token (or set GITLAB_TOKEN env var)"""


class LinearFilesystemConfig(FileSystemConfig):
    """Configuration for Linear Issues filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "Linear Configuration"})

    type: Literal["linear"] = Field("linear", init=False)
    """Linear filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "base"

    api_key: SecretStr | None = Field(default=None, title="API Key")
    """Linear API key for authentication (or set LINEAR_API_KEY env var)"""

    extended: bool = Field(default=False, title="Extended Mode")
    """Whether to use extended mode with issue directories.

    If True, issues are folders with comments as sub-files
    """

    group_by: Literal["project"] | None = Field(default=None, title="Group By")
    """Grouping strategy for issues.

    How to group issues. None for flat, 'project' for project folders
    """

    timeout: float | None = Field(default=None, title="Timeout", gt=0)
    """Request timeout in seconds"""


class McpToolsFilesystemConfig(FileSystemConfig):
    """Configuration for MCP tools filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "MCP Tools Configuration"})

    type: Literal["mcptools"] = Field("mcptools", init=False)
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

    stubs_only: bool = Field(default=False, title="Stubs Only")
    """If True, generate type stubs without implementation"""


RemoteFilesystemConfig = (
    AppwriteFilesystemConfig
    | GistFilesystemConfig
    | GitLabFilesystemConfig
    | LinearFilesystemConfig
    | McpFilesystemConfig
    | McpToolsFilesystemConfig
    | NotionFilesystemConfig
    | WikiFilesystemConfig
)
"""Union of all remote filesystem configurations."""
