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


class BeamFilesystemConfig(FileSystemConfig):
    """Configuration for Beam sandbox filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "Beam Configuration"})

    type: Literal["beam"] = Field("beam", init=False)
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

    model_config = ConfigDict(json_schema_extra={"title": "Daytona Configuration"})

    type: Literal["daytona"] = Field("daytona", init=False)
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

    model_config = ConfigDict(json_schema_extra={"title": "E2B Configuration"})

    type: Literal["e2b"] = Field("e2b", init=False)
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

    model_config = ConfigDict(json_schema_extra={"title": "Microsandbox Configuration"})

    type: Literal["microsandbox"] = Field("microsandbox", init=False)
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

    model_config = ConfigDict(json_schema_extra={"title": "Modal Configuration"})

    type: Literal["modal"] = Field("modal", init=False)
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

    model_config = ConfigDict(json_schema_extra={"title": "Vercel Configuration"})

    type: Literal["vercel"] = Field("vercel", init=False)
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

    model_config = ConfigDict(json_schema_extra={"title": "SRT Configuration"})

    type: Literal["srt"] = Field("srt", init=False)
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


SandboxFilesystemConfig = (
    BeamFilesystemConfig
    | DaytonaFilesystemConfig
    | E2BFilesystemConfig
    | ModalFilesystemConfig
    | MicrosandboxFilesystemConfig
    | SRTFilesystemConfig
    | VercelFilesystemConfig
)
"""Union of all sandbox filesystem configurations."""
