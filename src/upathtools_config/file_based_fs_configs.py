"""Configuration models for filesystem implementations."""

from __future__ import annotations

from typing import Any, ClassVar, Literal

from pydantic import ConfigDict, Field
from upath import UPath  # noqa: TC002

from upathtools_config.base import FilesystemCategoryType, FileSystemConfig  # noqa: TC001


class FileBasedConfig(FileSystemConfig):
    """Base configuration for file-based filesystems.

    Provides common fields for filesystems that operate on source files
    with optional target protocol and options for accessing remote files.
    """

    target_protocol: str | None = Field(
        default=None, title="Target Protocol", examples=["file", "s3", "http"]
    )
    """Protocol for source file"""

    target_options: dict[str, Any] | None = Field(default=None, title="Target Protocol Options")
    """Options for target protocol"""


class MarkdownFilesystemConfig(FileBasedConfig):
    """Configuration for Markdown filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "Markdown Configuration"})

    fo: UPath = Field(title="Markdown File Path", examples=["/path/to/file.md"])
    """Path to markdown file"""

    type: Literal["md"] = Field("md", init=False)
    """Markdown filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"


class OpenApiFilesystemConfig(FileBasedConfig):
    """Configuration for OpenAPI schema filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "OpenAPI Configuration"})

    type: Literal["openapi"] = Field("openapi", init=False)
    """OpenAPI filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"

    fo: UPath = Field(
        title="OpenAPI Spec Path",
        examples=["/path/to/openapi.yaml", "/path/to/spec.json"],
    )
    """Path to OpenAPI specification file"""

    serializer: Literal["json", "json-formatted", "yaml"] = Field(
        default="json",
        title="Serializer Format",
        examples=["json", "json-formatted", "yaml"],
    )
    """Output format: 'json' (compact), 'json-formatted' , or 'yaml'"""


class JsonSchemaFilesystemConfig(FileBasedConfig):
    """Configuration for JSON Schema filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "JSON Schema Configuration"})

    type: Literal["jsonschema"] = Field("jsonschema", init=False)
    """JSON Schema filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"

    fo: UPath = Field(
        title="JSON Schema Path",
        examples=["/path/to/schema.json", "https://example.com/schema.json"],
    )
    """Path or URL to JSON Schema file"""

    headers: dict[str, str] | None = Field(
        default=None,
        title="HTTP Headers",
        examples=[{"Authorization": "Bearer token", "Accept": "application/json"}],
    )
    """HTTP headers for fetching remote schemas"""

    resolve_refs: bool = Field(default=False, title="Resolve References")
    """Whether to automatically resolve $ref references in the schema"""

    serializer: Literal["json", "json-formatted", "yaml"] = Field(
        default="json",
        title="Serializer Format",
        examples=["json", "json-formatted", "yaml"],
    )
    """Output format: 'json' (compact), 'json-formatted' , or 'yaml'"""


class SqliteFilesystemConfig(FileBasedConfig):
    """Configuration for SQLite database filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "SQLite Configuration"})

    type: Literal["sqlite"] = Field("sqlite", init=False)
    """SQLite filesystem type"""

    _category: ClassVar[FilesystemCategoryType] = "transform"

    db_path: str = Field(title="Database Path", examples=["/path/to/database.db"])
    """Path to SQLite database file"""


class TreeSitterFilesystemConfig(FileBasedConfig):
    """Configuration for tree-sitter code structure filesystem."""

    model_config = ConfigDict(json_schema_extra={"title": "TreeSitter Configuration"})

    type: Literal["treesitter"] = Field("treesitter", init=False)
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


FileBasedFilesystemConfig = (
    JsonSchemaFilesystemConfig
    | MarkdownFilesystemConfig
    | OpenApiFilesystemConfig
    | SqliteFilesystemConfig
    | TreeSitterFilesystemConfig
)
"""Union of all file-based filesystem configurations."""
