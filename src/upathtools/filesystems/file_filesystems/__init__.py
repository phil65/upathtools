"""File-based filesystems that expose internal file structure.

These filesystems parse specific file types and expose their internal structure
as a virtual filesystem. For example, MarkdownFileSystem exposes markdown headers
as directories.
"""

from __future__ import annotations

from upathtools.filesystems.file_filesystems.jsonschema_fs import (
    JsonSchemaFileSystem,
    JsonSchemaInfo,
    JsonSchemaPath,
)
from upathtools.filesystems.file_filesystems.markdown_fs import (
    MarkdownFileSystem,
    MarkdownInfo,
    MarkdownNode,
    MarkdownPath,
)
from upathtools.filesystems.file_filesystems.openapi_fs import (
    OpenAPIFileSystem,
    OpenApiInfo,
    OpenAPIPath,
)

from upathtools.filesystems.file_filesystems.sqlite_fs import (
    SqliteFileSystem,
    SqliteInfo,
    SqlitePath,
)
from upathtools.filesystems.file_filesystems.treesitter_fs import (
    CodeNode,
    TreeSitterFileSystem,
    TreeSitterInfo,
    TreeSitterPath,
)

__all__ = [
    # TreeSitter
    "CodeNode",
    # JSON Schema
    "JsonSchemaFileSystem",
    "JsonSchemaInfo",
    "JsonSchemaPath",
    # Markdown
    "MarkdownFileSystem",
    "MarkdownInfo",
    "MarkdownNode",
    "MarkdownPath",
    # OpenAPI
    "OpenAPIFileSystem",
    "OpenAPIPath",
    "OpenApiInfo",
    # SQLite
    "SqliteFileSystem",
    "SqliteInfo",
    "SqlitePath",
    "TreeSitterFileSystem",
    "TreeSitterInfo",
    "TreeSitterPath",
]
