"""File-based filesystems that expose internal file structure.

These filesystems parse specific file types and expose their internal structure
as a virtual filesystem. For example, MarkdownFileSystem exposes markdown headers
as directories.
"""

from typing import TYPE_CHECKING

from upathtools._lazy import lazy_exports


if TYPE_CHECKING:
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

    FileFileSystem = (
        JsonSchemaFileSystem
        | MarkdownFileSystem
        | OpenAPIFileSystem
        | SqliteFileSystem
        | TreeSitterFileSystem
    )

__getattr__, __dir__ = lazy_exports(
    __name__,
    {
        "JsonSchemaFileSystem": "upathtools.filesystems.file_filesystems.jsonschema_fs",
        "JsonSchemaInfo": "upathtools.filesystems.file_filesystems.jsonschema_fs",
        "JsonSchemaPath": "upathtools.filesystems.file_filesystems.jsonschema_fs",
        "MarkdownFileSystem": "upathtools.filesystems.file_filesystems.markdown_fs",
        "MarkdownInfo": "upathtools.filesystems.file_filesystems.markdown_fs",
        "MarkdownNode": "upathtools.filesystems.file_filesystems.markdown_fs",
        "MarkdownPath": "upathtools.filesystems.file_filesystems.markdown_fs",
        "OpenAPIFileSystem": "upathtools.filesystems.file_filesystems.openapi_fs",
        "OpenApiInfo": "upathtools.filesystems.file_filesystems.openapi_fs",
        "OpenAPIPath": "upathtools.filesystems.file_filesystems.openapi_fs",
        "SqliteFileSystem": "upathtools.filesystems.file_filesystems.sqlite_fs",
        "SqliteInfo": "upathtools.filesystems.file_filesystems.sqlite_fs",
        "SqlitePath": "upathtools.filesystems.file_filesystems.sqlite_fs",
        "CodeNode": "upathtools.filesystems.file_filesystems.treesitter_fs",
        "TreeSitterFileSystem": "upathtools.filesystems.file_filesystems.treesitter_fs",
        "TreeSitterInfo": "upathtools.filesystems.file_filesystems.treesitter_fs",
        "TreeSitterPath": "upathtools.filesystems.file_filesystems.treesitter_fs",
    },
    {
        "FileFileSystem": (
            "JsonSchemaFileSystem",
            "MarkdownFileSystem",
            "OpenAPIFileSystem",
            "SqliteFileSystem",
            "TreeSitterFileSystem",
        ),
    },
)

__all__ = [
    "CodeNode",
    "FileFileSystem",
    "JsonSchemaFileSystem",
    "JsonSchemaInfo",
    "JsonSchemaPath",
    "MarkdownFileSystem",
    "MarkdownInfo",
    "MarkdownNode",
    "MarkdownPath",
    "OpenAPIFileSystem",
    "OpenAPIPath",
    "OpenApiInfo",
    "SqliteFileSystem",
    "SqliteInfo",
    "SqlitePath",
    "TreeSitterFileSystem",
    "TreeSitterInfo",
    "TreeSitterPath",
]
