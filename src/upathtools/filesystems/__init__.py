"""Filesystem implementations for upathtools."""

from typing import TYPE_CHECKING

from upathtools._lazy import lazy_exports


if TYPE_CHECKING:
    from .remote_filesystems.appwrite_fs import AppwriteFileSystem, AppwritePath
    from .remote_filesystems.gist_fs import GistFileSystem, GistPath
    from .remote_filesystems.gitlab_fs import GitLabFileSystem, GitLabPath
    from .remote_filesystems.linear_fs import LinearIssueFileSystem, LinearIssuePath
    from .remote_filesystems.mcp_fs import MCPFileSystem, MCPPath
    from .remote_filesystems.mcp_tools_fs import MCPToolsFileSystem, MCPToolsPath
    from .remote_filesystems.notion_fs import NotionFileSystem, NotionPath
    from .remote_filesystems.wiki_fs import WikiFileSystem, WikiPath
    from .remote_filesystems.issue_fs import IssueFileSystem, IssuePath
    from .file_filesystems.jsonschema_fs import JsonSchemaFileSystem, JsonSchemaPath
    from .file_filesystems.markdown_fs import MarkdownFileSystem, MarkdownPath
    from .file_filesystems.openapi_fs import OpenAPIFileSystem, OpenAPIPath
    from .file_filesystems.sqlite_fs import SqliteFileSystem, SqlitePath
    from .file_filesystems.treesitter_fs import TreeSitterFileSystem, TreeSitterPath
    from .sandbox_filesystems.beam_fs import BeamFS, BeamPath
    from .sandbox_filesystems.codex_fs import CodexFS, CodexPath
    from .sandbox_filesystems.cloudflare_fs import CloudflareFS, CloudflarePath
    from .sandbox_filesystems.daytona_fs import DaytonaFS, DaytonaPath
    from .sandbox_filesystems.e2b_fs import E2BFS, E2BPath
    from .sandbox_filesystems.hopx_fs import HopXFS, HopXPath
    from .sandbox_filesystems.modal_fs import ModalFS, ModalPath
    from .sandbox_filesystems.sprites_fs import SpritesFS, SpritesPath
    from .sandbox_filesystems.vercel_fs import VercelFS, VercelPath
    from .sandbox_filesystems.microsandbox_fs import MicrosandboxFS, MicrosandboxPath
    from .basemodel_fs import BaseModelFileSystem, BaseModelPath
    from .basemodel_instance_fs import BaseModelInstanceFileSystem, BaseModelInstancePath
    from .distribution_fs import DistributionFileSystem, DistributionPath
    from .combining_filesystems import FlatUnionFileSystem, FlatUnionPath
    from .combining_filesystems import OverlayFileSystem, OverlayPath
    from .combining_filesystems import UnionFileSystem, UnionPath
    from .package_fs import PackageFileSystem, PackagePath
    from .httpx_fs import HTTPFileSystem, HttpPath
    from .async_local_fs import AsyncLocalFileSystem, LocalPath
    from .isolated_memory_fs import IsolatedMemoryFileSystem

__getattr__, __dir__ = lazy_exports(
    __name__,
    {
        "AppwriteFileSystem": ".remote_filesystems.appwrite_fs",
        "AppwritePath": ".remote_filesystems.appwrite_fs",
        "GistFileSystem": ".remote_filesystems.gist_fs",
        "GistPath": ".remote_filesystems.gist_fs",
        "GitLabFileSystem": ".remote_filesystems.gitlab_fs",
        "GitLabPath": ".remote_filesystems.gitlab_fs",
        "LinearIssueFileSystem": ".remote_filesystems.linear_fs",
        "LinearIssuePath": ".remote_filesystems.linear_fs",
        "MCPFileSystem": ".remote_filesystems.mcp_fs",
        "MCPPath": ".remote_filesystems.mcp_fs",
        "MCPToolsFileSystem": ".remote_filesystems.mcp_tools_fs",
        "MCPToolsPath": ".remote_filesystems.mcp_tools_fs",
        "NotionFileSystem": ".remote_filesystems.notion_fs",
        "NotionPath": ".remote_filesystems.notion_fs",
        "WikiFileSystem": ".remote_filesystems.wiki_fs",
        "WikiPath": ".remote_filesystems.wiki_fs",
        "IssueFileSystem": ".remote_filesystems.issue_fs",
        "IssuePath": ".remote_filesystems.issue_fs",
        "JsonSchemaFileSystem": ".file_filesystems.jsonschema_fs",
        "JsonSchemaPath": ".file_filesystems.jsonschema_fs",
        "MarkdownFileSystem": ".file_filesystems.markdown_fs",
        "MarkdownPath": ".file_filesystems.markdown_fs",
        "OpenAPIFileSystem": ".file_filesystems.openapi_fs",
        "OpenAPIPath": ".file_filesystems.openapi_fs",
        "SqliteFileSystem": ".file_filesystems.sqlite_fs",
        "SqlitePath": ".file_filesystems.sqlite_fs",
        "TreeSitterFileSystem": ".file_filesystems.treesitter_fs",
        "TreeSitterPath": ".file_filesystems.treesitter_fs",
        "BeamFS": ".sandbox_filesystems.beam_fs",
        "BeamPath": ".sandbox_filesystems.beam_fs",
        "CodexFS": ".sandbox_filesystems.codex_fs",
        "CodexPath": ".sandbox_filesystems.codex_fs",
        "CloudflareFS": ".sandbox_filesystems.cloudflare_fs",
        "CloudflarePath": ".sandbox_filesystems.cloudflare_fs",
        "DaytonaFS": ".sandbox_filesystems.daytona_fs",
        "DaytonaPath": ".sandbox_filesystems.daytona_fs",
        "E2BFS": ".sandbox_filesystems.e2b_fs",
        "E2BPath": ".sandbox_filesystems.e2b_fs",
        "HopXFS": ".sandbox_filesystems.hopx_fs",
        "HopXPath": ".sandbox_filesystems.hopx_fs",
        "ModalFS": ".sandbox_filesystems.modal_fs",
        "ModalPath": ".sandbox_filesystems.modal_fs",
        "SpritesFS": ".sandbox_filesystems.sprites_fs",
        "SpritesPath": ".sandbox_filesystems.sprites_fs",
        "VercelFS": ".sandbox_filesystems.vercel_fs",
        "VercelPath": ".sandbox_filesystems.vercel_fs",
        "MicrosandboxFS": ".sandbox_filesystems.microsandbox_fs",
        "MicrosandboxPath": ".sandbox_filesystems.microsandbox_fs",
        "BaseModelFileSystem": ".basemodel_fs",
        "BaseModelPath": ".basemodel_fs",
        "BaseModelInstanceFileSystem": ".basemodel_instance_fs",
        "BaseModelInstancePath": ".basemodel_instance_fs",
        "DistributionFileSystem": ".distribution_fs",
        "DistributionPath": ".distribution_fs",
        "FlatUnionFileSystem": ".combining_filesystems",
        "FlatUnionPath": ".combining_filesystems",
        "OverlayFileSystem": ".combining_filesystems",
        "OverlayPath": ".combining_filesystems",
        "UnionFileSystem": ".combining_filesystems",
        "UnionPath": ".combining_filesystems",
        "PackageFileSystem": ".package_fs",
        "PackagePath": ".package_fs",
        "HTTPFileSystem": ".httpx_fs",
        "HttpPath": ".httpx_fs",
        "AsyncLocalFileSystem": ".async_local_fs",
        "LocalPath": ".async_local_fs",
        "IsolatedMemoryFileSystem": ".isolated_memory_fs",
    },
)

__all__ = [
    "E2BFS",
    "AppwriteFileSystem",
    "AppwritePath",
    "AsyncLocalFileSystem",
    "BaseModelFileSystem",
    "BaseModelInstanceFileSystem",
    "BaseModelInstancePath",
    "BaseModelPath",
    "BeamFS",
    "BeamPath",
    "CloudflareFS",
    "CloudflarePath",
    "CodexFS",
    "CodexPath",
    "DaytonaFS",
    "DaytonaPath",
    "DistributionFileSystem",
    "DistributionPath",
    "E2BPath",
    "FlatUnionFileSystem",
    "FlatUnionPath",
    "GistFileSystem",
    "GistPath",
    "GitLabFileSystem",
    "GitLabPath",
    "HTTPFileSystem",
    "HopXFS",
    "HopXPath",
    "HttpPath",
    "IsolatedMemoryFileSystem",
    "IssueFileSystem",
    "IssuePath",
    "JsonSchemaFileSystem",
    "JsonSchemaPath",
    "LinearIssueFileSystem",
    "LinearIssuePath",
    "LocalPath",
    "MCPFileSystem",
    "MCPPath",
    "MCPToolsFileSystem",
    "MCPToolsPath",
    "MarkdownFileSystem",
    "MarkdownPath",
    "MicrosandboxFS",
    "MicrosandboxPath",
    "ModalFS",
    "ModalPath",
    "NotionFileSystem",
    "NotionPath",
    "OpenAPIFileSystem",
    "OpenAPIPath",
    "OverlayFileSystem",
    "OverlayPath",
    "PackageFileSystem",
    "PackagePath",
    "SpritesFS",
    "SpritesPath",
    "SqliteFileSystem",
    "SqlitePath",
    "TreeSitterFileSystem",
    "TreeSitterPath",
    "UnionFileSystem",
    "UnionPath",
    "VercelFS",
    "VercelPath",
    "WikiFileSystem",
    "WikiPath",
]
