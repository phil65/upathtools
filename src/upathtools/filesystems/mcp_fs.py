"""MCP (Model Context Protocol) filesystem implementation for upathtools.

This filesystem exposes MCP resources through the fsspec interface,
allowing access to resources provided by MCP servers using standard
filesystem operations.
"""

from __future__ import annotations

import base64
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, unquote

from fsspec.asyn import sync_wrapper

from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath
from upathtools.log import get_logger


if TYPE_CHECKING:
    from fastmcp import Client as FastMCPClient


logger = get_logger(__name__)


class MCPPath(BaseUPath):
    """MCP-specific UPath implementation."""

    __slots__ = ()


class MCPFileSystem(BaseAsyncFileSystem[MCPPath]):
    """FSSpec filesystem that exposes MCP resources.

    This filesystem wraps a FastMCP client to expose MCP resources
    as files in a virtual filesystem. Resources are mapped to paths
    using URL encoding to handle special characters in URIs.
    """

    protocol = "mcp"
    upath_cls = MCPPath
    root_marker = "/"
    cachable = False

    def __init__(self, client: FastMCPClient, **kwargs: Any):
        """Initialize MCP filesystem.

        Args:
            client: FastMCP client instance for communicating with MCP server
            **kwargs: Additional fsspec options
        """
        super().__init__(**kwargs)
        self.client = client
        self._resource_cache: dict[str, dict[str, Any]] = {}
        self._cache_valid = False

    async def _ensure_connected(self):
        """Ensure the MCP client is connected."""
        if not self.client.is_connected():
            await self.client.__aenter__()

    def _uri_to_path(self, uri: str) -> str:
        """Convert MCP resource URI to filesystem path.

        Args:
            uri: MCP resource URI like 'file:///path/to/file.txt'

        Returns:
            Filesystem path like '/file___path_to_file.txt'
        """
        # URL encode the URI to handle special characters safely
        encoded_uri = quote(uri, safe="")
        return "/" + encoded_uri

    def _path_to_uri(self, path: str) -> str:
        """Convert filesystem path back to MCP resource URI.

        Args:
            path: Filesystem path like '/file___path_to_file.txt'

        Returns:
            MCP resource URI like 'file:///path/to/file.txt'
        """
        # Remove leading slash and URL decode
        path = path.lstrip("/")
        return unquote(path)

    async def _refresh_resources(self):
        """Refresh the resource cache from MCP server."""
        await self._ensure_connected()

        try:
            # List all available resources
            result = await self.client.list_resources()
            self._resource_cache = {}

            for resource in result:
                path = self._uri_to_path(str(resource.uri))
                self._resource_cache[path] = {
                    "name": path,
                    "size": resource.size,
                    "type": "file",
                    "uri": str(resource.uri),
                    "mimeType": resource.mimeType,
                    "description": resource.description,
                    "title": resource.title,
                }

            self._cache_valid = True
            logger.debug("Refreshed %s MCP resources", len(self._resource_cache))

        except Exception:
            logger.exception("Failed to refresh MCP resources")
            raise

    async def _ls(self, path: str = "", detail: bool = True, **kwargs: Any):
        """List directory contents asynchronously."""
        if not self._cache_valid:
            await self._refresh_resources()

        path = path.rstrip("/")
        if path == "":
            path = "/"

        if path == "/":
            # Root directory - return all resources
            items = list(self._resource_cache.values())
        elif path in self._resource_cache:
            # Specific resource
            items = [self._resource_cache[path]]
        else:
            items = []

        if detail:
            return items
        return [item["name"] for item in items]

    # Sync wrapper
    ls = sync_wrapper(_ls)

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any
    ) -> bytes:
        """Read a file's contents asynchronously.

        Args:
            path: File path to read
            start: Start byte position
            end: End byte position
            **kwargs: Additional parameters

        Returns:
            File contents as bytes
        """
        # Convert path back to URI
        uri = self._path_to_uri(path)
        content = await self._read_resource_async(uri)

        if start is not None or end is not None:
            return content[start:end]
        return content

    async def _read_resource_async(self, uri: str) -> bytes:
        """Read MCP resource content asynchronously.

        Args:
            uri: MCP resource URI

        Returns:
            Resource content as bytes
        """
        import mcp.types

        await self._ensure_connected()

        try:
            result = await self.client.read_resource(uri)
            if not result:
                return b""

            content = result[0]  # Get first content item
            match content:
                case mcp.types.TextResourceContents(text=text):
                    return text.encode("utf-8")
                case mcp.types.BlobResourceContents(blob=blob):
                    return base64.b64decode(blob)
        except Exception as e:
            logger.exception("Failed to read MCP resource from %s", uri)
            msg = f"Resource not found: {uri}"
            raise FileNotFoundError(msg) from e
        else:
            return b""

    # Sync wrapper
    cat_file = sync_wrapper(_cat_file)

    async def _info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Get file information asynchronously."""
        if not self._cache_valid:
            await self._refresh_resources()

        if path in self._resource_cache:
            return self._resource_cache[path].copy()
        msg = f"Path not found: {path}"
        raise FileNotFoundError(msg)

    # Sync wrapper
    info = sync_wrapper(_info)

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        """Check if path exists asynchronously."""
        try:
            await self._info(path)
        except FileNotFoundError:
            return False
        else:
            return True

    # Sync wrapper
    exists = sync_wrapper(_exists)

    async def _isfile(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a file asynchronously."""
        try:
            info = await self._info(path)
            return info.get("type") == "file"
        except FileNotFoundError:
            return False

    # Sync wrapper
    isfile = sync_wrapper(_isfile)

    async def _isdir(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a directory asynchronously."""
        # Only root is considered a directory in this filesystem
        return path in {"/", ""}

    # Sync wrapper
    isdir = sync_wrapper(_isdir)

    def _strip_protocol(self, path: str) -> str:
        """Strip protocol from path."""
        if path.startswith("mcp://"):
            return path[6:]
        return path

    # Read-only filesystem - these methods raise NotImplementedError
    async def _put_file(self, lpath: str, rpath: str, **kwargs: Any):
        """Put file (not supported)."""
        msg = "MCP filesystem is read-only"
        raise NotImplementedError(msg)

    async def _mkdir(self, path: str, **kwargs: Any):
        """Create directory (not supported)."""
        msg = "MCP filesystem is read-only"
        raise NotImplementedError(msg)

    mkdir = sync_wrapper(_mkdir)

    async def _rmdir(self, path: str, **kwargs: Any):
        """Remove directory (not supported)."""
        msg = "MCP filesystem is read-only"
        raise NotImplementedError(msg)

    rmdir = sync_wrapper(_rmdir)

    async def _rm_file(self, path: str, **kwargs: Any):
        """Remove file (not supported)."""
        msg = "MCP filesystem is read-only"
        raise NotImplementedError(msg)

    rm_file = sync_wrapper(_rm_file)

    async def _touch(self, path: str, **kwargs: Any):
        """Touch file (not supported)."""
        msg = "MCP filesystem is read-only"
        raise NotImplementedError(msg)

    touch = sync_wrapper(_touch)

    def invalidate_cache(self, path: str | None = None):
        """Invalidate the resource cache.

        Args:
            path: Specific path to invalidate (ignored - invalidates all)
        """
        self._cache_valid = False
        self._resource_cache.clear()
        logger.debug("Invalidated MCP resource cache")


# Register the filesystem with fsspec
try:
    import fsspec

    fsspec.register_implementation("mcp", MCPFileSystem)
except ImportError:
    pass
