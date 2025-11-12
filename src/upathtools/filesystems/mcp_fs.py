"""MCP (Model Context Protocol) filesystem implementation for upathtools.

This filesystem exposes MCP resources through the fsspec interface,
allowing access to resources provided by MCP servers using standard
filesystem operations.
"""

from __future__ import annotations

import base64
from typing import TYPE_CHECKING, Any, overload
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

    @overload
    def __init__(
        self,
        *,
        client: FastMCPClient,
        **kwargs: Any,
    ) -> None: ...

    @overload
    def __init__(
        self,
        *,
        url: str,
        **kwargs: Any,
    ) -> None: ...

    @overload
    def __init__(
        self,
        *,
        server_cmd: list[str],
        **kwargs: Any,
    ) -> None: ...

    def __init__(
        self,
        *,
        client: FastMCPClient | None = None,
        url: str | None = None,
        server_cmd: list[str] | None = None,
        **kwargs: Any,
    ):
        """Initialize MCP filesystem.

        Only one of client, url or server_cmd may be provided.

        Args:
            client: FastMCP client instance for communicating with MCP server
            url: URL of MCP server
            server_cmd: Command to start MCP server
            **kwargs: Additional fsspec options
        """
        # Validate that exactly one parameter is provided
        from fastmcp.client import SSETransport, StdioTransport, StreamableHttpTransport

        provided = sum(x is not None for x in [client, url, server_cmd])
        if provided != 1:
            msg = "Exactly one of client, url, or server_cmd must be provided"
            raise ValueError(msg)

        super().__init__(**kwargs)
        self.server_cmd = None
        self.url = None
        if client is not None:
            self.client = client
            match self.client.transport:
                case SSETransport() | StreamableHttpTransport() as tp:
                    self.url = tp.url
                case StdioTransport() as tp:
                    self.server_cmd = [tp.command, *tp.args]
        elif url is not None:
            # Import here to avoid circular imports
            from fastmcp import Client as FastMCPClient

            self.client = FastMCPClient(url)
        elif server_cmd is not None:
            # Import here to avoid circular imports
            from fastmcp import Client as FastMCPClient

            self.client = FastMCPClient(StdioTransport(server_cmd[0], server_cmd[1:]))

        self._resource_cache: dict[str, dict[str, Any]] = {}
        self._cache_valid = False

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, Any]:
        """Parse MCP URL and return constructor kwargs.

        Args:
            path: MCP URL path

        Returns:
            Dictionary with either 'url' or 'server_cmd' key
        """
        path = path.removeprefix("mcp://")

        if path.startswith("http"):
            return {"url": path}
        # Assume it's a command for stdio
        parts = path.split("/")
        return {"server_cmd": parts}

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
    cat_file = sync_wrapper(_cat_file)  # pyright: ignore[reportAssignmentType]

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
    exists = sync_wrapper(_exists)  # pyright: ignore[reportAssignmentType]

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


if __name__ == "__main__":
    fs = MCPFileSystem(server_cmd=["uvx", "mcp-server-git"])
    print(fs.ls(""))
