"""MCP Tools filesystem - exposes MCP tools as generated Python code files.

This filesystem presents MCP server tools as Python files that agents can
browse and load on-demand, implementing the "Code Mode" pattern for efficient
context usage.

Structure:
    /
    ├── _client.py          # FastMCP client boilerplate
    ├── tool_name.py        # Individual tool with model + async function
    └── ...
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, overload

from upathtools.async_helpers import sync_wrapper
from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath, FileInfo
from upathtools.log import get_logger


if TYPE_CHECKING:
    from fastmcp import Client as FastMCPClient
    from mcp.types import Tool as MCPTool


class McpToolInfo(FileInfo, total=False):
    """Info dict for MCP tools filesystem paths."""

    size: int | None
    tool_name: str | None
    description: str | None
    parameters: dict[str, Any] | None


logger = get_logger(__name__)


# Template for the shared client code
CLIENT_CODE_TEMPLATE = '''"""FastMCP client utilities for calling MCP tools."""

from __future__ import annotations

from typing import Any

from fastmcp import Client


_client: Client | None = None


def get_client() -> Client:
    """Get the FastMCP client instance."""
    if _client is None:
        raise RuntimeError("Client not initialized. Set _client before use.")
    return _client


async def call_mcp_tool(tool_name: str, arguments: dict[str, Any]) -> Any:
    """Call an MCP tool and return the result.

    Args:
        tool_name: Name of the tool to call
        arguments: Tool arguments

    Returns:
        Tool result
    """
    client = get_client()

    async with client:
        result = await client.call_tool(tool_name, arguments)

        if result.content:
            text_parts = []
            for content_block in result.content:
                if hasattr(content_block, "text"):
                    text_parts.append(content_block.text)
            return "\\n".join(text_parts) if text_parts else ""

        return ""
'''

# Template for individual tool files
TOOL_CODE_TEMPLATE = '''"""Generated code for MCP tool: {tool_name}."""

from __future__ import annotations

from typing import Any

{model_code}

async def {tool_name}({signature}) -> Any:
    """{description}"""
    from {client_module} import call_mcp_tool

    arguments = {arguments_dict}
    # Remove None values for optional parameters
    arguments = {{k: v for k, v in arguments.items() if v is not None}}
    return await call_mcp_tool("{tool_name}", arguments)
'''

# Template for stub-only tool files (no implementation)
TOOL_STUB_TEMPLATE = '''"""Stub for MCP tool: {tool_name}."""

from __future__ import annotations

from typing import Any

{model_code}

async def {tool_name}({signature}) -> Any:
    """{description}"""
    ...
'''


class MCPToolsPath(BaseUPath[McpToolInfo]):
    """MCP Tools-specific UPath implementation."""

    __slots__ = ()


class MCPToolsFileSystem(BaseAsyncFileSystem[MCPToolsPath, McpToolInfo]):
    """FSSpec filesystem exposing MCP tools as generated Python code.

    This enables the "Code Mode" pattern where agents discover tools
    by browsing a filesystem rather than loading all definitions upfront.
    """

    protocol = "mcptools"
    upath_cls = MCPToolsPath
    root_marker = ""
    cachable = False

    @overload
    def __init__(
        self,
        *,
        client: FastMCPClient[Any],
        stubs_only: bool = False,
        **kwargs: Any,
    ) -> None: ...

    @overload
    def __init__(
        self,
        *,
        url: str,
        stubs_only: bool = False,
        **kwargs: Any,
    ) -> None: ...

    @overload
    def __init__(
        self,
        *,
        server_cmd: list[str],
        stubs_only: bool = False,
        **kwargs: Any,
    ) -> None: ...

    def __init__(
        self,
        *,
        client: FastMCPClient[Any] | None = None,
        url: str | None = None,
        server_cmd: list[str] | None = None,
        stubs_only: bool = False,
        **kwargs: Any,
    ) -> None:
        """Initialize MCP Tools filesystem.

        Args:
            client: FastMCP client instance
            url: URL of MCP server
            server_cmd: Command to start MCP server
            stubs_only: If True, generate type stubs without implementation
            **kwargs: Additional fsspec options
        """
        from fastmcp.client import SSETransport, StdioTransport, StreamableHttpTransport

        if sum(x is not None for x in [client, url, server_cmd]) != 1:
            msg = "Exactly one of client, url, or server_cmd must be provided"
            raise ValueError(msg)

        super().__init__(**kwargs)
        self.stubs_only = stubs_only
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
            from fastmcp import Client as FastMCPClient

            self.client = FastMCPClient(url)
            self.url = url
        elif server_cmd is not None:
            from fastmcp import Client as FastMCPClient

            self.client = FastMCPClient(StdioTransport(server_cmd[0], server_cmd[1:]))
            self.server_cmd = server_cmd

        self._tools_cache: dict[str, MCPTool] = {}
        self._cache_valid = False

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, Any]:
        """Parse MCP Tools URL and return constructor kwargs."""
        path = path.removeprefix("mcptools://")

        if path.startswith("http"):
            return {"url": path}
        parts = path.split("/")
        return {"server_cmd": parts}

    async def _ensure_connected(self) -> None:
        """Ensure the MCP client is connected."""
        if not self.client.is_connected():
            await self.client.__aenter__()

    async def _refresh_tools(self) -> None:
        """Refresh the tools cache from MCP server."""
        await self._ensure_connected()

        try:
            tools = await self.client.list_tools()
            self._tools_cache = {tool.name: tool for tool in tools}
            self._cache_valid = True
            logger.debug("Refreshed %s MCP tools", len(self._tools_cache))
        except Exception:
            logger.exception("Failed to refresh MCP tools")
            raise

    @overload
    async def _ls(
        self, path: str, detail: Literal[True] = ..., **kwargs: Any
    ) -> list[McpToolInfo]: ...

    @overload
    async def _ls(self, path: str, detail: Literal[False], **kwargs: Any) -> list[str]: ...

    async def _ls(
        self, path: str, detail: bool = True, **kwargs: Any
    ) -> list[str] | list[McpToolInfo]:
        """List directory contents."""
        if not self._cache_valid:
            await self._refresh_tools()

        path = path.strip("/")

        if path == "":
            # Root directory - return _client.py + all tool files
            items: list[McpToolInfo] = [
                McpToolInfo(
                    name="_client.py",
                    type="file",
                    size=len(CLIENT_CODE_TEMPLATE),
                    tool_name=None,
                    description="FastMCP client utilities",
                )
            ]

            for tool_name, tool in self._tools_cache.items():
                items.append(
                    McpToolInfo(
                        name=_tool_to_filename(tool_name),
                        type="file",
                        size=None,
                        tool_name=tool_name,
                        description=tool.description,
                        parameters=tool.inputSchema,
                    )
                )

            if detail:
                return items
            return [item["name"] for item in items]

        # Check if it's a specific file
        if path == "_client.py":
            info = McpToolInfo(
                name="_client.py",
                type="file",
                size=len(CLIENT_CODE_TEMPLATE),
                tool_name=None,
                description="FastMCP client utilities",
            )
            return [info] if detail else ["_client.py"]  # type: ignore[list-item]

        t_name = _filename_to_tool(path)
        if t_name and t_name in self._tools_cache:
            tool = self._tools_cache[t_name]
            info = McpToolInfo(
                name=path,
                type="file",
                size=None,
                tool_name=t_name,
                description=tool.description,
                parameters=tool.inputSchema,
            )
            return [info] if detail else [path]  # type: ignore[list-item]

        return []

    ls = sync_wrapper(_ls)

    async def _cat_file(
        self,
        path: str,
        start: int | None = None,
        end: int | None = None,
        **kwargs: Any,
    ) -> bytes:
        """Read file contents - generates Python code for tools."""
        if not self._cache_valid:
            await self._refresh_tools()

        path = path.strip("/")

        if path == "_client.py":
            content = CLIENT_CODE_TEMPLATE
        else:
            tool_name = _filename_to_tool(path)
            if tool_name is None or tool_name not in self._tools_cache:
                msg = f"Tool not found: {path}"
                raise FileNotFoundError(msg)

            tool = self._tools_cache[tool_name]
            content = _generate_tool_code(tool, stubs_only=self.stubs_only)
        content_bytes = content.encode("utf-8")

        if start is not None or end is not None:
            return content_bytes[start:end]
        return content_bytes

    cat_file = sync_wrapper(_cat_file)  # pyright: ignore[reportAssignmentType]

    async def _info(self, path: str, **kwargs: Any) -> McpToolInfo:
        """Get file information."""
        if not self._cache_valid:
            await self._refresh_tools()

        path = path.strip("/")

        if path == "":
            return McpToolInfo(name="", type="directory", size=None)

        if path == "_client.py":
            return McpToolInfo(
                name="_client.py",
                type="file",
                size=len(CLIENT_CODE_TEMPLATE),
                tool_name=None,
                description="FastMCP client utilities",
            )

        tool_name = _filename_to_tool(path)
        if tool_name and tool_name in self._tools_cache:
            tool = self._tools_cache[tool_name]
            return McpToolInfo(
                name=path,
                type="file",
                size=None,
                tool_name=tool_name,
                description=tool.description,
                parameters=tool.inputSchema,
            )

        msg = f"Path not found: {path}"
        raise FileNotFoundError(msg)

    info = sync_wrapper(_info)

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        """Check if path exists."""
        try:
            await self._info(path)
        except FileNotFoundError:
            return False
        else:
            return True

    exists = sync_wrapper(_exists)  # pyright: ignore[reportAssignmentType]

    async def _isfile(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a file."""
        try:
            info = await self._info(path)
            return info.get("type") == "file"
        except FileNotFoundError:
            return False

    isfile = sync_wrapper(_isfile)

    async def _isdir(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a directory."""
        return path.strip("/") == ""

    isdir = sync_wrapper(_isdir)

    def _strip_protocol(self, path: str) -> str:
        """Strip protocol from path."""
        if path.startswith("mcptools://"):
            return path[11:]
        return path

    def invalidate_cache(self, path: str | None = None) -> None:
        """Invalidate the tools cache."""
        self._cache_valid = False
        self._tools_cache.clear()
        logger.debug("Invalidated MCP tools cache")

    async def _close(self) -> None:
        """Close the MCP client connection."""
        if self.client.is_connected():
            await self.client.__aexit__(None, None, None)
            logger.debug("Closed MCP client connection")

    close = sync_wrapper(_close)

    # Read-only filesystem
    async def _put_file(self, lpath: str, rpath: str, **kwargs: Any) -> None:
        msg = "MCPToolsFileSystem is read-only"
        raise NotImplementedError(msg)

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        msg = "MCPToolsFileSystem is read-only"
        raise NotImplementedError(msg)

    mkdir = sync_wrapper(_mkdir)

    async def _rmdir(self, path: str, **kwargs: Any) -> None:
        msg = "MCPToolsFileSystem is read-only"
        raise NotImplementedError(msg)

    rmdir = sync_wrapper(_rmdir)

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        msg = "MCPToolsFileSystem is read-only"
        raise NotImplementedError(msg)

    rm_file = sync_wrapper(_rm_file)

    async def _touch(self, path: str, **kwargs: Any) -> None:
        msg = "MCPToolsFileSystem is read-only"
        raise NotImplementedError(msg)

    touch = sync_wrapper(_touch)


def _tool_to_filename(tool_name: str) -> str:
    """Convert tool name to filename."""
    return f"{tool_name}.py"


def _filename_to_tool(filename: str) -> str | None:
    """Extract tool name from filename."""
    filename = filename.strip("/")
    if filename.endswith(".py") and not filename.startswith("_"):
        return filename[:-3]
    return None


def _generate_tool_code(tool: MCPTool, stubs_only: bool) -> str:
    """Generate Python code for a single tool."""
    properties = tool.inputSchema.get("properties", {})
    required = set(tool.inputSchema.get("required", []))

    # Build signature parts
    sig_parts: list[str] = []
    arguments_parts: list[str] = []

    for param_name, param_info in properties.items():
        param_type = _json_type_to_python(param_info.get("type", "Any"))
        is_required = param_name in required

        if is_required:
            sig_parts.append(f"{param_name}: {param_type}")
        else:
            sig_parts.append(f"{param_name}: {param_type} | None = None")

        arguments_parts.append(f'"{param_name}": {param_name}')

    signature = ", ".join(sig_parts) if sig_parts else ""
    arguments_dict = "{" + ", ".join(arguments_parts) + "}" if arguments_parts else "{}"

    template = TOOL_STUB_TEMPLATE if stubs_only else TOOL_CODE_TEMPLATE

    return template.format(
        tool_name=tool.name,
        description=tool.description or f"Call the {tool.name} tool",
        signature=signature,
        arguments_dict=arguments_dict,
        model_code="",  # Could add Pydantic models here if needed
        client_module="_client",
    )


def _json_type_to_python(json_type: str | list[str]) -> str:
    """Convert JSON schema type to Python type annotation."""
    if isinstance(json_type, list):
        types = [_json_type_to_python(t) for t in json_type]
        return " | ".join(types)

    type_map = {
        "string": "str",
        "integer": "int",
        "number": "float",
        "boolean": "bool",
        "array": "list[Any]",
        "object": "dict[str, Any]",
        "null": "None",
    }
    return type_map.get(json_type, "Any")


if __name__ == "__main__":
    fs = MCPToolsFileSystem(server_cmd=["uvx", "mcp-server-git"])
    print(fs.ls(""))
