"""Tests for MCPToolsFileSystem."""

from __future__ import annotations

from pathlib import Path

from fastmcp import Client
from fastmcp.client import StdioTransport
import pytest

from upathtools.filesystems.remote_filesystems.mcp_tools_fs import MCPToolsFileSystem


@pytest.fixture
def test_server_path() -> Path:
    """Get path to the test MCP tools server."""
    return Path(__file__).parent / "mcp_tools_server.py"


async def test_list_tools(test_server_path: Path):
    """Test listing available tools as files."""
    transport = StdioTransport(
        command="uv",
        args=["run", str(test_server_path)],
    )
    async with Client(transport=transport) as client:
        fs = MCPToolsFileSystem(client=client)

        files = await fs._ls("/", detail=False)

        # Should have _client.py + 3 tool files
        assert "/_client.py" in files
        assert "/greet.py" in files
        assert "/add_numbers.py" in files
        assert "/search_items.py" in files


async def test_list_tools_with_detail(test_server_path: Path):
    """Test listing tools with detailed info."""
    transport = StdioTransport(
        command="uv",
        args=["run", str(test_server_path)],
    )
    async with Client(transport=transport) as client:
        fs = MCPToolsFileSystem(client=client)

        files = await fs._ls("/", detail=True)

        # Find the greet tool info
        greet_info = next((f for f in files if f.get("tool_name") == "greet"), None)
        assert greet_info is not None
        assert greet_info["type"] == "file"
        assert greet_info["name"] == "/greet.py"
        assert "greeting" in greet_info.get("description", "").lower()


async def test_read_client_file(test_server_path: Path):
    """Test reading the _client.py file."""
    transport = StdioTransport(
        command="uv",
        args=["run", str(test_server_path)],
    )
    async with Client(transport=transport) as client:
        fs = MCPToolsFileSystem(client=client)

        content = await fs._cat_file("/_client.py")
        code = content.decode("utf-8")

        assert "from fastmcp import Client" in code
        assert "async def call_mcp_tool" in code
        assert "def get_client" in code


async def test_read_tool_file(test_server_path: Path):
    """Test reading a generated tool file."""
    transport = StdioTransport(
        command="uv",
        args=["run", str(test_server_path)],
    )
    async with Client(transport=transport) as client:
        fs = MCPToolsFileSystem(client=client)

        content = await fs._cat_file("/greet.py")
        code = content.decode("utf-8")

        # Check function signature
        assert "async def greet(" in code
        assert "name: str" in code
        # Optional param should have None default
        assert "greeting:" in code

        # Check docstring
        assert '"""' in code

        # Check implementation calls call_mcp_tool
        assert 'call_mcp_tool("greet"' in code


async def test_read_tool_with_multiple_params(test_server_path: Path):
    """Test reading a tool with multiple parameters."""
    transport = StdioTransport(
        command="uv",
        args=["run", str(test_server_path)],
    )
    async with Client(transport=transport) as client:
        fs = MCPToolsFileSystem(client=client)

        content = await fs._cat_file("/search_items.py")
        code = content.decode("utf-8")

        # Check all parameters are present
        assert "query: str" in code
        assert "limit:" in code
        assert "include_archived:" in code


async def test_stubs_only_mode(test_server_path: Path):
    """Test stubs_only mode generates stubs without implementation."""
    transport = StdioTransport(
        command="uv",
        args=["run", str(test_server_path)],
    )
    async with Client(transport=transport) as client:
        fs = MCPToolsFileSystem(client=client, stubs_only=True)

        content = await fs._cat_file("/greet.py")
        code = content.decode("utf-8")

        # Should have function signature
        assert "async def greet(" in code

        # Should end with ... instead of implementation
        assert "..." in code

        # Should NOT have call_mcp_tool
        assert "call_mcp_tool" not in code


async def test_file_info(test_server_path: Path):
    """Test getting file info."""
    transport = StdioTransport(
        command="uv",
        args=["run", str(test_server_path)],
    )
    async with Client(transport=transport) as client:
        fs = MCPToolsFileSystem(client=client)

        info = await fs._info("/greet.py")

        assert info["type"] == "file"
        assert info["tool_name"] == "greet"
        assert info["description"] is not None


async def test_root_is_directory(test_server_path: Path):
    """Test that root path is a directory."""
    transport = StdioTransport(
        command="uv",
        args=["run", str(test_server_path)],
    )
    async with Client(transport=transport) as client:
        fs = MCPToolsFileSystem(client=client)

        assert await fs._isdir("/")
        assert not await fs._isfile("/")


async def test_tool_file_exists(test_server_path: Path):
    """Test file existence checks."""
    transport = StdioTransport(
        command="uv",
        args=["run", str(test_server_path)],
    )
    async with Client(transport=transport) as client:
        fs = MCPToolsFileSystem(client=client)

        assert await fs._exists("/greet.py")
        assert await fs._exists("/_client.py")
        assert not await fs._exists("/nonexistent.py")


async def test_file_not_found(test_server_path: Path):
    """Test FileNotFoundError for missing files."""
    transport = StdioTransport(
        command="uv",
        args=["run", str(test_server_path)],
    )
    async with Client(transport=transport) as client:
        fs = MCPToolsFileSystem(client=client)

        with pytest.raises(FileNotFoundError):
            await fs._cat_file("/nonexistent.py")


async def test_read_only_operations(test_server_path: Path):
    """Test that write operations raise NotImplementedError."""
    transport = StdioTransport(
        command="uv",
        args=["run", str(test_server_path)],
    )
    async with Client(transport=transport) as client:
        fs = MCPToolsFileSystem(client=client)

        with pytest.raises(NotImplementedError):
            await fs._mkdir("/test")

        with pytest.raises(NotImplementedError):
            await fs._rmdir("/")

        with pytest.raises(NotImplementedError):
            await fs._touch("/test.py")


async def test_generated_code_is_valid_python(test_server_path: Path):
    """Test that generated code compiles as valid Python."""
    transport = StdioTransport(
        command="uv",
        args=["run", str(test_server_path)],
    )
    async with Client(transport=transport) as client:
        fs = MCPToolsFileSystem(client=client)

        files = await fs._ls("/", detail=False)

        for filename in files:
            content = await fs._cat_file(filename)
            code = content.decode("utf-8")

            # This will raise SyntaxError if code is invalid
            compile(code, filename, "exec")


if __name__ == "__main__":
    pytest.main(["-v", __file__])
