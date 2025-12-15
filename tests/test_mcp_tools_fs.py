"""Tests for MCPToolsFileSystem."""

from __future__ import annotations

from pathlib import Path

import pytest

from upathtools.filesystems.remote_filesystems.mcp_tools_fs import MCPToolsFileSystem


TEST_SERVER_PATH = Path(__file__).parent / "mcp_tools_server.py"


@pytest.fixture(scope="module")
def fs():
    """Create filesystem using server_cmd - connects on demand."""
    filesystem = MCPToolsFileSystem(server_cmd=["uv", "run", str(TEST_SERVER_PATH)])
    yield filesystem
    filesystem.close()


@pytest.fixture(scope="module")
def fs_stubs():
    """Create filesystem with stubs_only=True."""
    filesystem = MCPToolsFileSystem(
        server_cmd=["uv", "run", str(TEST_SERVER_PATH)],
        stubs_only=True,
    )
    yield filesystem
    filesystem.close()


async def test_list_tools(fs: MCPToolsFileSystem):
    """Test listing available tools as files."""
    files = await fs._ls("", detail=False)

    assert "_client.py" in files
    assert "greet.py" in files
    assert "add_numbers.py" in files
    assert "search_items.py" in files


async def test_list_tools_with_detail(fs: MCPToolsFileSystem):
    """Test listing tools with detailed info."""
    files = await fs._ls("", detail=True)

    greet_info = next((f for f in files if f.get("tool_name") == "greet"), None)
    assert greet_info is not None
    assert greet_info["type"] == "file"
    assert greet_info["name"] == "greet.py"
    assert "greeting" in greet_info.get("description", "").lower()


async def test_read_client_file(fs: MCPToolsFileSystem):
    """Test reading the _client.py file."""
    content = await fs._cat_file("_client.py")
    code = content.decode("utf-8")

    assert "from fastmcp import Client" in code
    assert "async def call_mcp_tool" in code
    assert "def get_client" in code


async def test_read_tool_file(fs: MCPToolsFileSystem):
    """Test reading a generated tool file."""
    content = await fs._cat_file("greet.py")
    code = content.decode("utf-8")

    assert "async def greet(" in code
    assert "name: str" in code
    assert "greeting:" in code
    assert '"""' in code
    assert 'call_mcp_tool("greet"' in code


async def test_read_tool_with_multiple_params(fs: MCPToolsFileSystem):
    """Test reading a tool with multiple parameters."""
    content = await fs._cat_file("search_items.py")
    code = content.decode("utf-8")

    assert "query: str" in code
    assert "limit:" in code
    assert "include_archived:" in code


async def test_stubs_only_mode(fs_stubs: MCPToolsFileSystem):
    """Test stubs_only mode generates stubs without implementation."""
    content = await fs_stubs._cat_file("greet.py")
    code = content.decode("utf-8")

    assert "async def greet(" in code
    assert "..." in code
    assert "call_mcp_tool" not in code


async def test_file_info(fs: MCPToolsFileSystem):
    """Test getting file info."""
    info = await fs._info("greet.py")

    assert info["type"] == "file"
    assert info["tool_name"] == "greet"
    assert info["description"] is not None


async def test_root_is_directory(fs: MCPToolsFileSystem):
    """Test that root path is a directory."""
    assert await fs._isdir("")
    assert not await fs._isfile("")


async def test_tool_file_exists(fs: MCPToolsFileSystem):
    """Test file existence checks."""
    assert await fs._exists("greet.py")
    assert await fs._exists("_client.py")
    assert not await fs._exists("nonexistent.py")


async def test_file_not_found(fs: MCPToolsFileSystem):
    """Test FileNotFoundError for missing files."""
    with pytest.raises(FileNotFoundError):
        await fs._cat_file("nonexistent.py")


async def test_read_only_operations(fs: MCPToolsFileSystem):
    """Test that write operations raise NotImplementedError."""
    with pytest.raises(NotImplementedError):
        await fs._mkdir("test")

    with pytest.raises(NotImplementedError):
        await fs._rmdir("")

    with pytest.raises(NotImplementedError):
        await fs._touch("test.py")


async def test_generated_code_is_valid_python(fs: MCPToolsFileSystem):
    """Test that generated code compiles as valid Python."""
    files = await fs._ls("", detail=False)

    for filename in files:
        content = await fs._cat_file(filename)
        code = content.decode("utf-8")
        compile(code, filename, "exec")
