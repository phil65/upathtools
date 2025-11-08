"""Example usage of MCP filesystem for exposing MCP resources through fsspec."""

from pathlib import Path

from fastmcp.client import StdioTransport
import pytest

from upathtools.filesystems.mcp_fs import MCPFileSystem


async def test_mcp_fs():
    """Demonstrate MCP filesystem usage."""
    from fastmcp import Client

    # Example MCP server config (this would come from your actual config)
    path = Path(__file__).parent / "test_mcp_server.py"
    transport = StdioTransport(
        command="uv",
        args=["run", str(path)],
    )
    async with Client(transport=transport) as mcp_client:
        # Create MCP filesystem
        fs = MCPFileSystem(mcp_client)

        # List all available resources
        print("=== Available MCP Resources ===")
        resources = await fs._ls("/", detail=True)
        for resource in resources:
            print(f"- {resource['name']}")
            print(f"  URI: {resource['uri']}")
            print(f"  Type: {resource.get('mimeType', 'unknown')}")
            if resource.get("description"):
                print(f"  Description: {resource['description']}")
            print()

        if resources:
            first_resource = resources[0]
            assert await fs._cat(first_resource["name"])
            resource_path = resources[0]["name"]
            resource_path = resources[0]["name"]
            info = fs.info(resource_path)
            for key, value in info.items():
                print(f"  {key}: {value}")


if __name__ == "__main__":
    pytest.main(["-v", __file__])
