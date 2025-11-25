"""Test MCP server with sample tools for MCPToolsFileSystem testing."""

from __future__ import annotations

from fastmcp import FastMCP


mcp = FastMCP("Test Tools Server")


@mcp.tool
async def greet(name: str, greeting: str = "Hello") -> str:
    """Generate a personalized greeting message.

    Args:
        name: The name of the person to greet
        greeting: The greeting word to use (default: Hello)

    Returns:
        A formatted greeting message
    """
    return f"{greeting}, {name}!"


@mcp.tool
async def add_numbers(a: int, b: int) -> int:
    """Add two numbers together.

    Args:
        a: First number
        b: Second number

    Returns:
        Sum of the two numbers
    """
    return a + b


@mcp.tool
async def search_items(query: str, limit: int = 10, include_archived: bool = False) -> list[dict]:
    """Search for items matching a query.

    Args:
        query: Search query string
        limit: Maximum number of results to return
        include_archived: Whether to include archived items

    Returns:
        List of matching items
    """
    return [
        {"id": 1, "name": f"Result for '{query}'", "archived": include_archived},
    ][:limit]


if __name__ == "__main__":
    mcp.run(show_banner=False, log_level="error")
