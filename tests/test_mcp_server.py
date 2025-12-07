"""Compact FastMCP server demonstrating sampling and elicitation in one workflow."""

from __future__ import annotations

from fastmcp import FastMCP


mcp = FastMCP("Test Server")


@mcp.resource("resource://greeting")
def get_greeting() -> str:
    """Provides a simple greeting message."""
    return "Hello from FastMCP Resources!"


if __name__ == "__main__":
    mcp.run(show_banner=False, log_level="error")
