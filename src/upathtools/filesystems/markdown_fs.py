"""Filesystem implementation for browsing markdown documents by header hierarchy."""

from __future__ import annotations

import io
import re
import sys
from typing import TYPE_CHECKING, Any, Literal, overload

import fsspec
from fsspec.spec import AbstractFileSystem
from upath import UPath


if TYPE_CHECKING:
    from collections.abc import Sequence


class MarkdownNode:
    """Represents a markdown header and its content."""

    def __init__(
        self,
        title: str,
        level: int,
        content: str = "",
        children: dict[str, MarkdownNode] | None = None,
    ) -> None:
        """Initialize a markdown node.

        Args:
            title: Header text
            level: Header level (number of #)
            content: Content belonging to this header
            children: Child nodes (sub-headers)
        """
        self.title = title
        self.level = level
        self.content = content
        self.children = children or {}

    def is_dir(self) -> bool:
        """Check if node should be treated as directory."""
        return bool(self.children)

    def get_size(self) -> int:
        """Get size of node's content."""
        return len(self.content.encode())


class MarkdownPath(UPath):
    """UPath implementation for browsing markdown documents."""

    __slots__ = ()

    def iterdir(self):
        if not self.is_dir():
            raise NotADirectoryError(str(self))
        yield from super().iterdir()


class MarkdownFS(AbstractFileSystem):
    """Filesystem for browsing markdown documents by header hierarchy."""

    protocol = "md"

    def __init__(
        self,
        fo: str = "",
        target_protocol: str | None = None,
        target_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the filesystem.

        Args:
            fo: Path to markdown file
            target_protocol: Protocol for source file
            target_options: Options for target protocol
            **kwargs: Additional filesystem options
        """
        super().__init__(**kwargs)
        self.path = fo
        self.target_protocol = target_protocol
        self.target_options = target_options or {}
        self._content: str | None = None
        self._root: MarkdownNode | None = None

    def _make_path(self, path: str) -> UPath:
        """Create a path object from string."""
        return MarkdownPath(path)

    def _load(self) -> None:
        """Load and parse the markdown file if not already loaded."""
        if self._content is not None:
            return

        with fsspec.open(
            self.path,
            mode="r",
            protocol=self.target_protocol,
            **self.target_options,
        ) as f:
            self._content = f.read()  # pyright: ignore

        self._parse_content()

    def _parse_content(self) -> None:
        """Parse markdown content into node hierarchy."""
        if not self._content:
            self._root = MarkdownNode("root", 0)
            return

        lines = self._content.splitlines()
        header_pattern = re.compile(r"^(#+)\s+(.+)$")

        # First pass: find minimum header level
        min_level = sys.maxsize
        for line in lines:
            if match := header_pattern.match(line):
                level = len(match.group(1))
                min_level = min(level, min_level)

        # If no headers found, or only higher levels, default to 1
        min_level = min(min_level, 1)

        current_path: list[MarkdownNode] = []
        current_content: list[str] = []

        # Create root node
        self._root = MarkdownNode("root", 0)
        current_path.append(self._root)

        for line in lines:
            if match := header_pattern.match(line):
                # Process accumulated content
                if current_content and current_path:
                    current_path[-1].content = "\n".join(current_content)
                current_content = []

                # Process header
                actual_level = len(match.group(1))
                # Adjust level relative to minimum found
                level = actual_level - min_level + 1
                title = match.group(2).strip()
                node = MarkdownNode(title, level)

                # Find appropriate parent
                while current_path and current_path[-1].level >= level:
                    current_path.pop()

                # Skip if we lost the path (header level too deep)
                if not current_path:
                    continue

                # Add to parent's children
                current_path[-1].children[title] = node
                current_path.append(node)
            else:
                current_content.append(line)

        # Handle remaining content
        if current_content and current_path:
            current_path[-1].content = "\n".join(current_content)

    def _get_node(self, path: str) -> MarkdownNode:
        """Get node at path.

        Args:
            path: Path to node

        Returns:
            MarkdownNode at path

        Raises:
            FileNotFoundError: If path doesn't exist
        """
        self._load()
        assert self._root is not None

        if not path or path == "/":
            return self._root

        current = self._root
        parts = self._strip_protocol(path).strip("/").split("/")  # type: ignore

        for part in parts:
            if part not in current.children:
                msg = f"Section not found: {path}"
                raise FileNotFoundError(msg)
            current = current.children[part]

        return current

    @overload
    def ls(
        self,
        path: str = "",
        detail: Literal[True] = True,
        **kwargs: Any,
    ) -> list[dict[str, Any]]: ...

    @overload
    def ls(
        self,
        path: str = "",
        detail: Literal[False] = False,
        **kwargs: Any,
    ) -> list[str]: ...

    def ls(
        self,
        path: str = "",
        detail: bool = True,
        **kwargs: Any,
    ) -> Sequence[str | dict[str, Any]]:
        """List contents of a path."""
        node = self._get_node(path)

        if not detail:
            return list(node.children)

        return [
            {
                "name": name,
                "size": child.get_size(),
                "type": "directory" if child.is_dir() else "file",
                "level": child.level,
            }
            for name, child in node.children.items()
        ]

    def cat(self, path: str) -> bytes:
        """Get section content including header and all content up to next header."""
        node = self._get_node(path)

        # Skip header generation for root
        if node is self._root:
            return node.content.encode()

        # Build section content
        lines = []
        # Add header
        lines.append(f"{'#' * node.level} {node.title}")

        # Add content if any
        if node.content:
            lines.append(node.content)

        return "\n".join(lines).encode()

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Get info about a path."""
        node = self._get_node(path)
        name = (
            "root" if not path or path == "/" else path.split("/")[-1]  # type: ignore
        )

        return {
            "name": name,
            "size": node.get_size(),
            "type": "directory" if node.is_dir() else "file",
            "level": node.level,
        }

    def _open(
        self,
        path: str,
        mode: str = "rb",
        **kwargs: Any,
    ) -> Any:
        """Provide file-like access to section content."""
        if "w" in mode or "a" in mode:
            msg = "Write mode not supported"
            raise NotImplementedError(msg)

        node = self._get_node(path)
        content = node.content.encode()
        return io.BytesIO(content)


if __name__ == "__main__":
    # Example markdown content
    EXAMPLE = """\
# Section 1
Content 1
### Skipped Section
Skipped content
## Section 1.1
Content 1.1
## Section 1.2
Content 1.2
# Section 2
Content 2
## Section 2.1
Content 2.1
"""

    import tempfile

    # Create temporary file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".md", delete=False) as f:
        f.write(EXAMPLE)
        f.flush()

        # Create filesystem
        fs = MarkdownFS(f.name)

        # List root
        print("\nRoot sections:")
        for item in fs.ls("/", detail=True):
            print(f"- {item['name']} ({item['type']})")

        # List subsections
        print("\nSubsections of Section 1:")
        for item in fs.ls("Section 1", detail=True):
            print(f"- {item['name']} ({item['type']})")

        # Read content
        print("\nContent of Section 1.1:")
        print(fs.cat("Section 1/Section 1.1").decode())
