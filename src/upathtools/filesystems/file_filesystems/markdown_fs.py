"""Filesystem implementation for browsing markdown documents by header hierarchy."""

from __future__ import annotations

import io
import re
import sys
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Required, overload

import fsspec

from upathtools.filesystems.base import BaseFileFileSystem, BaseUPath, FileInfo, ProbeResult


if TYPE_CHECKING:
    from collections.abc import Sequence


class MarkdownInfo(FileInfo, total=False):
    """Info dict for Markdown filesystem paths."""

    size: Required[int]
    level: Required[int]


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


class MarkdownPath(BaseUPath[MarkdownInfo]):
    """UPath implementation for browsing markdown documents."""

    __slots__ = ()

    def iterdir(self):
        if not self.is_dir():
            raise NotADirectoryError(str(self))
        yield from super().iterdir()


class MarkdownFileSystem(BaseFileFileSystem[MarkdownPath, MarkdownInfo]):
    """Filesystem for browsing markdown documents by header hierarchy."""

    protocol = "md"
    upath_cls = MarkdownPath
    supported_extensions: ClassVar[frozenset[str]] = frozenset({"md", "markdown", "mdown", "mkd"})
    priority: ClassVar[int] = 50

    @classmethod
    def probe_content(cls, content: bytes, extension: str = "") -> ProbeResult:
        """Probe content to check if it's a markdown document.

        Looks for markdown indicators like headers (#) or common markdown syntax.
        """
        if not cls.supports_extension(extension):
            return ProbeResult.UNSUPPORTED

        try:
            text = content.decode("utf-8")
            # Check for markdown headers
            header_pattern = re.compile(r"^#{1,6}\s+.+$", re.MULTILINE)
            if header_pattern.search(text):
                return ProbeResult.SUPPORTED

            # Check for other common markdown patterns
            markdown_patterns = [
                r"^\s*[-*+]\s+",  # Unordered lists
                r"^\s*\d+\.\s+",  # Ordered lists
                r"\[.+\]\(.+\)",  # Links
                r"```",  # Code blocks
                r"^\s*>\s+",  # Blockquotes
            ]
            for pattern in markdown_patterns:
                if re.search(pattern, text, re.MULTILINE):
                    return ProbeResult.MAYBE

            # Extension matches but no clear markdown indicators
        except Exception:  # noqa: BLE001
            return ProbeResult.UNSUPPORTED
        else:
            return ProbeResult.MAYBE

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

    @classmethod
    def from_content(
        cls,
        content: bytes,
        **kwargs: Any,
    ) -> MarkdownFileSystem:
        """Create filesystem instance from raw markdown content.

        Args:
            content: Raw markdown content as bytes.
            **kwargs: Additional filesystem options.

        Returns:
            Configured filesystem instance with pre-loaded content.
        """
        fs = cls(fo="<content>", **kwargs)
        fs._content = content.decode("utf-8")
        fs._parse_content()
        return fs

    @classmethod
    def from_file(
        cls,
        path: str,
        target_protocol: str | None = None,
        target_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> MarkdownFileSystem:
        """Create filesystem instance from a markdown file path."""
        return cls(
            fo=path, target_protocol=target_protocol, target_options=target_options, **kwargs
        )

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, Any]:
        path = path.removeprefix("md://")
        return {"fo": path}

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
        parts = self._strip_protocol(path).strip("/").split("/")  # pyright: ignore[reportAttributeAccessIssue]

        for part in parts:
            if part not in current.children:
                msg = f"Section not found: {path}"
                raise FileNotFoundError(msg)
            current = current.children[part]

        return current

    @overload
    def ls(self, path: str, detail: Literal[True] = ..., **kwargs: Any) -> list[MarkdownInfo]: ...

    @overload
    def ls(self, path: str, detail: Literal[False], **kwargs: Any) -> list[str]: ...

    def ls(self, path: str, detail: bool = True, **kwargs: Any) -> Sequence[str | MarkdownInfo]:
        """List contents of a path."""
        node = self._get_node(path)

        if not detail:
            return list(node.children)

        return [
            MarkdownInfo(
                name=name,
                size=child.get_size(),
                type="directory" if child.is_dir() else "file",
                level=child.level,
            )
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

    def info(self, path: str, **kwargs: Any) -> MarkdownInfo:
        """Get info about a path."""
        node = self._get_node(path)
        name = "root" if not path or path == "/" else path.split("/")[-1]

        return MarkdownInfo(
            name=name,
            size=node.get_size(),
            type="directory" if node.is_dir() else "file",
            level=node.level,
        )

    def isdir(self, path: str) -> bool:
        """Check if path is a directory (has children)."""
        try:
            node = self._get_node(path)
            return node.is_dir()
        except FileNotFoundError:
            return False

    def _open(self, path: str, mode: str = "rb", **kwargs: Any) -> Any:
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
        fs = MarkdownFileSystem(f.name)

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
