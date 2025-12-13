"""Filesystem implementation for browsing code structure using tree-sitter."""

from __future__ import annotations

import io
import os
from typing import TYPE_CHECKING, Any, ClassVar, Literal, overload

import fsspec

from upathtools.filesystems.base import BaseFileFileSystem, BaseUPath, FileInfo, ProbeResult


LANGUAGE_MAP = {
    # Python
    ".py": "python",
    ".pyi": "python",
    # JavaScript/TypeScript
    ".js": "javascript",
    ".mjs": "javascript",
    ".cjs": "javascript",
    ".jsx": "javascript",
    ".ts": "typescript",
    ".tsx": "typescript",
    ".mts": "typescript",
    ".cts": "typescript",
    # C/C++
    ".c": "c",
    ".h": "c",
    ".cpp": "cpp",
    ".cc": "cpp",
    ".cxx": "cpp",
    ".hpp": "cpp",
    ".hxx": "cpp",
    # Rust
    ".rs": "rust",
    # Go
    ".go": "go",
    # Java
    ".java": "java",
    # Config/Data formats
    ".json": "json",
    ".yaml": "yaml",
    ".yml": "yaml",
    ".toml": "toml",
    # Shell/Ops
    ".sh": "bash",
    ".bash": "bash",
    # Other languages
    ".rb": "ruby",
    ".php": "php",
    ".cs": "c_sharp",
    ".sql": "sql",
}

if TYPE_CHECKING:
    from collections.abc import Sequence


class TreeSitterInfo(FileInfo, total=False):
    """Info dict for tree-sitter filesystem paths."""

    node_type: str
    size: int
    start_byte: int
    end_byte: int
    start_line: int
    end_line: int
    doc: str | None


class CodeNode:
    """Represents a named code entity (function, class, variable, etc.)."""

    def __init__(
        self,
        name: str,
        node_type: str,
        start_byte: int,
        end_byte: int,
        start_line: int = 0,
        end_line: int = 0,
        children: dict[str, CodeNode] | None = None,
        doc: str | None = None,
    ) -> None:
        """Initialize a code node.

        Args:
            name: The identifier/name from source code
            node_type: Tree-sitter node type
            start_byte: Start position in source
            end_byte: End position in source
            start_line: Start line number (1-based)
            end_line: End line number (1-based)
            children: Child nodes (methods, nested classes, etc.)
            doc: Associated docstring if any
        """
        self.name = name
        self.node_type = node_type
        self.start_byte = start_byte
        self.end_byte = end_byte
        self.start_line = start_line
        self.end_line = end_line
        self.children = children or {}
        self.doc = doc

    def is_dir(self) -> bool:
        """Check if node should be treated as directory."""
        return bool(self.children)

    def get_size(self) -> int:
        """Get size of node's source code."""
        return self.end_byte - self.start_byte


class TreeSitterPath(BaseUPath[TreeSitterInfo]):
    """UPath implementation for browsing code with tree-sitter."""

    __slots__ = ()

    def iterdir(self):
        if not self.is_dir():
            raise NotADirectoryError(str(self))
        yield from super().iterdir()


class TreeSitterFileSystem(BaseFileFileSystem[TreeSitterPath, TreeSitterInfo]):
    """Browse source code structure using tree-sitter."""

    protocol = "ts"
    upath_cls = TreeSitterPath
    supported_extensions: ClassVar[frozenset[str]] = frozenset({
        # Python
        "py",
        "pyi",
        # JavaScript/TypeScript
        "js",
        "mjs",
        "cjs",
        "jsx",
        "ts",
        "tsx",
        "mts",
        "cts",
        # C/C++
        "c",
        "h",
        "cpp",
        "cc",
        "cxx",
        "hpp",
        "hxx",
        # Rust
        "rs",
        # Go
        "go",
        # Java
        "java",
        # Config/Data
        "json",
        "yaml",
        "yml",
        "toml",
        # Shell/Ops
        "sh",
        "bash",
        # Other
        "rb",
        "php",
        "cs",
        "sql",
    })
    priority: ClassVar[int] = 70

    @classmethod
    def probe_content(cls, content: bytes, extension: str = "") -> ProbeResult:
        """Probe content to check if tree-sitter can parse it.

        For supported extensions, assumes content is valid source code.
        """
        if not cls.supports_extension(extension):
            return ProbeResult.UNSUPPORTED

        # Tree-sitter is lenient and can parse most source files
        # Check that content is valid UTF-8 text
        try:
            content.decode("utf-8")
        except UnicodeDecodeError:
            return ProbeResult.UNSUPPORTED
        else:
            return ProbeResult.SUPPORTED

    @classmethod
    def from_content(
        cls,
        content: bytes,
        language: str = "python",
        **kwargs: Any,
    ) -> TreeSitterFileSystem:
        """Create filesystem instance from raw source code content.

        Args:
            content: Raw source code as bytes.
            language: Programming language (e.g., 'python', 'javascript').
            **kwargs: Additional filesystem options.

        Returns:
            Configured filesystem instance with pre-loaded content.
        """
        fs = cls(source_file="<content>", language=language, **kwargs)
        fs._source = content.decode("utf-8")
        fs._parse_source()
        return fs

    @classmethod
    def from_file(
        cls,
        path: str,
        target_protocol: str | None = None,
        target_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> TreeSitterFileSystem:
        """Create filesystem instance from a source file path."""
        return cls(
            source_file=path,
            target_protocol=target_protocol,
            target_options=target_options,
            **kwargs,
        )

    def __init__(
        self,
        source_file: str = "",
        language: str | None = None,
        target_protocol: str | None = None,
        target_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        # Handle both direct usage and chaining
        fo = kwargs.pop("fo", "")
        path = source_file or fo

        if not path:
            msg = "Source file path required"
            raise ValueError(msg)

        self.path = path
        self.target_protocol = target_protocol
        self.target_options = target_options or {}

        # Determine language
        if language:
            self.language = language
        else:
            ext = os.path.splitext(path)[1].lower()  # noqa: PTH122
            self.language = LANGUAGE_MAP.get(ext, "python")

        # Initialize state
        self._source: str | None = None
        self._root: CodeNode | None = None
        self._parser: Any = None
        self._tree: Any = None

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, Any]:
        path = path.removeprefix("ts://")
        return {"source_file": path}

    def _load(self) -> None:
        """Load and parse the source file if not already loaded."""
        if self._source is not None:
            return

        with fsspec.open(
            self.path,
            mode="r",
            protocol=self.target_protocol,
            **self.target_options,
        ) as f:
            self._source = f.read()  # type: ignore

        self._parse_source()

    def _parse_source(self) -> None:
        """Parse source code using tree-sitter."""
        if not self._source:
            self._root = CodeNode("root", "module", 0, 0)
            return

        from tree_sitter import Language, Parser

        # Import the specific language
        language_module = _import_language_module(self.language)
        language = Language(language_module.language())
        # Create parser
        self._parser = Parser(language)  # type: ignore
        self._tree = self._parser.parse(self._source.encode())  # type: ignore
        # Build node hierarchy
        source_bytes = self._source.encode()
        total_lines = len(self._source.splitlines())
        self._root = CodeNode("root", "module", 0, len(source_bytes), 1, total_lines)
        self._extract_nodes(self._tree.root_node, self._root)  # type: ignore

    def _extract_nodes(self, ts_node, parent_node: CodeNode) -> None:
        """Extract named entities from tree-sitter node."""
        # Start with generic extraction that works for all languages
        self._extract_generic_nodes(ts_node, parent_node)

        # Apply language-specific enhancements
        if self.language == "python":
            self._apply_python_enhancements(ts_node, parent_node)
        elif self.language in ("javascript", "typescript"):
            self._apply_js_enhancements(ts_node, parent_node)

    def _extract_generic_nodes(self, ts_node, parent_node: CodeNode) -> None:
        """Generic extraction that works for all languages."""
        imports_node: CodeNode | None = None

        for child in ts_node.children:
            if not child.is_named:
                continue

            node_type = child.type
            # Skip noise nodes
            if node_type in ("comment", "string", "number", "boolean", "null"):
                continue
            # Handle imports generically
            if "import" in node_type.lower():
                if imports_node is None:
                    imports_node = CodeNode("imports", "imports_group", 0, 0)
                    parent_node.children["imports"] = imports_node

                import_text = self._get_node_text(child).strip()
                start_line = child.start_point[0] + 1
                end_line = child.end_point[0] + 1 if child.end_point else start_line
                import_node = CodeNode(
                    import_text,
                    node_type,
                    child.start_byte,
                    child.end_byte,
                    start_line,
                    end_line,
                )
                imports_node.children[import_text] = import_node
                continue

            # Look for nodes that have identifiers (potential named entities)
            name = self._find_identifier_name(child)
            if name:
                start_line = child.start_point[0] + 1  # Convert to 1-based
                end_line = child.end_point[0] + 1 if child.end_point else start_line
                entity_node = CodeNode(
                    name,
                    node_type,
                    child.start_byte,
                    child.end_byte,
                    start_line,
                    end_line,
                )

                # Check if this node might have children (functions, classes, etc.)
                if self._node_might_have_children(child):
                    parent_node.children[name] = entity_node
                    self._extract_nodes(child, entity_node)
                else:
                    parent_node.children[name] = entity_node
            else:
                # No identifier found, recurse into children
                self._extract_nodes(child, parent_node)

    def _find_identifier_name(self, node) -> str | None:
        """Find the identifier/name for a node using common patterns."""
        # Try common field names for identifiers
        for field_name in ("name", "identifier", "id", "key"):
            name_node = node.child_by_field_name(field_name)
            if name_node and name_node.type in ("identifier", "name"):
                return self._get_node_text(name_node)

        # Look for first identifier child
        for child in node.children:
            if child.type in ("identifier", "name") and child.is_named:
                return self._get_node_text(child)

        return None

    def _node_might_have_children(self, node) -> bool:
        """Check if a node type typically contains other named entities.

        Functions/methods are treated as leaf nodes (files, not directories).
        Only structural containers like classes and modules have children.
        """
        node_type = node.type.lower()
        container_patterns = [
            "class",
            "struct",
            "interface",
            "module",
            "namespace",
        ]
        return any(pattern in node_type for pattern in container_patterns)

    def _apply_python_enhancements(self, ts_node, parent_node: CodeNode) -> None:
        """Apply Python-specific enhancements to generic extraction."""
        for child in ts_node.children:
            node_type = child.type
            name_node = child.child_by_field_name("name")

            if not name_node:
                continue

            name = self._get_node_text(name_node)

            # Enhanced docstring extraction for Python
            if (
                node_type
                in (
                    "function_definition",
                    "async_function_definition",
                    "class_definition",
                )
                and name in parent_node.children
            ):
                doc = self._extract_python_docstring(child)
                parent_node.children[name].doc = doc

            # Continue recursively
            if name in parent_node.children and parent_node.children[name].children:
                self._apply_python_enhancements(child, parent_node.children[name])

    def _apply_js_enhancements(self, ts_node, parent_node: CodeNode) -> None:
        """Apply JavaScript/TypeScript-specific enhancements."""
        for child in ts_node.children:
            node_type = child.type

            # Enhanced handling for JS/TS specific patterns
            if node_type == "variable_declaration":
                # Better variable extraction for JS/TS
                for declarator in child.children:
                    if declarator.type == "variable_declarator":
                        name_node = declarator.child_by_field_name("name")
                        if name_node:
                            name = self._get_node_text(name_node)
                            if name in parent_node.children:
                                # Update with more specific info
                                parent_node.children[name].node_type = "variable_declaration"

            # Continue recursively for nested structures
            child_name = self._find_identifier_name(child)
            if (
                child_name
                and child_name in parent_node.children
                and parent_node.children[child_name].children
            ):
                self._apply_js_enhancements(child, parent_node.children[child_name])

    def _extract_python_docstring(self, node) -> str | None:
        """Extract docstring from Python function/class."""
        body = node.child_by_field_name("body")
        if body and body.children:
            first_stmt = body.children[0]
            if first_stmt.type == "expression_statement" and first_stmt.children:
                expr = first_stmt.children[0]
                if expr.type == "string":
                    docstring = self._get_node_text(expr)
                    # Remove quotes and clean up
                    return docstring.strip("\"'").strip()
        return None

    def _get_node_text(self, node) -> str:
        """Get text content of a tree-sitter node."""
        if not self._source:
            return ""
        return self._source[node.start_byte : node.end_byte]

    def _get_node(self, path: str) -> CodeNode:
        """Get code node at path."""
        self._load()
        assert self._root is not None

        if not path or path == "/":
            return self._root

        current = self._root
        parts = self._strip_protocol(path).strip("/").split("/")  # pyright: ignore[reportAttributeAccessIssue]

        for part in parts:
            if part not in current.children:
                msg = f"Entity not found: {path}"
                raise FileNotFoundError(msg)
            current = current.children[part]

        return current

    @overload
    def ls(self, path: str, detail: Literal[True] = ..., **kwargs: Any) -> list[TreeSitterInfo]: ...

    @overload
    def ls(self, path: str, detail: Literal[False], **kwargs: Any) -> list[str]: ...

    def ls(self, path: str, detail: bool = True, **kwargs: Any) -> Sequence[str | TreeSitterInfo]:
        """List code entities at path."""
        node = self._get_node(path)

        if not detail:
            return list(node.children)

        return [
            TreeSitterInfo(
                name=name,
                size=child.get_size(),
                type="directory" if child.is_dir() else "file",
                node_type=child.node_type,
                start_byte=child.start_byte,
                end_byte=child.end_byte,
                start_line=child.start_line,
                end_line=child.end_line,
                doc=child.doc,
            )
            for name, child in node.children.items()
        ]

    def cat(self, path: str) -> bytes:
        """Get source code of entity."""
        self._load()
        assert self._source is not None

        node = self._get_node(path)

        # Return source text for the entity's byte range
        source_bytes = self._source.encode()
        return source_bytes[node.start_byte : node.end_byte]

    def isdir(self, path: str) -> bool:
        """Check if path is a directory (has children)."""
        try:
            node = self._get_node(path)
            return node.is_dir()
        except FileNotFoundError:
            return False

    def info(self, path: str, **kwargs: Any) -> TreeSitterInfo:
        """Get info about a code entity."""
        node = self._get_node(path)
        name = "root" if not path or path == "/" else path.split("/")[-1]

        return TreeSitterInfo(
            name=name,
            size=node.get_size(),
            type="directory" if node.is_dir() else "file",
            node_type=node.node_type,
            start_byte=node.start_byte,
            end_byte=node.end_byte,
            start_line=node.start_line,
            end_line=node.end_line,
            doc=node.doc,
        )

    def _open(self, path: str, mode: str = "rb", **kwargs: Any) -> Any:
        """Provide file-like access to entity source code."""
        if "w" in mode or "a" in mode:
            msg = "Write mode not supported"
            raise NotImplementedError(msg)

        content = self.cat(path)
        return io.BytesIO(content)


def _import_language_module(language: Any):
    """Import the appropriate tree-sitter language module."""
    language_modules = {
        "python": "tree_sitter_python",
        "javascript": "tree_sitter_javascript",
        "typescript": "tree_sitter_typescript",
        "c": "tree_sitter_c",
        "cpp": "tree_sitter_cpp",
        "java": "tree_sitter_java",
        "rust": "tree_sitter_rust",
        "go": "tree_sitter_go",
        "ruby": "tree_sitter_ruby",
        "php": "tree_sitter_php",
    }

    module_name = language_modules.get(language)
    if not module_name:
        msg = f"Language {language} not supported"
        raise ValueError(msg)

    try:
        return __import__(module_name)
    except ImportError as e:
        msg = f"{module_name} not installed. Install with: pip install {module_name}"
        raise ImportError(msg) from e


if __name__ == "__main__":
    try:
        fs = TreeSitterFileSystem(__file__, language="python")

        print("Code entities:")
        for item in fs.ls("/", detail=True):
            print(f"- {item['name']} ({item.get('node_type')}) - {item['type']}")
            if item.get("doc"):
                print(f"  Doc: {item.get('doc')}")

    except ImportError as e:
        print(f"Tree-sitter not available: {e}")
        print("Install with: pip install tree-sitter tree-sitter-python")
