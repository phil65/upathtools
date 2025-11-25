"""Filesystem implementation for browsing code structure using tree-sitter."""

from __future__ import annotations

import io
import os
from typing import TYPE_CHECKING, Any, Literal, TypedDict, overload

import fsspec

from upathtools.filesystems.base import BaseFileSystem, BaseUPath


if TYPE_CHECKING:
    from collections.abc import Sequence


class TreeSitterInfo(TypedDict, total=False):
    """Info dict for tree-sitter filesystem paths."""

    name: str
    type: Literal["directory", "file"]
    node_type: str
    size: int
    start_byte: int
    end_byte: int
    doc: str | None


class CodeNode:
    """Represents a named code entity (function, class, variable, etc.)."""

    def __init__(
        self,
        name: str,
        node_type: str,
        start_byte: int,
        end_byte: int,
        children: dict[str, CodeNode] | None = None,
        doc: str | None = None,
    ) -> None:
        """Initialize a code node.

        Args:
            name: The identifier/name from source code
            node_type: Tree-sitter node type
            start_byte: Start position in source
            end_byte: End position in source
            children: Child nodes (methods, nested classes, etc.)
            doc: Associated docstring if any
        """
        self.name = name
        self.node_type = node_type
        self.start_byte = start_byte
        self.end_byte = end_byte
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


class TreeSitterFS(BaseFileSystem[TreeSitterPath, TreeSitterInfo]):
    """Browse source code structure using tree-sitter."""

    protocol = "ts"
    upath_cls = TreeSitterPath

    # Language extensions mapping
    LANGUAGE_MAP = {
        ".py": "python",
        ".js": "javascript",
        ".ts": "typescript",
        ".jsx": "javascript",
        ".tsx": "typescript",
        ".c": "c",
        ".cpp": "cpp",
        ".cc": "cpp",
        ".cxx": "cpp",
        ".h": "c",
        ".hpp": "cpp",
        ".java": "java",
        ".rs": "rust",
        ".go": "go",
        ".rb": "ruby",
        ".php": "php",
        ".cs": "c_sharp",
    }

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
            ext = os.path.splitext(path)[1].lower()
            self.language = self.LANGUAGE_MAP.get(ext, "python")

        # Initialize state
        self._source: str | None = None
        self._root: CodeNode | None = None
        self._parser = None
        self._tree = None

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

        try:
            # Import tree-sitter dynamically
            from tree_sitter import Language, Parser

            # Import the specific language
            language_module = self._import_language_module()
            language = Language(language_module.language())

            # Create parser
            self._parser = Parser(language)
            self._tree = self._parser.parse(self._source.encode())

            # Build node hierarchy
            self._root = CodeNode("root", "module", 0, len(self._source.encode()))
            self._extract_nodes(self._tree.root_node, self._root)

        except ImportError as e:
            msg = f"Tree-sitter support not available: {e}. Install with: pip install tree-sitter tree-sitter-{self.language}"
            raise ImportError(msg) from e

    def _import_language_module(self):
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

        module_name = language_modules.get(self.language)
        if not module_name:
            msg = f"Language {self.language} not supported"
            raise ValueError(msg)

        try:
            return __import__(module_name)
        except ImportError as e:
            msg = f"Language module {module_name} not installed. Install with: pip install {module_name}"
            raise ImportError(msg) from e

    def _extract_nodes(self, ts_node, parent_node: CodeNode) -> None:
        """Extract named entities from tree-sitter node."""
        # Language-specific node extraction
        if self.language == "python":
            self._extract_python_nodes(ts_node, parent_node)
        elif self.language in ("javascript", "typescript"):
            self._extract_js_nodes(ts_node, parent_node)
        else:
            self._extract_generic_nodes(ts_node, parent_node)

    def _extract_python_nodes(self, ts_node, parent_node: CodeNode) -> None:
        """Extract Python-specific named entities."""
        imports_node = None

        for child in ts_node.children:
            node_type = child.type

            if node_type in ("function_definition", "async_function_definition"):
                name_node = child.child_by_field_name("name")
                if name_node:
                    name = self._get_node_text(name_node)
                    doc = self._extract_python_docstring(child)
                    func_node = CodeNode(name, node_type, child.start_byte, child.end_byte, doc=doc)
                    parent_node.children[name] = func_node

            elif node_type == "class_definition":
                name_node = child.child_by_field_name("name")
                if name_node:
                    name = self._get_node_text(name_node)
                    doc = self._extract_python_docstring(child)
                    class_node = CodeNode(
                        name, node_type, child.start_byte, child.end_byte, doc=doc
                    )
                    parent_node.children[name] = class_node
                    # Recursively extract methods and nested classes
                    self._extract_nodes(child, class_node)

            elif node_type == "assignment":
                # Handle variable assignments
                target = child.child_by_field_name("left")
                if target and target.type == "identifier":
                    name = self._get_node_text(target)
                    var_node = CodeNode(name, node_type, child.start_byte, child.end_byte)
                    parent_node.children[name] = var_node

            elif node_type in ("import_statement", "import_from_statement"):
                # Group all imports under a single "imports" directory
                if imports_node is None:
                    imports_node = CodeNode("imports", "imports_group", 0, 0)
                    parent_node.children["imports"] = imports_node

                import_text = self._get_node_text(child).strip()
                # Use the actual import line as the name
                import_node = CodeNode(import_text, node_type, child.start_byte, child.end_byte)
                imports_node.children[import_text] = import_node

            else:
                # Continue recursively for other nodes
                self._extract_nodes(child, parent_node)

    def _extract_js_nodes(self, ts_node, parent_node: CodeNode) -> None:
        """Extract JavaScript/TypeScript-specific named entities."""
        imports_node = None

        for child in ts_node.children:
            node_type = child.type

            if node_type == "function_declaration":
                name_node = child.child_by_field_name("name")
                if name_node:
                    name = self._get_node_text(name_node)
                    func_node = CodeNode(name, node_type, child.start_byte, child.end_byte)
                    parent_node.children[name] = func_node

            elif node_type == "class_declaration":
                name_node = child.child_by_field_name("name")
                if name_node:
                    name = self._get_node_text(name_node)
                    class_node = CodeNode(name, node_type, child.start_byte, child.end_byte)
                    parent_node.children[name] = class_node
                    self._extract_nodes(child, class_node)

            elif node_type == "method_definition":
                name_node = child.child_by_field_name("name")
                if name_node:
                    name = self._get_node_text(name_node)
                    method_node = CodeNode(name, node_type, child.start_byte, child.end_byte)
                    parent_node.children[name] = method_node

            elif node_type == "variable_declaration":
                # Extract variable names
                for declarator in child.children:
                    if declarator.type == "variable_declarator":
                        name_node = declarator.child_by_field_name("name")
                        if name_node:
                            name = self._get_node_text(name_node)
                            var_node = CodeNode(
                                name, node_type, declarator.start_byte, declarator.end_byte
                            )
                            parent_node.children[name] = var_node

            elif node_type in ("import_statement", "import_declaration", "export_statement"):
                # Group all imports/exports under a single "imports" directory
                if imports_node is None:
                    imports_node = CodeNode("imports", "imports_group", 0, 0)
                    parent_node.children["imports"] = imports_node

                import_text = self._get_node_text(child).strip()
                # Use the actual import line as the name
                import_node = CodeNode(import_text, node_type, child.start_byte, child.end_byte)
                imports_node.children[import_text] = import_node

            else:
                self._extract_nodes(child, parent_node)

    def _extract_generic_nodes(self, ts_node, parent_node: CodeNode) -> None:
        """Generic extraction for other languages."""
        for child in ts_node.children:
            # Look for named nodes that might represent identifiable entities
            if child.is_named and child.type not in ("comment", "string", "number"):
                # Try to find a name/identifier child
                name = None
                for grandchild in child.children:
                    if grandchild.type == "identifier":
                        name = self._get_node_text(grandchild)
                        break

                if name:
                    entity_node = CodeNode(name, child.type, child.start_byte, child.end_byte)
                    parent_node.children[name] = entity_node
                    self._extract_nodes(child, entity_node)
                else:
                    self._extract_nodes(child, parent_node)
            else:
                self._extract_nodes(child, parent_node)

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
        """List code entities at path."""
        node = self._get_node(path)

        if not detail:
            return list(node.children)

        return [
            {
                "name": name,
                "size": child.get_size(),
                "type": "directory" if child.is_dir() else "file",
                "node_type": child.node_type,
                "start_byte": child.start_byte,
                "end_byte": child.end_byte,
                "doc": child.doc,
            }
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
            doc=node.doc,
        )

    def _open(
        self,
        path: str,
        mode: str = "rb",
        **kwargs: Any,
    ) -> Any:
        """Provide file-like access to entity source code."""
        if "w" in mode or "a" in mode:
            msg = "Write mode not supported"
            raise NotImplementedError(msg)

        content = self.cat(path)
        return io.BytesIO(content)


if __name__ == "__main__":
    # Example usage
    try:
        fs = TreeSitterFS(__file__, language="python")

        print("Code entities:")
        for item in fs.ls("/", detail=True):
            print(f"- {item['name']} ({item['node_type']}) - {item['type']}")
            if item.get("doc"):
                print(f"  Doc: {item['doc']}")

    except ImportError as e:
        print(f"Tree-sitter not available: {e}")
        print("Install with: pip install tree-sitter tree-sitter-python")
