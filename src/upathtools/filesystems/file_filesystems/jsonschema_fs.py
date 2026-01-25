"""Filesystem implementation for browsing JSON Schema specifications."""

from __future__ import annotations

import importlib
import json
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Required, TypedDict, overload
from urllib.parse import urlparse

import fsspec

from upathtools.filesystems.base import BaseFileFileSystem, BaseUPath, ProbeResult


if TYPE_CHECKING:
    from collections.abc import Callable

    from pydantic import TypeAdapter

    Serializer = (
        Literal["json", "json-formatted", "yaml"] | Callable[[dict[str, Any] | list[Any]], str]
    )


class JsonSchemaInfo(TypedDict, total=False):
    """Info dict for JSON Schema filesystem paths."""

    name: Required[str]
    type: Required[str]
    size: int
    schema_type: str | list[str] | None
    description: str | None
    required: bool
    default: Any
    enum: list[Any] | None
    ref: str | None
    items_type: str | None
    properties_count: int
    definition_count: int


class JsonSchemaPath(BaseUPath[JsonSchemaInfo]):
    """UPath implementation for browsing JSON Schema specifications."""

    __slots__ = ()

    def iterdir(self):
        if not self.is_dir():
            raise NotADirectoryError(str(self))
        yield from super().iterdir()

    @property
    def path(self) -> str:
        path = super().path
        return "/" if path == "." else path


class JsonSchemaFileSystem(BaseFileFileSystem[JsonSchemaPath, JsonSchemaInfo]):
    """Filesystem for browsing JSON Schema specifications.

    Provides a virtual filesystem interface to explore JSON Schema documents,
    making it easy for agents to navigate large schemas token-efficiently.

    Structure:
    - /: Root showing top-level sections
    - /$defs/: All schema definitions
    - /$defs/{name}/: A specific definition's structure
    - /properties/: Root schema properties
    - /properties/{name}: Property details
    - /__meta__: Schema metadata ($schema, title, description)
    - /__raw__: Raw JSON schema
    """

    protocol = "jsonschema"
    upath_cls = JsonSchemaPath
    supported_extensions: ClassVar[frozenset[str]] = frozenset({"json", "yaml", "yml"})
    priority: ClassVar[int] = 50  # Higher priority than generic JSON handlers

    @classmethod
    def probe_content(cls, content: bytes, extension: str = "") -> ProbeResult:  # noqa: PLR0911
        """Probe content to check if it's a JSON Schema.

        Looks for JSON Schema indicators like $schema, $defs, or type with properties.
        """
        try:
            text = content.decode("utf-8")
            # Try JSON first
            try:
                data = json.loads(text)
            except json.JSONDecodeError:
                # Try YAML
                try:
                    import yaml

                    data = yaml.safe_load(text)
                except Exception:  # noqa: BLE001
                    return ProbeResult.UNSUPPORTED

            if not isinstance(data, dict):
                return ProbeResult.UNSUPPORTED

            # Strong indicators of JSON Schema
            if "$schema" in data and "json-schema" in str(data.get("$schema", "")).lower():
                return ProbeResult.SUPPORTED
            if "$defs" in data or "definitions" in data:
                return ProbeResult.SUPPORTED
            # Weaker indicators - looks like a schema definition
            if "type" in data and "properties" in data:
                return ProbeResult.MAYBE
            if "allOf" in data or "anyOf" in data or "oneOf" in data:
                return ProbeResult.MAYBE

        except Exception:  # noqa: BLE001
            return ProbeResult.UNSUPPORTED
        else:
            return ProbeResult.UNSUPPORTED

    def __init__(
        self,
        schema_url: str = "",
        headers: dict[str, str] | None = None,
        resolve_refs: bool = False,
        target_protocol: str | None = None,
        target_options: dict[str, Any] | None = None,
        serializer: Serializer = "json",
        **kwargs: Any,
    ) -> None:
        """Initialize the filesystem.

        Args:
            schema_url: URL or file path to JSON Schema
            headers: HTTP headers for fetching remote schemas
            resolve_refs: If True, transparently resolve $ref when navigating
            target_protocol: Protocol for source file (e.g., 's3', 'file')
            target_options: Options for target protocol
            serializer: Output format - "json" (compact), "json-formatted" (pretty-printed), "yaml",
                        or custom callable
            kwargs: Additional keyword arguments for the filesystem
        """
        super().__init__(**kwargs)

        fo = kwargs.pop("fo", "")
        url = schema_url or fo

        if not url:
            msg = "JSON Schema URL required"
            raise ValueError(msg)

        self.schema_url = url
        self.headers = headers or {}
        self.resolve_refs = resolve_refs
        self.target_protocol = target_protocol
        self.target_options = target_options or {}
        self.serializer = serializer
        self._schema: dict[str, Any] | None = None

    @classmethod
    def from_content(
        cls,
        content: bytes,
        **kwargs: Any,
    ) -> JsonSchemaFileSystem:
        """Create filesystem instance from raw JSON Schema content.

        Args:
            content: Raw JSON Schema content as bytes (JSON or YAML).
            **kwargs: Additional filesystem options.

        Returns:
            Configured filesystem instance with pre-loaded schema.
        """
        fs = cls(schema_url="<content>", **kwargs)
        text = content.decode("utf-8")
        # Try JSON first, then YAML
        try:
            fs._schema = json.loads(text)
        except json.JSONDecodeError:
            try:
                import yaml

                fs._schema = yaml.safe_load(text)
            except ImportError as exc:
                msg = "PyYAML required for YAML content"
                raise ImportError(msg) from exc
        return fs

    @classmethod
    def from_type(
        cls,
        model: Any,
        resolve_refs: bool = True,
        **kwargs: Any,
    ) -> JsonSchemaFileSystem:
        """Create filesystem from a Python type using Pydantic TypeAdapter.

        Supports any TypeAdapter-compatible type including:
        - Pydantic BaseModel classes
        - Standard Python dataclasses
        - TypedDict definitions
        - Other structured types

        Args:
            model: Any type supported by TypeAdapter, or import path string
                   (e.g., "mypackage.MyModel")
            resolve_refs: If True (default), transparently resolve $ref when
                          navigating. Recommended for type-generated schemas.
            **kwargs: Additional filesystem options.

        Returns:
            Configured filesystem instance with generated schema.
        """
        from pydantic import TypeAdapter

        if isinstance(model, str):
            model = cls._import_type(model)

        adapter: TypeAdapter[Any] = TypeAdapter(model)
        schema = adapter.json_schema()

        fs = cls(schema_url="<generated-from-type>", resolve_refs=resolve_refs, **kwargs)
        fs._schema = schema
        return fs

    @staticmethod
    def _import_type(import_path: str) -> Any:
        """Import a type from a string path."""
        try:
            module_path, class_name = import_path.rsplit(".", 1)
            module = importlib.import_module(module_path)
            return getattr(module, class_name)
        except (ImportError, AttributeError, ValueError) as exc:
            msg = f"Could not import type from {import_path}"
            raise FileNotFoundError(msg) from exc

    @classmethod
    def from_file(
        cls,
        path: str,
        target_protocol: str | None = None,
        target_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> JsonSchemaFileSystem:
        """Create filesystem instance from a JSON Schema file path."""
        return cls(schema_url=path, **kwargs)

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, Any]:
        path = path.removeprefix("jsonschema://")

        # Check for query parameters at the end
        if "?" in path:
            schema_part, query_part = path.rsplit("?", 1)
            result: dict[str, Any] = {"schema_url": schema_part}

            from urllib.parse import parse_qs

            params = parse_qs(query_part)
            if "resolve_refs" in params:
                result["resolve_refs"] = params["resolve_refs"][0].lower() in ("true", "1", "yes")

            return result

        return {"schema_url": path}

    def _load_schema(self) -> dict[str, Any]:
        """Load and parse the JSON Schema."""
        import requests

        if self._schema is not None:
            return self._schema

        try:
            parsed_url = urlparse(self.schema_url)
            if parsed_url.scheme in ("http", "https"):
                response = requests.get(self.schema_url, headers=self.headers, timeout=30)
                response.raise_for_status()
                self._schema = response.json()
            else:
                with fsspec.open(
                    self.schema_url,
                    "r",
                    protocol=self.target_protocol,
                    **self.target_options,
                ) as f:
                    content = f.read()  # pyright: ignore[reportAttributeAccessIssue]
                if self.schema_url.endswith((".yaml", ".yml")):
                    try:
                        import yaml

                        self._schema = yaml.safe_load(content)
                    except ImportError as exc:
                        msg = "PyYAML required for YAML files"
                        raise ImportError(msg) from exc
                else:
                    self._schema = json.loads(content)

        except Exception as exc:
            msg = f"Failed to load JSON Schema from {self.schema_url}: {exc}"
            raise FileNotFoundError(msg) from exc

        return self._schema

    def _serialize(self, data: dict[str, Any] | list[Any]) -> bytes:
        """Serialize data using configured serializer.

        Args:
            data: Dictionary or list to serialize

        Returns:
            Serialized bytes
        """
        if callable(self.serializer):
            # Custom serializer
            result = self.serializer(data)
            return result.encode() if isinstance(result, str) else result

        if self.serializer == "yaml":
            try:
                import yaml

                return yaml.dump(data, default_flow_style=False, sort_keys=False).encode()
            except ImportError as exc:
                msg = "PyYAML is required for YAML serialization. Install with: pip install pyyaml"
                raise ImportError(msg) from exc

        if self.serializer == "json-formatted":
            return json.dumps(data, indent=2).encode()

        # Default: compact JSON
        return json.dumps(data, separators=(",", ":")).encode()

    def _get_schema_at_path(self, path_parts: list[str]) -> dict[str, Any] | None:
        """Navigate to a specific path in the schema and return that subschema."""
        schema = self._load_schema()
        current = schema

        for part in path_parts:
            if isinstance(current, dict):
                # Resolve $ref if enabled
                if self.resolve_refs:
                    current = self._resolve_ref(current)
                if part in current or (part.startswith("$") and part in current):
                    current = current[part]
                else:
                    return None
            else:
                return None

        # Final resolution
        if self.resolve_refs and isinstance(current, dict):
            current = self._resolve_ref(current)

        return current if isinstance(current, dict) else None

    def _resolve_ref(self, schema: dict[str, Any]) -> dict[str, Any]:
        """Resolve a $ref in the schema if present."""
        if "$ref" not in schema:
            return schema

        ref = schema["$ref"]
        if not ref.startswith("#/"):
            return schema  # Only resolve local refs

        # Parse ref path (e.g., "#/$defs/Address" -> ["$defs", "Address"])
        ref_parts = ref[2:].split("/")
        root_schema = self._load_schema()
        resolved = root_schema

        for part in ref_parts:
            if isinstance(resolved, dict) and part in resolved:
                resolved = resolved[part]
            else:
                return schema  # Can't resolve, return original

        if isinstance(resolved, dict):
            # Merge any additional properties from the original schema
            # (excluding $ref itself)
            merged = dict(resolved)
            for k, v in schema.items():
                if k != "$ref":
                    merged[k] = v  # noqa: PERF403
            return merged

        return schema

    def _get_type_string(self, schema: dict[str, Any]) -> str | None:  # noqa: PLR0911
        """Get a human-readable type string from a schema."""
        if "$ref" in schema:
            ref = schema["$ref"]
            if ref.startswith("#/$defs/"):
                return f"$ref:{ref[8:]}"
            return f"$ref:{ref}"

        if "type" in schema:
            type_val = schema["type"]
            if isinstance(type_val, list):
                return " | ".join(type_val)
            if type_val == "array" and "items" in schema:
                items_type = self._get_type_string(schema["items"])
                return f"array[{items_type}]"
            return type_val

        if "anyOf" in schema:
            types = [self._get_type_string(s) for s in schema["anyOf"]]
            return " | ".join(t for t in types if t)

        if "oneOf" in schema:
            types = [self._get_type_string(s) for s in schema["oneOf"]]
            return " | ".join(t for t in types if t)

        if "allOf" in schema:
            return "allOf[...]"

        if "const" in schema:
            return f"const:{schema['const']!r}"

        if "enum" in schema:
            return f"enum[{len(schema['enum'])}]"

        return None

    def _format_property_info(
        self,
        name: str,
        prop_schema: dict[str, Any],
        required_props: list[str] | None = None,
    ) -> JsonSchemaInfo:
        """Format property information for listing."""
        is_required = required_props is not None and name in required_props
        schema_type = self._get_type_string(prop_schema)

        info = JsonSchemaInfo(
            name=name,
            type="file",
            size=len(json.dumps(prop_schema)),
            schema_type=schema_type,
            description=prop_schema.get("description"),
            required=is_required,
        )

        if "default" in prop_schema:
            info["default"] = prop_schema["default"]
        if "enum" in prop_schema:
            info["enum"] = prop_schema["enum"]
        if "$ref" in prop_schema:
            info["ref"] = prop_schema["$ref"]

        return info

    @overload
    def ls(self, path: str, detail: Literal[True] = ..., **kwargs: Any) -> list[JsonSchemaInfo]: ...

    @overload
    def ls(self, path: str, detail: Literal[False], **kwargs: Any) -> list[str]: ...

    def ls(self, path: str, detail: bool = True, **kwargs: Any) -> list[JsonSchemaInfo] | list[str]:
        """List JSON Schema contents."""
        schema = self._load_schema()
        path = self._strip_protocol(path).strip("/")  # pyright: ignore[reportAttributeAccessIssue]

        if not path:
            # Root level - show available sections
            items = []
            if "properties" in schema:
                items.append("properties")
            if "$defs" in schema:
                items.append("$defs")
            if "definitions" in schema:
                items.append("definitions")
            items.extend(["__meta__", "__raw__"])

            if not detail:
                return items

            result: list[JsonSchemaInfo] = []
            if "properties" in schema:
                prop_count = len(schema["properties"])
                result.append(
                    JsonSchemaInfo(
                        name="properties",
                        type="directory",
                        size=0,
                        description=f"Schema properties ({prop_count} fields)",
                        properties_count=prop_count,
                    )
                )
            if "$defs" in schema:
                def_count = len(schema["$defs"])
                result.append(
                    JsonSchemaInfo(
                        name="$defs",
                        type="directory",
                        size=0,
                        description=f"Schema definitions ({def_count} types)",
                        definition_count=def_count,
                    )
                )
            if "definitions" in schema:
                def_count = len(schema["definitions"])
                result.append(
                    JsonSchemaInfo(
                        name="definitions",
                        type="directory",
                        size=0,
                        description=f"Schema definitions ({def_count} types)",
                        definition_count=def_count,
                    )
                )
            result.append(
                JsonSchemaInfo(
                    name="__meta__",
                    type="file",
                    size=0,
                    description="Schema metadata ($schema, title, description)",
                )
            )
            result.append(
                JsonSchemaInfo(
                    name="__raw__",
                    type="file",
                    size=len(json.dumps(schema)),
                    description="Raw JSON schema",
                )
            )
            return result

        parts = path.split("/")

        match parts[0]:
            case "properties":
                return self._ls_properties(schema, parts[1:], detail)

            case "$defs" | "definitions":
                defs_key = parts[0]
                definitions = schema.get(defs_key, {})
                return self._ls_definitions(definitions, parts[1:], detail, defs_key)

            case "__meta__" | "__raw__":
                # These are files, not directories
                return []

            case _:
                return []

    def _ls_properties(
        self,
        schema: dict[str, Any],
        remaining_parts: list[str],
        detail: bool,
    ) -> list[JsonSchemaInfo] | list[str]:
        """List schema properties."""
        properties = schema.get("properties", {})
        required = schema.get("required", [])

        if not remaining_parts:
            # List all properties
            if not detail:
                return list(properties.keys())

            return [
                self._format_property_info(name, prop_schema, required)
                for name, prop_schema in properties.items()
            ]

        # Navigate into a specific property
        prop_name = remaining_parts[0]
        if prop_name not in properties:
            return []

        prop_schema = properties[prop_name]
        return self._ls_schema_node(prop_schema, remaining_parts[1:], detail)

    def _ls_definitions(
        self,
        definitions: dict[str, Any],
        remaining_parts: list[str],
        detail: bool,
        defs_key: str,
    ) -> list[JsonSchemaInfo] | list[str]:
        """List schema definitions."""
        if not remaining_parts:
            # List all definitions
            if not detail:
                return list(definitions.keys())

            result = []
            for name, def_schema in definitions.items():
                prop_count = len(def_schema.get("properties", {}))
                result.append(
                    JsonSchemaInfo(
                        name=name,
                        type="directory" if "properties" in def_schema else "file",
                        size=len(json.dumps(def_schema)),
                        description=def_schema.get("description"),
                        schema_type=self._get_type_string(def_schema),
                        properties_count=prop_count if prop_count else 0,
                    )
                )
            return result

        # Navigate into a specific definition
        def_name = remaining_parts[0]
        if def_name not in definitions:
            return []

        def_schema = definitions[def_name]
        return self._ls_schema_node(def_schema, remaining_parts[1:], detail)

    def _ls_schema_node(  # noqa: PLR0911
        self,
        schema: dict[str, Any],
        remaining_parts: list[str],
        detail: bool,
    ) -> list[JsonSchemaInfo] | list[str]:
        """List contents of a schema node (property or definition)."""
        # Resolve $ref if enabled
        if self.resolve_refs:
            schema = self._resolve_ref(schema)

        if not remaining_parts:
            # List available sub-sections of this schema
            items = []
            if "properties" in schema:
                items.append("properties")
            if "items" in schema:
                items.append("items")
            if "anyOf" in schema:
                items.append("anyOf")
            if "oneOf" in schema:
                items.append("oneOf")
            if "allOf" in schema:
                items.append("allOf")
            items.append("__schema__")

            if not detail:
                return items

            result: list[JsonSchemaInfo] = []
            if "properties" in schema:
                count = len(schema["properties"])
                result.append(
                    JsonSchemaInfo(
                        name="properties",
                        type="directory",
                        size=0,
                        description=f"Nested properties ({count} fields)",
                        properties_count=count,
                    )
                )
            if "items" in schema:
                result.append(
                    JsonSchemaInfo(
                        name="items",
                        type="directory",
                        size=0,
                        description="Array item schema",
                        schema_type=self._get_type_string(schema["items"]),
                    )
                )
            for key in ("anyOf", "oneOf", "allOf"):
                if key in schema:
                    count = len(schema[key])
                    result.append(
                        JsonSchemaInfo(
                            name=key,
                            type="directory",
                            size=0,
                            description=f"{key} variants ({count} options)",
                        )
                    )
            result.append(
                JsonSchemaInfo(
                    name="__schema__",
                    type="file",
                    size=len(json.dumps(schema)),
                    description="Full schema for this node",
                )
            )
            return result

        # Handle nested navigation
        next_part = remaining_parts[0]
        rest = remaining_parts[1:]

        match next_part:
            case "properties":
                if "properties" not in schema:
                    return []
                props = schema["properties"]
                required = schema.get("required", [])

                if not rest:
                    if not detail:
                        return list(props.keys())
                    return [
                        self._format_property_info(name, prop_schema, required)
                        for name, prop_schema in props.items()
                    ]

                prop_name = rest[0]
                if prop_name not in props:
                    return []
                return self._ls_schema_node(props[prop_name], rest[1:], detail)

            case "items":
                if "items" not in schema:
                    return []
                return self._ls_schema_node(schema["items"], rest, detail)

            case "anyOf" | "oneOf" | "allOf":
                if next_part not in schema:
                    return []
                variants = schema[next_part]

                if not rest:
                    if not detail:
                        return [str(i) for i in range(len(variants))]
                    return [
                        JsonSchemaInfo(
                            name=str(i),
                            type="directory" if "properties" in v else "file",
                            size=len(json.dumps(v)),
                            schema_type=self._get_type_string(v),
                            description=v.get("description"),
                        )
                        for i, v in enumerate(variants)
                    ]

                try:
                    idx = int(rest[0])
                    if 0 <= idx < len(variants):
                        return self._ls_schema_node(variants[idx], rest[1:], detail)
                except ValueError:
                    pass
                return []

            case "__schema__":
                return []

            case _:
                return []

    def cat(self, path: str, **kwargs: Any) -> bytes:
        """Read file contents."""
        schema = self._load_schema()
        path = self._strip_protocol(path).strip("/")  # pyright: ignore[reportAttributeAccessIssue]
        parts = path.split("/") if path else []

        if not parts or parts[-1] == "__raw__":
            return self._serialize(schema)

        if parts[-1] == "__meta__":
            meta = {
                "$schema": schema.get("$schema"),
                "title": schema.get("title"),
                "description": schema.get("description"),
                "type": schema.get("type"),
            }
            return self._serialize({k: v for k, v in meta.items() if v})

        # Handle __schema__ for nested paths
        if parts[-1] == "__schema__":
            node = self._navigate_to_node(parts[:-1])
            if node is not None:
                return self._serialize(node)
            msg = f"Path not found: {path}"
            raise FileNotFoundError(msg)

        # Navigate to the specific property/definition
        node = self._navigate_to_node(parts)
        if node is not None:
            return self._serialize(node)

        msg = f"Path not found: {path}"
        raise FileNotFoundError(msg)

    def _navigate_to_node(self, parts: list[str]) -> dict[str, Any] | Any | None:
        """Navigate schema to reach a specific node."""
        schema = self._load_schema()
        current: Any = schema

        i = 0
        while i < len(parts):
            part = parts[i]

            # Resolve $ref if enabled
            if self.resolve_refs and isinstance(current, dict):
                current = self._resolve_ref(current)

            if isinstance(current, dict):
                if part in current:
                    current = current[part]
                elif part.isdigit() and isinstance(current, list):
                    idx = int(part)
                    if 0 <= idx < len(current):
                        current = current[idx]
                    else:
                        return None
                else:
                    return None
            elif isinstance(current, list) and part.isdigit():
                idx = int(part)
                if 0 <= idx < len(current):
                    current = current[idx]
                else:
                    return None
            else:
                return None

            i += 1

        # Final resolution
        if self.resolve_refs and isinstance(current, dict):
            current = self._resolve_ref(current)

        return current

    def info(self, path: str, **kwargs: Any) -> JsonSchemaInfo:
        """Get info about a path."""
        schema = self._load_schema()
        path = self._strip_protocol(path).strip("/")  # pyright: ignore[reportAttributeAccessIssue]

        if not path:
            return JsonSchemaInfo(
                name="/",
                type="directory",
                size=len(json.dumps(schema)),
                description=schema.get("title") or schema.get("description"),
            )

        parts = path.split("/")

        # Special files
        if parts[-1] in ("__raw__", "__meta__", "__schema__"):
            content = self.cat(path)
            return JsonSchemaInfo(
                name=parts[-1],
                type="file",
                size=len(content),
            )

        # Try to navigate and determine if it's a file or directory
        node = self._navigate_to_node(parts)
        if node is None:
            msg = f"Path not found: {path}"
            raise FileNotFoundError(msg)

        if isinstance(node, dict):
            # It's a directory if it has properties or is a schema object
            has_children = (
                "properties" in node
                or "items" in node
                or any(k in node for k in ("anyOf", "oneOf", "allOf"))
            )
            return JsonSchemaInfo(
                name=parts[-1],
                type="directory" if has_children else "file",
                size=len(json.dumps(node)),
                description=node.get("description"),
                schema_type=self._get_type_string(node) if isinstance(node, dict) else None,
            )

        return JsonSchemaInfo(
            name=parts[-1],
            type="file",
            size=len(json.dumps(node)),
        )

    def isdir(self, path: str) -> bool:  # noqa: PLR0911
        """Check if path is a directory.

        Args:
            path: Path to check

        Returns:
            True if path is a directory (has navigable children), False otherwise
        """
        path = self._strip_protocol(path).strip("/")  # pyright: ignore[reportAttributeAccessIssue]

        if not path:
            # Root is always a directory
            return True

        parts = path.split("/")

        # Special files are never directories
        if parts[-1] in ("__raw__", "__meta__", "__schema__"):
            return False

        # Check if it's a known directory section at root
        if len(parts) == 1 and parts[0] in ("properties", "$defs", "definitions"):
            schema = self._load_schema()
            return parts[0] in schema

        # Navigate to the node and check if it has children
        try:
            node = self._navigate_to_node(parts)
            if node is None:
                return False

            if isinstance(node, dict):
                # It's a directory if it has navigable children
                return (
                    "properties" in node
                    or "items" in node
                    or any(k in node for k in ("anyOf", "oneOf", "allOf"))
                    or parts[-1]
                    in ("properties", "$defs", "definitions", "items", "anyOf", "oneOf", "allOf")
                )

        except (FileNotFoundError, KeyError):
            return False
        else:
            return False

    def _open(self, path: str, mode: str = "rb", **kwargs: Any) -> Any:
        """Open a file for reading."""
        import io

        if "w" in mode or "a" in mode:
            msg = "Write mode not supported"
            raise NotImplementedError(msg)

        content = self.cat(path)
        if "b" not in mode:
            content_str = content.decode()
            return io.StringIO(content_str)
        return io.BytesIO(content)


# Register the filesystem
fsspec.register_implementation("jsonschema", JsonSchemaFileSystem)
