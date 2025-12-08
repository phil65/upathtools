"""Filesystem implementation for browsing JSON Schema specifications."""

from __future__ import annotations

import json
from typing import Any, Literal, Required, TypedDict, overload
from urllib.parse import urlparse

import fsspec

from upathtools.filesystems.base import BaseFileSystem, BaseUPath


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


class JsonSchemaFileSystem(BaseFileSystem[JsonSchemaPath, JsonSchemaInfo]):
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

    def __init__(
        self,
        schema_url: str = "",
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the filesystem.

        Args:
            schema_url: URL or file path to JSON Schema
            headers: HTTP headers for fetching remote schemas
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
        self._schema: dict[str, Any] | None = None

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, Any]:
        path = path.removeprefix("jsonschema://")
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
                with fsspec.open(self.schema_url, "r") as f:
                    content = f.read()
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

    def _get_schema_at_path(self, path_parts: list[str]) -> dict[str, Any] | None:
        """Navigate to a specific path in the schema and return that subschema."""
        schema = self._load_schema()
        current = schema

        for part in path_parts:
            if isinstance(current, dict):
                if part in current or (part.startswith("$") and part in current):
                    current = current[part]
                else:
                    return None
            else:
                return None

        return current if isinstance(current, dict) else None

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
        path = self._strip_protocol(path).strip("/")

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
        path = self._strip_protocol(path).strip("/")
        parts = path.split("/") if path else []

        if not parts or parts[-1] == "__raw__":
            return json.dumps(schema, indent=2).encode()

        if parts[-1] == "__meta__":
            meta = {
                "$schema": schema.get("$schema"),
                "title": schema.get("title"),
                "description": schema.get("description"),
                "type": schema.get("type"),
            }
            return json.dumps({k: v for k, v in meta.items() if v}, indent=2).encode()

        # Handle __schema__ for nested paths
        if parts[-1] == "__schema__":
            node = self._navigate_to_node(parts[:-1])
            if node is not None:
                return json.dumps(node, indent=2).encode()
            msg = f"Path not found: {path}"
            raise FileNotFoundError(msg)

        # Navigate to the specific property/definition
        node = self._navigate_to_node(parts)
        if node is not None:
            return json.dumps(node, indent=2).encode()

        msg = f"Path not found: {path}"
        raise FileNotFoundError(msg)

    def _navigate_to_node(self, parts: list[str]) -> dict[str, Any] | Any | None:
        """Navigate schema to reach a specific node."""
        schema = self._load_schema()
        current: Any = schema

        i = 0
        while i < len(parts):
            part = parts[i]

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

        return current

    def info(self, path: str, **kwargs: Any) -> JsonSchemaInfo:
        """Get info about a path."""
        schema = self._load_schema()
        path = self._strip_protocol(path).strip("/")

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
