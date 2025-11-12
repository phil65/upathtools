"""Filesystem implementation for browsing type schemas via Pydantic TypeAdapter.

Supports any type compatible with Pydantic's TypeAdapter, including:
- Pydantic BaseModel classes
- Standard Python dataclasses
- TypedDict definitions
- Other types supported by TypeAdapter
"""

from __future__ import annotations

import importlib
import json
from typing import Any, Literal, overload

from upathtools.filesystems.base import BaseFileSystem, BaseUPath


class TypeAdapterPath(BaseUPath):
    """UPath implementation for browsing Pydantic BaseModel schemas."""

    __slots__ = ()

    def iterdir(self):
        if not self.is_dir():
            raise NotADirectoryError(str(self))
        yield from super().iterdir()

    @property
    def path(self) -> str:
        path = super().path
        return "/" if path == "." else path


class TypeAdapterFS(BaseFileSystem[TypeAdapterPath]):
    """Filesystem for browsing type schemas via Pydantic TypeAdapter.

    Supports browsing field definitions and schemas for any TypeAdapter-compatible type:
    - Pydantic BaseModel classes
    - Standard Python dataclasses
    - TypedDict definitions
    - Other structured types
    """

    protocol = "typeadapter"
    upath_cls = TypeAdapterPath

    def __init__(
        self,
        model: Any | str,
        **kwargs: Any,
    ) -> None:
        """Initialize the filesystem.

        Args:
            model: Any type supported by TypeAdapter, or import path
                  (e.g., BaseModel, dataclass, TypedDict, "mypackage.MyModel", etc.)
            kwargs: Additional keyword arguments for the filesystem
        """
        super().__init__(**kwargs)

        if isinstance(model, str):
            model_type = self._import_model(model)
            self.model_path = model
        else:
            model_type = model
            self.model_path = getattr(model, "__name__", str(model))

        from pydantic import TypeAdapter

        self.type_adapter = TypeAdapter(model_type)
        self.model_type = model_type

    @staticmethod
    def _get_kwargs_from_urls(path):
        path = path.removeprefix("typeadapter://")
        return {"model": path}

    @classmethod
    def _strip_protocol(cls, path):
        """Override to handle model name in URL by treating it as root path."""
        stripped = super()._strip_protocol(path)
        # If the stripped path equals the model identifier, treat it as root
        # This handles URLs like typeadapter://schemez.Schema where schemez.Schema
        # should be treated as the root path "/" for the model filesystem
        if stripped and "/" not in stripped and "." in stripped:
            # This looks like a model identifier (e.g., "schemez.Schema")
            return ""
        return stripped

    def _import_model(self, import_path: str) -> Any:
        """Import a model class from a string path."""
        try:
            module_path, class_name = import_path.rsplit(".", 1)
            module = importlib.import_module(module_path)
            return getattr(module, class_name)
        except (ImportError, AttributeError) as exc:
            msg = f"Could not import model from {import_path}"
            raise FileNotFoundError(msg) from exc

    def _get_schema_fields(self) -> dict[str, Any]:
        """Get fields from the TypeAdapter's JSON schema."""
        schema = self.type_adapter.json_schema()
        return schema.get("properties", {})

    def _get_nested_field_info(self, path: str) -> tuple[dict[str, Any], str]:
        """Get the field info and final field name at a given path."""
        if not path:
            return self._get_schema_fields(), ""

        parts = path.strip("/").split("/")
        current_fields = self._get_schema_fields()

        for _i, part in enumerate(parts[:-1]):
            if part.startswith("__") and part.endswith("__"):
                # Skip special paths like __schema__, __examples__
                continue

            if part not in current_fields:
                msg = f"Field {part} not found"
                raise FileNotFoundError(msg)

            field_schema = current_fields[part]

            # Navigate into nested object properties
            if "properties" in field_schema:
                current_fields = field_schema["properties"]
            elif field_schema.get("type") == "array" and "items" in field_schema:
                item_schema = field_schema["items"]
                if "properties" in item_schema:
                    current_fields = item_schema["properties"]

        return current_fields, parts[-1] if parts else ""

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
    ) -> list[dict[str, Any]] | list[str]:
        """List model fields and special paths."""
        path = self._strip_protocol(path).strip("/")  # type: ignore

        try:
            current_fields, field_name = self._get_nested_field_info(path)
        except FileNotFoundError:
            return []

        if field_name:
            # Listing a specific field - show special paths
            items = ["__schema__", "__type__", "__constraints__"]
            if field_name in current_fields:
                field_schema = current_fields[field_name]
                if "default" in field_schema:
                    items.append("__default__")
                if field_schema.get("title") != field_name:
                    items.append("__title__")
        else:
            # Listing model root - show all fields plus special paths
            items = list(current_fields.keys())
            items.extend(["__schema__", "__json_schema__", "__examples__"])

        if not detail:
            return items

        result = []
        for item in items:
            if item.startswith("__"):
                result.append({
                    "name": item,
                    "type": "special",
                    "size": 0,
                    "description": f"Special path for {item[2:-2]} information",
                })
            else:
                # It's a field
                field_schema = current_fields[item]

                # Determine if field is nested (has properties or is array of objects)
                is_nested = "properties" in field_schema or (
                    field_schema.get("type") == "array"
                    and field_schema.get("items", {}).get("properties")
                )

                result.append({
                    "name": item,
                    "type": "field",
                    "field_type": field_schema.get("type", "unknown"),
                    "required": item in current_fields,
                    "default": field_schema.get("default"),
                    "title": field_schema.get("title", item),
                    "nested_model": is_nested,
                    "item": 0,
                    "description": field_schema.get("description"),
                })

        return result

    def cat(self, path: str = "") -> bytes:  # noqa: PLR0911
        """Get field definition, schema, or other information."""
        path = self._strip_protocol(path).strip("/")  # type: ignore

        if not path:
            # Return full schema
            schema = self.type_adapter.json_schema()
            return json.dumps(schema, indent=2).encode()

        parts = path.split("/")

        # Handle special paths
        if parts[-1].startswith("__") and parts[-1].endswith("__"):
            special_path = parts[-1]
            field_path = "/".join(parts[:-1])

            try:
                current_fields, field_name = self._get_nested_field_info(field_path)
            except FileNotFoundError:
                msg = f"Path {field_path} not found"
                raise FileNotFoundError(msg) from None

            match special_path:
                case "__schema__" | "__json_schema__":
                    if field_name:
                        # Field schema
                        if field_name not in current_fields:
                            msg = f"Field {field_name} not found"
                            raise FileNotFoundError(msg)
                        field_schema = current_fields[field_name]
                        return json.dumps(field_schema, indent=2, default=str).encode()
                    # Full schema
                    schema = self.type_adapter.json_schema()
                    return json.dumps(schema, indent=2).encode()

                case "__examples__":
                    # Generate example data
                    try:
                        # Try to create a simple example based on schema
                        schema = self.type_adapter.json_schema()
                        example = self._generate_example_from_schema(schema)
                        return json.dumps(example, indent=2, default=str).encode()
                    except Exception:  # noqa: BLE001
                        return b'{"note": "No examples available"}'

                case "__type__":
                    if not field_name:
                        msg = "__type__ only available for fields"
                        raise FileNotFoundError(msg)
                    if field_name not in current_fields:
                        msg = f"Field {field_name} not found"
                        raise FileNotFoundError(msg)
                    field_schema = current_fields[field_name]
                    field_type = field_schema.get("type", "unknown")
                    return field_type.encode()

                case "__constraints__":
                    if not field_name:
                        msg = "__constraints__ only available for fields"
                        raise FileNotFoundError(msg)
                    if field_name not in current_fields:
                        msg = f"Field {field_name} not found"
                        raise FileNotFoundError(msg)
                    field_schema = current_fields[field_name]
                    constraints = {}
                    for key in [
                        "minimum",
                        "maximum",
                        "minLength",
                        "maxLength",
                        "pattern",
                    ]:
                        if key in field_schema:
                            constraints[key] = field_schema[key]
                    return json.dumps(constraints, indent=2).encode()

                case "__default__":
                    if not field_name:
                        msg = "__default__ only available for fields"
                        raise FileNotFoundError(msg)
                    if field_name not in current_fields:
                        msg = f"Field {field_name} not found"
                        raise FileNotFoundError(msg)
                    field_schema = current_fields[field_name]
                    if "default" not in field_schema:
                        msg = f"Field {field_name} has no default value"
                        raise FileNotFoundError(msg)
                    return json.dumps(field_schema["default"], default=str).encode()

                case "__title__":
                    if not field_name:
                        msg = "__title__ only available for fields"
                        raise FileNotFoundError(msg)
                    if field_name not in current_fields:
                        msg = f"Field {field_name} not found"
                        raise FileNotFoundError(msg)
                    field_schema = current_fields[field_name]
                    title = field_schema.get("title", field_name)
                    return title.encode()

                case _:
                    msg = f"Unknown special path: {special_path}"
                    raise FileNotFoundError(msg)

        # Regular field path
        # Regular field access
        try:
            current_fields, field_name = self._get_nested_field_info(path)
        except FileNotFoundError:
            msg = f"Path {path} not found"
            raise FileNotFoundError(msg) from None

        if not field_name:
            # Return full schema for this level
            return json.dumps(current_fields, indent=2, default=str).encode()

        if field_name not in current_fields:
            msg = f"Field {field_name} not found"
            raise FileNotFoundError(msg)

        field_schema = current_fields[field_name]
        return json.dumps(field_schema, indent=2, default=str).encode()

    def _generate_example_from_schema(self, schema: dict[str, Any]) -> dict[str, Any]:
        """Generate a simple example from JSON schema."""
        if schema.get("type") != "object" or "properties" not in schema:
            return {}

        example: dict[str, Any] = {}
        for field_name, field_schema in schema["properties"].items():
            if field_schema.get("type") == "string":
                example[field_name] = f"example_{field_name}"
            elif field_schema.get("type") == "integer":
                example[field_name] = 42
            elif field_schema.get("type") == "number":
                example[field_name] = 3.14
            elif field_schema.get("type") == "boolean":
                example[field_name] = True
            elif field_schema.get("type") == "array":
                example[field_name] = []
            elif field_schema.get("type") == "object":
                example[field_name] = {}
            else:
                example[field_name] = None

        return example

    def isdir(self, path: str) -> bool:
        """Check if path is a directory (model or nested model)."""
        path = self._strip_protocol(path).strip("/")  # type: ignore

        if not path:
            # Root is always a directory
            return True

        try:
            _current_fields, field_name = self._get_nested_field_info(path)
            return bool(not field_name)
        except FileNotFoundError:
            return False

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Get detailed info about a model or field."""
        path = self._strip_protocol(path).strip("/")  # type: ignore

        if not path:
            # Root model info
            schema = self.type_adapter.json_schema()
            return {
                "name": self.model_path,
                "type": "model",
                "title": schema.get("title", self.model_path),
                "description": schema.get("description"),
                "field_count": len(schema.get("properties", {})),
                "schema": schema,
                "size": len(json.dumps(schema)),
            }

        try:
            current_fields, field_name = self._get_nested_field_info(path)
        except FileNotFoundError as exc:
            msg = f"Path {path} not found"
            raise FileNotFoundError(msg) from exc

        if not field_name:
            # Nested object info
            return {
                "name": path.split("/")[-1],
                "type": "nested_object",
                "field_count": len(current_fields),
                "size": len(json.dumps(current_fields)),
            }

        # Field info
        if field_name not in current_fields:
            msg = f"Field {field_name} not found"
            raise FileNotFoundError(msg)

        field_schema = current_fields[field_name]
        return {
            "name": field_name,
            "type": "field",
            "field_type": field_schema.get("type", "unknown"),
            "title": field_schema.get("title", field_name),
            "description": field_schema.get("description"),
            "default": field_schema.get("default"),
            "size": len(json.dumps(field_schema)),
        }


if __name__ == "__main__":
    # Example usage
    from pydantic import BaseModel, Field

    class User(BaseModel):
        name: str = Field(min_length=1, max_length=50)
        age: int = Field(ge=0, le=120)
        email: str

    # Test with direct filesystem creation
    fs = TypeAdapterFS(User)
    print("Fields:", fs.ls("/", detail=False))
    print("User info:", fs.info("/"))
    print("Name field:", fs.info("/name"))

    # Test with UPath using explicit storage options
    import upath

    path = upath.UPath("/", protocol="typeadapter", model="schemez.Schema")
    print("UPath with explicit options:", path)
    print("Storage options:", path.storage_options)
    print("Fields:", list(path.iterdir())[:5])

    # Test the original failing URL syntax
    path = upath.UPath("typeadapter://schemez.Schema")
    print("Original URL syntax works:", path)
    print("Storage options:", path.storage_options)
    print("Fields:", list(path.iterdir())[:5])

    # Test fsspec directly
    import fsspec

    fs, parsed_path = fsspec.core.url_to_fs("typeadapter://schemez.Schema")
    print("fsspec works - parsed path:", parsed_path)
    print("Filesystem fields:", fs.ls("/", detail=False)[:5])
