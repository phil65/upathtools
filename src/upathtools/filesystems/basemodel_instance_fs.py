"""Filesystem implementation for browsing Pydantic BaseModel instance data."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Literal, overload

from fsspec.spec import AbstractFileSystem
from upath import UPath


if TYPE_CHECKING:
    from pydantic import BaseModel


class BaseModelInstancePath(UPath):
    """UPath implementation for browsing Pydantic BaseModel instance data."""

    __slots__ = ()

    def iterdir(self):
        if not self.is_dir():
            raise NotADirectoryError(str(self))
        yield from super().iterdir()

    @property
    def path(self) -> str:
        path = super().path
        return "/" if path == "." else path


class BaseModelInstanceFS(AbstractFileSystem):
    """Filesystem for browsing Pydantic BaseModel instance data and values."""

    protocol = "basemodel-instance"

    def __init__(
        self,
        instance: BaseModel,
        **kwargs: Any,
    ) -> None:
        """Initialize the filesystem.

        Args:
            instance: BaseModel instance to browse
            kwargs: Additional keyword arguments for the filesystem
        """
        super().__init__(**kwargs)

        if not hasattr(type(instance), "model_fields"):
            msg = "instance must be a Pydantic BaseModel instance"
            raise ValueError(msg)

        self.instance = instance

    def _make_path(self, path: str) -> UPath:
        """Create a path object from string."""
        return BaseModelInstancePath(path)

    def _get_nested_value_at_path(self, path: str) -> tuple[Any, str]:
        """Get the object and field name at a given path."""
        if not path:
            return self.instance, ""

        parts = path.strip("/").split("/")
        current_obj = self.instance

        for i, part in enumerate(parts[:-1]):
            if part.startswith("__") and part.endswith("__"):
                # Skip special paths like __json__, __dict__
                continue

            if not hasattr(current_obj, part):
                msg = f"Field {part} not found in {type(current_obj).__name__}"
                raise FileNotFoundError(msg)

            current_obj = getattr(current_obj, part)

            # Handle list/dict access with numeric indices
            if isinstance(current_obj, (list, tuple)) and i + 1 < len(parts) - 1:
                next_part = parts[i + 1]
                if next_part.isdigit():
                    idx = int(next_part)
                    if idx >= len(current_obj):
                        msg = f"Index {idx} out of range for {part}"
                        raise FileNotFoundError(msg)
                    current_obj = current_obj[idx]
                    parts.pop(i + 1)  # Remove the index from parts

        return current_obj, parts[-1] if parts else ""

    def _is_basemodel_instance(self, obj: Any) -> bool:
        """Check if object is a BaseModel instance."""
        return hasattr(type(obj), "model_fields") and hasattr(obj, "model_dump")

    def _is_list_like(self, obj: Any) -> bool:
        """Check if object is list-like."""
        return isinstance(obj, (list, tuple, set))

    def _is_dict_like(self, obj: Any) -> bool:
        """Check if object is dict-like."""
        return isinstance(obj, dict)

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
        """List instance fields and values."""
        path = self._strip_protocol(path).strip("/")  # type: ignore

        try:
            current_obj, field_name = self._get_nested_value_at_path(path)
        except FileNotFoundError:
            return []

        if field_name:
            # Listing a specific field value
            if not hasattr(current_obj, field_name):
                return []

            field_value = getattr(current_obj, field_name)

            if self._is_basemodel_instance(field_value):
                # BaseModel instance - show its fields
                items = list(type(field_value).model_fields.keys())
                items.extend(["__json__", "__dict__", "__schema__"])
            elif self._is_list_like(field_value):
                # List-like - show indices
                items = [str(i) for i in range(len(field_value))]
                items.extend(["__json__", "__length__", "__type__"])
            elif self._is_dict_like(field_value):
                # Dict-like - show keys
                items = list(field_value.keys())
                items.extend(["__json__", "__keys__", "__values__"])
            else:
                # Primitive value - show special paths
                items = ["__value__", "__type__", "__str__", "__repr__"]
        # Listing model root - show all fields plus special paths
        elif self._is_basemodel_instance(current_obj):
            items = list(type(current_obj).model_fields.keys())
            items.extend(["__json__", "__dict__", "__schema__", "__model_dump__"])
        elif self._is_list_like(current_obj):
            items = [str(i) for i in range(len(current_obj))]
            items.extend(["__json__", "__length__", "__type__"])
        elif self._is_dict_like(current_obj):
            items = list(current_obj.keys())
            items.extend(["__json__", "__keys__", "__values__"])
        else:
            items = ["__value__", "__type__", "__str__", "__repr__"]

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
            # It's a field or item
            elif self._is_basemodel_instance(current_obj):
                field_value = getattr(current_obj, item)
                result.append({
                    "name": item,
                    "type": "field",
                    "value_type": type(field_value).__name__,
                    "value": str(field_value)[:100] + "..."
                    if len(str(field_value)) > 100  # noqa: PLR2004
                    else str(field_value),
                    "is_nested": self._is_basemodel_instance(field_value),
                    "is_collection": self._is_list_like(field_value)
                    or self._is_dict_like(field_value),
                })
            elif self._is_list_like(current_obj):
                idx = int(item)
                item_value = current_obj[idx]
                result.append({
                    "name": item,
                    "type": "item",
                    "index": idx,
                    "value_type": type(item_value).__name__,
                    "value": str(item_value)[:100] + "..."
                    if len(str(item_value)) > 100  # noqa: PLR2004
                    else str(item_value),
                    "is_nested": self._is_basemodel_instance(item_value),
                })
            elif self._is_dict_like(current_obj):
                dict_value = current_obj[item]
                result.append({
                    "name": item,
                    "type": "key",
                    "value_type": type(dict_value).__name__,
                    "value": str(dict_value)[:100] + "..."
                    if len(str(dict_value)) > 100  # noqa: PLR2004
                    else str(dict_value),
                    "is_nested": self._is_basemodel_instance(dict_value),
                })

        return result

    def cat(self, path: str = "") -> bytes:  # noqa: PLR0911
        """Get field values, JSON representation, or other information."""
        path = self._strip_protocol(path).strip("/")  # type: ignore

        if not path:
            # Return instance JSON
            if self._is_basemodel_instance(self.instance):
                return self.instance.model_dump_json(indent=2).encode()
            return json.dumps(self.instance, indent=2, default=str).encode()

        parts = path.split("/")

        # Handle special paths
        if parts[-1].startswith("__") and parts[-1].endswith("__"):
            special_path = parts[-1]
            field_path = "/".join(parts[:-1])

            try:
                current_obj, field_name = self._get_nested_value_at_path(field_path)
            except FileNotFoundError:
                msg = f"Path {field_path} not found"
                raise FileNotFoundError(msg) from None

            target_obj = getattr(current_obj, field_name) if field_name else current_obj

            match special_path:
                case "__json__":
                    if self._is_basemodel_instance(target_obj):
                        return target_obj.model_dump_json(indent=2).encode()
                    return json.dumps(target_obj, indent=2, default=str).encode()

                case "__dict__":
                    if self._is_basemodel_instance(target_obj):
                        return json.dumps(target_obj.model_dump(), indent=2).encode()
                    if hasattr(target_obj, "__dict__"):
                        return json.dumps(
                            target_obj.__dict__, indent=2, default=str
                        ).encode()
                    return json.dumps(dict(target_obj), indent=2, default=str).encode()

                case "__schema__":
                    if self._is_basemodel_instance(target_obj):
                        return json.dumps(
                            target_obj.model_json_schema(), indent=2
                        ).encode()
                    msg = "Schema only available for BaseModel instances"
                    raise FileNotFoundError(msg)

                case "__model_dump__":
                    if self._is_basemodel_instance(target_obj):
                        return json.dumps(target_obj.model_dump(), indent=2).encode()
                    msg = "model_dump only available for BaseModel instances"
                    raise FileNotFoundError(msg)

                case "__value__":
                    return str(target_obj).encode()

                case "__type__":
                    return str(type(target_obj)).encode()

                case "__str__":
                    return str(target_obj).encode()

                case "__repr__":
                    return repr(target_obj).encode()

                case "__length__":
                    if hasattr(target_obj, "__len__"):
                        return str(len(target_obj)).encode()
                    msg = f"Length not available for {type(target_obj)}"
                    raise FileNotFoundError(msg)

                case "__keys__":
                    if self._is_dict_like(target_obj):
                        return json.dumps(list(target_obj.keys()), indent=2).encode()
                    msg = "Keys only available for dict-like objects"
                    raise FileNotFoundError(msg)

                case "__values__":
                    if self._is_dict_like(target_obj):
                        return json.dumps(
                            list(target_obj.values()), indent=2, default=str
                        ).encode()
                    msg = "Values only available for dict-like objects"
                    raise FileNotFoundError(msg)

                case _:
                    msg = f"Unknown special path: {special_path}"
                    raise FileNotFoundError(msg)

        # Regular field/item path
        try:
            current_obj, field_name = self._get_nested_value_at_path(path)
        except FileNotFoundError:
            msg = f"Path {path} not found"
            raise FileNotFoundError(msg) from None

        if not field_name:
            # Return the object itself
            if self._is_basemodel_instance(current_obj):
                return current_obj.model_dump_json(indent=2).encode()
            return json.dumps(current_obj, indent=2, default=str).encode()

        # Get the field value
        if self._is_basemodel_instance(current_obj):
            if not hasattr(current_obj, field_name):
                msg = f"Field {field_name} not found"
                raise FileNotFoundError(msg)
            field_value = getattr(current_obj, field_name)
        elif self._is_list_like(current_obj):
            try:
                idx = int(field_name)
                field_value = current_obj[idx]
            except (ValueError, IndexError) as exc:
                msg = f"Invalid index {field_name}"
                raise FileNotFoundError(msg) from exc
        elif self._is_dict_like(current_obj):
            if field_name not in current_obj:
                msg = f"Key {field_name} not found"
                raise FileNotFoundError(msg)
            field_value = current_obj[field_name]
        else:
            msg = f"Cannot access {field_name} on {type(current_obj)}"
            raise FileNotFoundError(msg)

        # Return the field value
        if self._is_basemodel_instance(field_value):
            return field_value.model_dump_json(indent=2).encode()
        return json.dumps(field_value, indent=2, default=str).encode()

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Get detailed info about an instance field or value."""
        path = self._strip_protocol(path).strip("/")  # type: ignore

        if not path:
            # Root instance info
            return {
                "name": type(self.instance).__name__,
                "type": "instance",
                "class": f"{type(self.instance).__module__}.{type(self.instance).__name__}",  # noqa: E501
                "is_basemodel": self._is_basemodel_instance(self.instance),
                "field_count": len(type(self.instance).model_fields)
                if self._is_basemodel_instance(self.instance)
                else 0,
                "data": str(self.instance)[:200] + "..."
                if len(str(self.instance)) > 200  # noqa: PLR2004
                else str(self.instance),
            }

        try:
            current_obj, field_name = self._get_nested_value_at_path(path)
        except FileNotFoundError as exc:
            msg = f"Path {path} not found"
            raise FileNotFoundError(msg) from exc

        if not field_name:
            # Nested object info
            return {
                "name": type(current_obj).__name__,
                "type": "nested_object",
                "class": f"{type(current_obj).__module__}.{type(current_obj).__name__}",
                "is_basemodel": self._is_basemodel_instance(current_obj),
                "is_collection": self._is_list_like(current_obj)
                or self._is_dict_like(current_obj),
                "size": len(current_obj) if hasattr(current_obj, "__len__") else None,
                "data": str(current_obj)[:200] + "..."
                if len(str(current_obj)) > 200  # noqa: PLR2004
                else str(current_obj),
            }

        # Field/item value info
        if self._is_basemodel_instance(current_obj):
            if not hasattr(current_obj, field_name):
                msg = f"Field {field_name} not found"
                raise FileNotFoundError(msg)
            field_value = getattr(current_obj, field_name)
        elif self._is_list_like(current_obj):
            try:
                idx = int(field_name)
                field_value = current_obj[idx]
            except (ValueError, IndexError) as exc:
                msg = f"Invalid index {field_name}"
                raise FileNotFoundError(msg) from exc
        elif self._is_dict_like(current_obj):
            if field_name not in current_obj:
                msg = f"Key {field_name} not found"
                raise FileNotFoundError(msg)
            field_value = current_obj[field_name]
        else:
            msg = f"Cannot access {field_name} on {type(current_obj)}"
            raise FileNotFoundError(msg)

        return {
            "name": field_name,
            "type": "value",
            "value_type": type(field_value).__name__,
            "value": str(field_value)[:200] + "..."
            if len(str(field_value)) > 200  # noqa: PLR2004
            else str(field_value),
            "is_basemodel": self._is_basemodel_instance(field_value),
            "is_collection": self._is_list_like(field_value)
            or self._is_dict_like(field_value),
            "size": len(field_value) if hasattr(field_value, "__len__") else None,
        }


if __name__ == "__main__":
    # Example usage
    from pydantic import BaseModel, Field

    class Address(BaseModel):
        street: str
        city: str
        country: str = "USA"

    class User(BaseModel):
        name: str = Field(min_length=1, max_length=50)
        age: int = Field(ge=0, le=120)
        email: str
        address: Address
        tags: list[str] = []

    user = User(
        name="John Doe",
        age=30,
        email="john@example.com",
        address=Address(street="123 Main St", city="New York"),
        tags=["developer", "python"],
    )

    fs = BaseModelInstanceFS(user)
    print("Fields:", fs.ls("/", detail=False))
    print("User info:", fs.info("/"))
    print("Address info:", fs.info("/address"))
    print("Tags:", fs.cat("/tags"))
