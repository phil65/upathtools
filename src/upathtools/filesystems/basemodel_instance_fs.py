"""Filesystem implementation for browsing Pydantic BaseModel instance data."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Literal, Required, TypedDict, overload

from upathtools.filesystems.base import BaseFileSystem, BaseUPath


if TYPE_CHECKING:
    from pydantic import BaseModel


class BaseModelInstanceInfo(TypedDict, total=False):
    """Info dict for BaseModel instance paths."""

    name: Required[str]
    type: Literal["instance", "nested_object", "value", "special", "key", "item", "field"]
    class_name: str
    is_basemodel: bool
    field_count: int
    data: str
    is_collection: bool
    size: int | None
    value_type: str
    value: str
    description: str
    is_nested: bool
    index: int


class BaseModelInstancePath(BaseUPath[BaseModelInstanceInfo]):
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


class BaseModelInstanceFileSystem(BaseFileSystem[BaseModelInstancePath, BaseModelInstanceInfo]):
    """Filesystem for browsing Pydantic BaseModel instance data and values."""

    protocol = "basemodel-instance"
    upath_cls = BaseModelInstancePath

    def __init__(self, instance: BaseModel, **kwargs: Any) -> None:
        """Initialize the filesystem.

        Args:
            instance: BaseModel instance to browse
            kwargs: Additional keyword arguments for the filesystem
        """
        super().__init__(**kwargs)
        self.instance = instance

    @overload
    def ls(
        self,
        path: str,
        detail: Literal[True] = ...,
        **kwargs: Any,
    ) -> list[BaseModelInstanceInfo]: ...

    @overload
    def ls(self, path: str, detail: Literal[False], **kwargs: Any) -> list[str]: ...

    def ls(
        self,
        path: str,
        detail: bool = True,
        **kwargs: Any,
    ) -> list[BaseModelInstanceInfo] | list[str]:
        """List instance fields and values."""
        path = self._strip_protocol(path).strip("/")  # pyright: ignore[reportAttributeAccessIssue]

        try:
            current_obj, field_name = _get_nested_value_at_path(self.instance, path)
        except FileNotFoundError:
            return []

        if field_name:
            # Listing a specific field value
            if not hasattr(current_obj, field_name):
                return []

            field_value = getattr(current_obj, field_name)

            if _is_basemodel_instance(field_value):
                # BaseModel instance - show its fields
                items = list(type(field_value).model_fields.keys())
                items.extend(["__json__", "__dict__", "__schema__"])
            elif _is_list_like(field_value):
                # List-like - show indices
                items = [str(i) for i in range(len(field_value))]
                items.extend(["__json__", "__length__", "__type__"])
            elif _is_dict_like(field_value):
                # Dict-like - show keys
                items = list(field_value.keys())
                items.extend(["__json__", "__keys__", "__values__"])
            else:
                # Primitive value - show special paths
                items = ["__value__", "__type__", "__str__", "__repr__"]
        # Listing model root - show all fields plus special paths
        elif _is_basemodel_instance(current_obj):
            items = list(type(current_obj).model_fields.keys())
            items.extend(["__json__", "__dict__", "__schema__", "__model_dump__"])
        elif _is_list_like(current_obj):
            items = [str(i) for i in range(len(current_obj))]
            items.extend(["__json__", "__length__", "__type__"])
        elif _is_dict_like(current_obj):
            items = list(current_obj.keys())
            items.extend(["__json__", "__keys__", "__values__"])
        else:
            items = ["__value__", "__type__", "__str__", "__repr__"]

        if not detail:
            return items

        result = []
        for item in items:
            if item.startswith("__"):
                desc = f"Special path for {item[2:-2]} information"
                info = BaseModelInstanceInfo(name=item, type="special", size=0, description=desc)
                result.append(info)
            # It's a field or item
            elif _is_basemodel_instance(current_obj):
                field_value = getattr(current_obj, item)
                result.append({
                    "name": item,
                    "type": "field",
                    "value_type": type(field_value).__name__,
                    "value": str(field_value)[:100] + "..."
                    if len(str(field_value)) > 100  # noqa: PLR2004
                    else str(field_value),
                    "is_nested": _is_basemodel_instance(field_value),
                    "is_collection": _is_list_like(field_value) or _is_dict_like(field_value),
                })
            elif _is_list_like(current_obj):
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
                    "is_nested": _is_basemodel_instance(item_value),
                })
            elif _is_dict_like(current_obj):
                dict_value = current_obj[item]
                result.append(
                    BaseModelInstanceInfo(
                        name=item,
                        type="key",
                        value_type=type(dict_value).__name__,
                        value=str(dict_value)[:100] + "..."
                        if len(str(dict_value)) > 100  # noqa: PLR2004
                        else str(dict_value),
                        is_nested=_is_basemodel_instance(dict_value),
                    )
                )

        return result

    def isdir(self, path: str) -> bool:
        """Check if path is a directory (BaseModel instance, list, or dict)."""
        path = self._strip_protocol(path).strip("/")  # pyright: ignore[reportAttributeAccessIssue]

        if not path:
            # Root is always a directory
            return True

        try:
            current_obj, field_name = _get_nested_value_at_path(self.instance, path)
        except FileNotFoundError:
            return False

        if field_name:
            # Check if the field value is navigable
            if not hasattr(current_obj, field_name):
                return False
            field_value = getattr(current_obj, field_name)
            return (
                _is_basemodel_instance(field_value)
                or _is_list_like(field_value)
                or _is_dict_like(field_value)
            )

        # current_obj itself - check if it's navigable
        return (
            _is_basemodel_instance(current_obj)
            or _is_list_like(current_obj)
            or _is_dict_like(current_obj)
        )

    def cat(self, path: str = "") -> bytes:  # noqa: PLR0911
        """Get field values, JSON representation, or other information."""
        path = self._strip_protocol(path).strip("/")  # pyright: ignore[reportAttributeAccessIssue]

        if not path:
            # Return instance JSON
            if _is_basemodel_instance(self.instance):
                return self.instance.model_dump_json(indent=2).encode()
            return json.dumps(self.instance, indent=2, default=str).encode()

        parts = path.split("/")

        # Handle special paths
        if parts[-1].startswith("__") and parts[-1].endswith("__"):
            special_path = parts[-1]
            field_path = "/".join(parts[:-1])

            try:
                current_obj, field_name = _get_nested_value_at_path(self.instance, field_path)
            except FileNotFoundError:
                raise FileNotFoundError(f"Path {field_path} not found") from None

            target_obj = getattr(current_obj, field_name) if field_name else current_obj

            match special_path:
                case "__json__":
                    if _is_basemodel_instance(target_obj):
                        return target_obj.model_dump_json(indent=2).encode()
                    return json.dumps(target_obj, indent=2, default=str).encode()

                case "__dict__":
                    if _is_basemodel_instance(target_obj):
                        return json.dumps(target_obj.model_dump(), indent=2).encode()
                    if hasattr(target_obj, "__dict__"):
                        return json.dumps(target_obj.__dict__, indent=2, default=str).encode()
                    return json.dumps(dict(target_obj), indent=2, default=str).encode()

                case "__schema__":
                    if _is_basemodel_instance(target_obj):
                        return json.dumps(target_obj.model_json_schema(), indent=2).encode()

                    raise FileNotFoundError("Schema only available for BaseModel instances")

                case "__model_dump__":
                    if _is_basemodel_instance(target_obj):
                        return json.dumps(target_obj.model_dump(), indent=2).encode()

                    raise FileNotFoundError("model_dump only available for BaseModel instances")

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

                    raise FileNotFoundError(f"Length not available for {type(target_obj)}")

                case "__keys__":
                    if _is_dict_like(target_obj):
                        return json.dumps(list(target_obj.keys()), indent=2).encode()

                    raise FileNotFoundError("Keys only available for dict-like objects")

                case "__values__":
                    if _is_dict_like(target_obj):
                        return json.dumps(list(target_obj.values()), indent=2, default=str).encode()

                    raise FileNotFoundError("Values only available for dict-like objects")

                case _:
                    raise FileNotFoundError(f"Unknown special path: {special_path}")

        # Regular field/item path
        try:
            current_obj, field_name = _get_nested_value_at_path(self.instance, path)
        except FileNotFoundError:
            raise FileNotFoundError(f"Path {path} not found") from None

        if not field_name:
            # Return the object itself
            if _is_basemodel_instance(current_obj):
                return current_obj.model_dump_json(indent=2).encode()
            return json.dumps(current_obj, indent=2, default=str).encode()

        # Get the field value
        if _is_basemodel_instance(current_obj):
            if not hasattr(current_obj, field_name):
                raise FileNotFoundError(f"Field {field_name} not found")
            field_value = getattr(current_obj, field_name)
        elif _is_list_like(current_obj):
            try:
                idx = int(field_name)
                field_value = current_obj[idx]
            except (ValueError, IndexError) as exc:
                raise FileNotFoundError(f"Invalid index {field_name}") from exc
        elif _is_dict_like(current_obj):
            if field_name not in current_obj:
                raise FileNotFoundError(f"Key {field_name} not found")
            field_value = current_obj[field_name]
        else:
            raise FileNotFoundError(f"Cannot access {field_name} on {type(current_obj)}")

        # Return the field value
        if _is_basemodel_instance(field_value):
            return field_value.model_dump_json(indent=2).encode()
        return json.dumps(field_value, indent=2, default=str).encode()

    def info(self, path: str, **kwargs: Any) -> BaseModelInstanceInfo:
        """Get detailed info about an instance field or value."""
        path = self._strip_protocol(path).strip("/")  # pyright: ignore[reportAttributeAccessIssue]

        if not path:
            # Root instance info
            return BaseModelInstanceInfo(
                name=type(self.instance).__name__,
                type="instance",
                class_name=f"{type(self.instance).__module__}.{type(self.instance).__name__}",
                is_basemodel=_is_basemodel_instance(self.instance),
                field_count=len(type(self.instance).model_fields)
                if _is_basemodel_instance(self.instance)
                else 0,
                data=str(self.instance)[:200] + "..."
                if len(str(self.instance)) > 200  # noqa: PLR2004
                else str(self.instance),
            )

        try:
            current_obj, field_name = _get_nested_value_at_path(self.instance, path)
        except FileNotFoundError as exc:
            raise FileNotFoundError(f"Path {path} not found") from exc

        if not field_name:
            # Nested object info
            return BaseModelInstanceInfo(
                name=type(current_obj).__name__,
                type="nested_object",
                class_name=f"{type(current_obj).__module__}.{type(current_obj).__name__}",
                is_basemodel=_is_basemodel_instance(current_obj),
                is_collection=_is_list_like(current_obj) or _is_dict_like(current_obj),
                size=len(current_obj) if hasattr(current_obj, "__len__") else None,
                data=str(current_obj)[:200] + "..."
                if len(str(current_obj)) > 200  # noqa: PLR2004
                else str(current_obj),
            )

        # Field/item value info
        if _is_basemodel_instance(current_obj):
            if not hasattr(current_obj, field_name):
                raise FileNotFoundError(f"Field {field_name} not found")
            field_value = getattr(current_obj, field_name)
        elif _is_list_like(current_obj):
            try:
                idx = int(field_name)
                field_value = current_obj[idx]
            except (ValueError, IndexError) as exc:
                raise FileNotFoundError(f"Invalid index {field_name}") from exc
        elif _is_dict_like(current_obj):
            if field_name not in current_obj:
                raise FileNotFoundError(f"Key {field_name} not found")
            field_value = current_obj[field_name]
        else:
            raise FileNotFoundError(f"Cannot access {field_name} on {type(current_obj)}")

        return BaseModelInstanceInfo(
            name=field_name,
            type="value",
            value_type=type(field_value).__name__,
            value=str(field_value)[:200] + "..."
            if len(str(field_value)) > 200  # noqa: PLR2004
            else str(field_value),
            is_basemodel=_is_basemodel_instance(field_value),
            is_collection=_is_list_like(field_value) or _is_dict_like(field_value),
            size=len(field_value) if hasattr(field_value, "__len__") else None,
        )


def _is_basemodel_instance(obj: Any) -> bool:
    """Check if object is a BaseModel instance."""
    return hasattr(type(obj), "model_fields") and hasattr(obj, "model_dump")


def _is_list_like(obj: Any) -> bool:
    """Check if object is list-like."""
    return isinstance(obj, (list, tuple, set))


def _is_dict_like(obj: Any) -> bool:
    """Check if object is dict-like."""
    return isinstance(obj, dict)


def _get_nested_value_at_path(instance: BaseModel, path: str) -> tuple[Any, str]:
    """Get the object and field name at a given path."""
    if not path:
        return instance, ""

    parts = path.strip("/").split("/")
    current_obj = instance

    for i, part in enumerate(parts[:-1]):
        if part.startswith("__") and part.endswith("__"):
            # Skip special paths like __json__, __dict__
            continue

        if not hasattr(current_obj, part):
            raise FileNotFoundError(f"Field {part} not found in {type(current_obj).__name__}")

        current_obj = getattr(current_obj, part)

        # Handle list/dict access with numeric indices
        if isinstance(current_obj, (list, tuple)) and i + 1 < len(parts) - 1:
            next_part = parts[i + 1]
            if next_part.isdigit():
                idx = int(next_part)
                if idx >= len(current_obj):
                    raise FileNotFoundError(f"Index {idx} out of range for {part}")
                current_obj = current_obj[idx]
                parts.pop(i + 1)  # Remove the index from parts

    return current_obj, parts[-1] if parts else ""


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

    fs = BaseModelInstanceFileSystem(user)
    print("Fields:", fs.ls("/", detail=False))
    print("User info:", fs.info("/"))
    print("Address info:", fs.info("/address"))
    print("Tags:", fs.cat("/tags"))
