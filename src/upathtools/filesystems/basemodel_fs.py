"""Filesystem implementation for browsing Pydantic BaseModel schemas."""

from __future__ import annotations

import importlib
import json
from typing import TYPE_CHECKING, Any, Literal, TypedDict, get_args, get_origin, overload

from upathtools import core
from upathtools.filesystems.base import BaseFileSystem, BaseUPath


if TYPE_CHECKING:
    from pydantic import BaseModel


class BaseModelInfo(TypedDict, total=False):
    """Info dict for BaseModel paths."""

    name: str
    type: Literal["model", "nested_model", "field", "special"]
    size: int
    module: str | None
    doc: str | None
    field_count: int | None
    schema: dict[str, Any] | None
    annotation: str | None
    required: bool | None
    default: Any
    alias: str | None
    description: str | None
    constraints: list[str] | None
    field_type: str
    nested_model: bool


class BaseModelPath(BaseUPath[BaseModelInfo]):
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


class BaseModelFileSystem(BaseFileSystem[BaseModelPath, BaseModelInfo]):
    """Filesystem for browsing Pydantic BaseModel schemas and field definitions."""

    protocol = "basemodel"
    upath_cls = BaseModelPath

    def __init__(self, model: type[BaseModel] | str, **kwargs: Any) -> None:
        """Initialize the filesystem.

        Args:
            model: BaseModel class or import path (e.g., "mypackage.MyModel")
            kwargs: Additional keyword arguments for the filesystem
        """
        super().__init__(**kwargs)

        if isinstance(model, str):
            self.model_class = _import_model(model)
            self.model_path = model
        else:
            self.model_class = model
            self.model_path = f"{model.__module__}.{model.__name__}"

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, Any]:
        path = path.removeprefix("basemodel://")
        return {"model": path}

    @classmethod
    def _strip_protocol(cls, path):
        """Override to handle model name in URL by treating it as root path."""
        stripped = super()._strip_protocol(path)
        # If the stripped path equals the model identifier, treat it as root
        # This handles URLs like basemodel://schemez.Schema where schemez.Schema
        # should be treated as the root path "/" for the model filesystem
        if stripped and "/" not in stripped and "." in stripped:
            # This looks like a model identifier (e.g., "schemez.Schema")
            return ""
        return stripped

    def _get_nested_model_at_path(self, path: str) -> tuple[type[BaseModel], str]:
        """Get the model class and field name at a given path."""
        if not path:
            return self.model_class, ""

        parts = path.strip("/").split("/")
        current_model: Any = self.model_class

        for _i, part in enumerate(parts[:-1]):
            if part.startswith("__") and part.endswith("__"):
                # Skip special paths like __schema__, __examples__
                continue
            assert current_model
            if part not in current_model.model_fields:
                msg = f"Field {part} not found in {current_model.__name__}"
                raise FileNotFoundError(msg)

            field_info = current_model.model_fields[part]
            field_type = field_info.annotation

            # Handle Optional, List, etc.
            origin = get_origin(field_type)
            if origin is not None:
                args = get_args(field_type)
                if origin in (list, tuple, set) and args:
                    field_type = args[0]
                elif hasattr(field_type, "__args__") and len(field_type.__args__) >= 1:  # type: ignore
                    # Handle Union/Optional types
                    field_type = next(
                        arg
                        for arg in field_type.__args__  # type: ignore
                        if arg is not type(None) and hasattr(arg, "model_fields")
                    )

            if not hasattr(field_type, "model_fields"):
                msg = f"Field {part} is not a nested BaseModel"
                raise FileNotFoundError(msg)

            current_model = field_type

        return current_model, parts[-1] if parts else ""

    @overload
    def ls(self, path: str, detail: Literal[True] = True, **kwargs: Any) -> list[BaseModelInfo]: ...

    @overload
    def ls(self, path: str, detail: Literal[False] = False, **kwargs: Any) -> list[str]: ...

    def ls(self, path: str, detail: bool = True, **kwargs: Any) -> list[BaseModelInfo] | list[str]:
        """List model fields and special paths."""
        path = self._strip_protocol(path).strip("/")  # pyright: ignore[reportAttributeAccessIssue]

        try:
            current_model, field_name = self._get_nested_model_at_path(path)
        except FileNotFoundError:
            return []

        if field_name and field_name in current_model.model_fields:
            field_info = current_model.model_fields[field_name]
            field_type = field_info.annotation

            # Unwrap Optional, List, etc.
            origin = get_origin(field_type)
            if origin is not None:
                args = get_args(field_type)
                if origin in (list, tuple, set) and args:
                    field_type = args[0]
                elif hasattr(field_type, "__args__"):
                    field_type = next(
                        (
                            arg
                            for arg in field_type.__args__  # type: ignore
                            if arg is not type(None) and hasattr(arg, "model_fields")
                        ),
                        field_type,
                    )

            # If it's a nested model, list its fields
            if hasattr(field_type, "model_fields"):
                current_model = field_type  # type: ignore[assignment]
                field_name = ""  # Now treat it as listing the nested model
            else:
                # It's a regular field - show special paths
                items = ["__schema__", "__type__", "__constraints__"]
                if field_info.default is not ...:
                    items.append("__default__")
                if field_info.alias:
                    items.append("__alias__")

        if not field_name:
            # Listing model root - show all fields plus special paths
            items = list(current_model.model_fields.keys())
            items.extend(["__schema__", "__fields_info__", "__model_config__"])

        if not detail:
            return items

        result = []
        for item in items:
            if item.startswith("__"):
                desc = f"Special path for {item[2:-2]} information"
                result.append(BaseModelInfo(name=item, type="special", size=0, description=desc))
            else:
                # It's a field
                field_info = current_model.model_fields[item]
                field_type = field_info.annotation
                # Determine if field is a nested model
                is_nested = False
                origin = get_origin(field_type)
                if origin is not None:
                    args = get_args(field_type)
                    if origin in (list, tuple, set) and args:
                        field_type = args[0]
                    elif hasattr(field_type, "__args__"):
                        # Check if any type in Union is a BaseModel
                        for arg in field_type.__args__:  # type: ignore
                            if arg != type(None) and hasattr(arg, "model_fields"):  # noqa: E721
                                is_nested = True
                                break
                else:
                    is_nested = hasattr(field_type, "model_fields")

                result.append(
                    BaseModelInfo(
                        name=item,
                        type="field",
                        field_type=str(field_type),
                        required=field_info.is_required(),
                        default=str(field_info.default) if field_info.default is not ... else None,
                        alias=field_info.alias,
                        nested_model=is_nested,
                        description=field_info.description,
                    )
                )

        return result

    def cat(self, path: str = "") -> bytes:  # noqa: PLR0911
        """Get field definition, schema, or other information."""
        path = self._strip_protocol(path).strip("/")  # pyright: ignore[reportAttributeAccessIssue]

        if not path:
            # Return model schema
            schema = self.model_class.model_json_schema()
            return json.dumps(schema, indent=2).encode()

        parts = path.split("/")

        # Handle special paths
        if parts[-1].startswith("__") and parts[-1].endswith("__"):
            special_path = parts[-1]
            field_path = "/".join(parts[:-1])

            try:
                current_model, field_name = self._get_nested_model_at_path(field_path)
            except FileNotFoundError:
                msg = f"Path {field_path} not found"
                raise FileNotFoundError(msg) from None

            match special_path:
                case "__schema__":
                    if field_name:
                        # Field schema
                        field_info = current_model.model_fields[field_name]
                        field_schema = {
                            "type": str(field_info.annotation),
                            "required": field_info.is_required(),
                            "default": field_info.default
                            if field_info.default is not ...
                            else None,
                            "alias": field_info.alias,
                            "description": field_info.description,
                            "constraints": [str(constraint) for constraint in field_info.metadata]
                            if field_info.metadata
                            else [],
                        }
                        return json.dumps(field_schema, indent=2, default=str).encode()
                    # Model schema
                    schema = current_model.model_json_schema()
                    return json.dumps(schema, indent=2).encode()

                case "__fields_info__":
                    fields_info = {
                        name: {
                            "type": str(field.annotation),
                            "required": field.is_required(),
                            "default": field.default if field.default is not ... else None,
                            "alias": field.alias,
                        }
                        for name, field in current_model.model_fields.items()
                    }
                    return json.dumps(fields_info, indent=2, default=str).encode()

                case "__model_config__":
                    config = getattr(current_model, "model_config", {})
                    return json.dumps(dict(config), indent=2, default=str).encode()

                case "__type__":
                    if not field_name:
                        msg = "__type__ only available for fields"
                        raise FileNotFoundError(msg)
                    field_info = current_model.model_fields[field_name]
                    return str(field_info.annotation).encode()

                case "__constraints__":
                    if not field_name:
                        msg = "__constraints__ only available for fields"
                        raise FileNotFoundError(msg)
                    field_info = current_model.model_fields[field_name]
                    constraints = field_info.metadata if field_info.metadata else []
                    return json.dumps([str(c) for c in constraints], indent=2).encode()

                case "__default__":
                    if not field_name:
                        msg = "__default__ only available for fields"
                        raise FileNotFoundError(msg)
                    field_info = current_model.model_fields[field_name]
                    if field_info.default is ...:
                        msg = f"Field {field_name} has no default value"
                        raise FileNotFoundError(msg)
                    return str(field_info.default).encode()

                case "__alias__":
                    if not field_name:
                        msg = "__alias__ only available for fields"
                        raise FileNotFoundError(msg)
                    field_info = current_model.model_fields[field_name]
                    if not field_info.alias:
                        msg = f"Field {field_name} has no alias"
                        raise FileNotFoundError(msg)
                    return field_info.alias.encode()

                case _:
                    msg = f"Unknown special path: {special_path}"
                    raise FileNotFoundError(msg)

        # Regular field path
        try:
            current_model, field_name = self._get_nested_model_at_path(path)
        except FileNotFoundError:
            msg = f"Field {path} not found"
            raise FileNotFoundError(msg) from None

        if not field_name:
            msg = f"Path {path} is not a field"
            raise FileNotFoundError(msg)

        if field_name not in current_model.model_fields:
            msg = f"Field {field_name} not found"
            raise FileNotFoundError(msg)

        # Return field information
        field_info = current_model.model_fields[field_name]
        field_data = {
            "name": field_name,
            "type": str(field_info.annotation),
            "required": field_info.is_required(),
            "default": field_info.default if field_info.default is not ... else None,
            "alias": field_info.alias,
            "description": field_info.description,
            "constraints": [str(c) for c in field_info.metadata] if field_info.metadata else [],
        }

        return json.dumps(field_data, indent=2, default=str).encode()

    def isdir(self, path: str) -> bool:
        """Check if path is a directory (model or nested model field)."""
        path = self._strip_protocol(path).strip("/")  # pyright: ignore[reportAttributeAccessIssue]

        if not path:
            # Root is always a directory
            return True

        try:
            current_model, field_name = self._get_nested_model_at_path(path)
            if not field_name:
                # It's a model path (not a field)
                return True
            # Check if the field is a nested model
            if field_name in current_model.model_fields:
                field_info = current_model.model_fields[field_name]
                field_type = field_info.annotation
                origin = get_origin(field_type)
                if origin is not None:
                    args = get_args(field_type)
                    if origin in (list, tuple, set) and args:
                        field_type = args[0]
                    elif hasattr(field_type, "__args__"):
                        for arg in field_type.__args__:  # type: ignore
                            if arg is not type(None) and hasattr(arg, "model_fields"):
                                return True
                return hasattr(field_type, "model_fields")
        except FileNotFoundError:
            return False
        else:
            return False

    def info(self, path: str, **kwargs: Any) -> BaseModelInfo:
        """Get detailed info about a model or field."""
        path = self._strip_protocol(path).strip("/")  # pyright: ignore[reportAttributeAccessIssue]

        if not path:
            # Root model info
            return BaseModelInfo(
                name=self.model_class.__name__,
                type="model",
                module=self.model_class.__module__,
                doc=self.model_class.__doc__,
                field_count=len(self.model_class.model_fields),
                schema=self.model_class.model_json_schema(),
                size=len(self.model_class.model_fields),
            )

        try:
            current_model, field_name = self._get_nested_model_at_path(path)
        except FileNotFoundError as exc:
            msg = f"Path {path} not found"
            raise FileNotFoundError(msg) from exc

        if not field_name:
            # Nested model info
            return BaseModelInfo(
                name=current_model.__name__,
                type="nested_model",
                module=current_model.__module__,
                doc=current_model.__doc__,
                field_count=len(current_model.model_fields),
                size=len(current_model.model_fields),
            )

        # Field info
        if field_name not in current_model.model_fields:
            msg = f"Field {field_name} not found"
            raise FileNotFoundError(msg)

        field_info = current_model.model_fields[field_name]
        return BaseModelInfo(
            name=field_name,
            type="field",
            annotation=str(field_info.annotation),
            required=field_info.is_required(),
            default=field_info.default if field_info.default is not ... else None,
            alias=field_info.alias,
            description=field_info.description,
            size=0,
            constraints=[str(c) for c in field_info.metadata] if field_info.metadata else None,
        )


def _import_model(import_path: str) -> type[BaseModel]:
    """Import a BaseModel class from a string path."""
    try:
        module_path, class_name = import_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        model_class = getattr(module, class_name)

        # Basic check if it's a BaseModel subclass
        if not hasattr(model_class, "model_fields"):
            msg = f"{import_path} is not a Pydantic BaseModel"
            raise ValueError(msg)  # noqa: TRY301

    except (ImportError, AttributeError, ValueError) as exc:
        msg = f"Could not import BaseModel from {import_path}"
        raise FileNotFoundError(msg) from exc
    else:
        return model_class


if __name__ == "__main__":
    from pydantic import BaseModel, Field
    import upath

    class User(BaseModel):
        name: str = Field(min_length=1, max_length=50)
        age: int = Field(ge=0, le=120)
        email: str

    fs = BaseModelFileSystem(User)
    print("Fields:", fs.ls("/", detail=False))
    print("User info:", fs.info("/"))
    print("Name field:", fs.info("/name"))
    # Test with UPath using explicit storage options
    path = upath.UPath("/", protocol="basemodel", model="schemez.Schema")
    print("UPath with explicit options:", path)
    print("Storage options:", path.storage_options)
    print("Fields:", list(path.iterdir())[:5])
    # Test the original failing URL syntax
    path = upath.UPath("basemodel://schemez.Schema")
    print("Original URL syntax works:", path)
    print("Storage options:", path.storage_options)
    print("Fields:", list(path.iterdir())[:5])
    # Test fsspec directly
    fs, parsed_path = core.url_to_fs("basemodel://schemez.Schema")
    print("fsspec works - parsed path:", parsed_path)
    print("Filesystem fields:", fs.ls("/", detail=False)[:5])
