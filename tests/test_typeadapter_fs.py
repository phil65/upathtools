"""Tests for BaseModelFileSystem with TypeAdapter support."""

from __future__ import annotations

import dataclasses
import json
from typing import TypedDict

import fsspec
from pydantic import BaseModel
import pytest


class User(BaseModel):
    """A Pydantic BaseModel user."""

    id: int
    name: str = "John Doe"
    email: str | None = None
    age: int | None = None


@dataclasses.dataclass
class DataUser:
    """A dataclass user."""

    id: int
    name: str = "Jane Doe"
    email: str | None = None
    age: int | None = None


class TypedUser(TypedDict):
    """A TypedDict user."""

    id: int
    name: str
    email: str | None
    age: int | None


def test_typeadapter_fs_with_pydantic_model():
    """Test BaseModelFileSystem with a Pydantic BaseModel."""
    fs = fsspec.filesystem("typeadapter", model=User)

    # Test root listing
    fields = fs.ls("/", detail=False)
    assert "id" in fields
    assert "name" in fields
    assert "email" in fields
    assert "age" in fields
    assert "__schema__" in fields

    # Test detailed listing
    detailed = fs.ls("/", detail=True)
    id_field = next(f for f in detailed if f["name"] == "id")
    assert id_field["type"] == "field"
    assert id_field["field_type"] == "integer"

    # Test schema access
    schema = fs.cat("/__schema__").decode()
    schema_data = json.loads(schema)
    assert "properties" in schema_data
    assert "id" in schema_data["properties"]


def test_typeadapter_fs_with_dataclass():
    """Test BaseModelFileSystem with a dataclass using TypeAdapter."""
    fs = fsspec.filesystem("typeadapter", model=DataUser)

    # Test root listing
    fields = fs.ls("/", detail=False)
    assert "id" in fields
    assert "name" in fields
    assert "email" in fields
    assert "age" in fields
    assert "__schema__" in fields

    # Test field access
    id_schema = fs.cat("/id").decode()
    id_data = json.loads(id_schema)
    assert id_data["type"] == "integer"

    # Test schema access
    schema = fs.cat("/__schema__").decode()
    schema_data = json.loads(schema)
    assert "properties" in schema_data
    assert "id" in schema_data["properties"]


def test_typeadapter_fs_with_typeddict():
    """Test BaseModelFileSystem with a TypedDict using TypeAdapter."""
    fs = fsspec.filesystem("typeadapter", model=TypedUser)

    # Test root listing
    fields = fs.ls("/", detail=False)
    assert "id" in fields
    assert "name" in fields
    assert "email" in fields
    assert "age" in fields
    assert "__schema__" in fields

    # Test field access
    id_schema = fs.cat("/id").decode()
    id_data = json.loads(id_schema)
    assert id_data["type"] == "integer"

    # Test schema access
    schema = fs.cat("/__schema__").decode()
    schema_data = json.loads(schema)
    assert "properties" in schema_data
    assert "id" in schema_data["properties"]


def test_typeadapter_fs_field_operations():
    """Test field-specific operations."""
    fs = fsspec.filesystem("typeadapter", model=User)

    # Test field type
    field_type = fs.cat("/name/__type__").decode()
    assert field_type == "string"

    # Test field default
    default_value = fs.cat("/name/__default__").decode()
    default_data = json.loads(default_value)
    assert default_data == "John Doe"

    # Test field without default raises error
    with pytest.raises(FileNotFoundError, match="has no default value"):
        fs.cat("/id/__default__")


def test_typeadapter_fs_special_paths():
    """Test special paths and operations."""
    fs = fsspec.filesystem("typeadapter", model=User)

    # Test examples generation
    examples = fs.cat("/__examples__").decode()
    examples_data = json.loads(examples)
    assert "id" in examples_data
    assert "name" in examples_data

    # Test field listing with detail
    name_details = fs.ls("/name", detail=True)
    assert any(item["name"] == "__type__" for item in name_details)
    assert any(item["name"] == "__default__" for item in name_details)


def test_typeadapter_fs_info_method():
    """Test info method for various paths."""
    fs = fsspec.filesystem("typeadapter", model=User)

    # Root info
    root_info = fs.info("/")
    assert root_info["type"] == "model"
    assert "field_count" in root_info
    assert root_info["field_count"] == 4  # id, name, email, age  # noqa: PLR2004

    # Field info
    field_info = fs.info("/name")
    assert field_info["type"] == "field"
    assert field_info["field_type"] == "string"
    assert field_info["name"] == "name"


def test_typeadapter_fs_string_import():
    """Test importing model from string path."""
    fs = fsspec.filesystem("typeadapter", model="tests.test_typeadapter_fs.User")

    fields = fs.ls("/", detail=False)
    assert "id" in fields
    assert "name" in fields


def test_typeadapter_fs_url_parsing():
    """Test URL parsing functionality."""
    from upathtools.filesystems import TypeAdapterFileSystem

    kwargs = TypeAdapterFileSystem._get_kwargs_from_urls("typeadapter://mypackage.MyModel")
    assert kwargs == {"model": "mypackage.MyModel"}


def test_typeadapter_fs_isdir():
    """Test directory checking."""
    fs = fsspec.filesystem("typeadapter", model=User)

    # Root should be directory
    assert fs.isdir("/")

    # Fields should not be directories
    assert not fs.isdir("/name")
    assert not fs.isdir("/id")

    # Non-existent paths should not be directories
    assert not fs.isdir("/nonexistent")


def test_typeadapter_fs_file_not_found_errors():
    """Test proper FileNotFoundError handling."""
    fs = fsspec.filesystem("typeadapter", model=User)

    # Non-existent field
    with pytest.raises(FileNotFoundError, match="not found"):
        fs.cat("/nonexistent")

    # Non-existent special path
    with pytest.raises(FileNotFoundError, match="Unknown special path"):
        fs.cat("/name/__nonexistent__")


if __name__ == "__main__":
    pytest.main(["-vv", __file__])
