"""Tests for JsonSchemaFileSystem.from_type() with various Python types."""

from __future__ import annotations

import dataclasses
import json
from typing import TypedDict

from pydantic import BaseModel
import pytest

from upathtools.filesystems.file_filesystems.jsonschema_fs import JsonSchemaFileSystem


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


def test_from_type_with_pydantic_model():
    """Test from_type with a Pydantic BaseModel."""
    fs = JsonSchemaFileSystem.from_type(User)

    # Test root listing
    root_items = fs.ls("/", detail=False)
    assert "properties" in root_items
    assert "$defs" in root_items or "__meta__" in root_items

    # Test properties listing
    fields = fs.ls("/properties", detail=False)
    assert "id" in fields
    assert "name" in fields
    assert "email" in fields
    assert "age" in fields

    # Test detailed listing
    detailed = fs.ls("/properties", detail=True)
    id_field = next(f for f in detailed if f["name"] == "id")
    assert id_field.get("schema_type") == "integer"

    # Test schema access
    schema = fs.cat("/__raw__").decode()
    schema_data = json.loads(schema)
    assert "properties" in schema_data
    assert "id" in schema_data["properties"]


def test_from_type_with_dataclass():
    """Test from_type with a dataclass."""
    fs = JsonSchemaFileSystem.from_type(DataUser)
    # Test properties listing
    fields = fs.ls("/properties", detail=False)
    assert "id" in fields
    assert "name" in fields
    assert "email" in fields
    assert "age" in fields
    # Test field schema access
    id_schema = fs.cat("/properties/id/__schema__").decode()
    id_data = json.loads(id_schema)
    assert id_data["type"] == "integer"


def test_from_type_with_typeddict():
    """Test from_type with a TypedDict."""
    fs = JsonSchemaFileSystem.from_type(TypedUser)

    # Test properties listing
    fields = fs.ls("/properties", detail=False)
    assert "id" in fields
    assert "name" in fields
    assert "email" in fields
    assert "age" in fields

    # Test field schema access
    id_schema = fs.cat("/properties/id/__schema__").decode()
    id_data = json.loads(id_schema)
    assert id_data["type"] == "integer"


def test_from_type_field_operations():
    """Test field-specific operations."""
    fs = JsonSchemaFileSystem.from_type(User)

    # Test field schema
    name_schema = fs.cat("/properties/name/__schema__").decode()
    name_data = json.loads(name_schema)
    assert name_data["type"] == "string"
    assert name_data.get("default") == "John Doe"


def test_from_type_info_method():
    """Test info method for various paths."""
    fs = JsonSchemaFileSystem.from_type(User)

    # Root info
    root_info = fs.info("/")
    assert root_info["type"] == "directory"

    # Properties - check it exists and has content
    props_listing = fs.ls("/properties", detail=False)
    # id, name, email, age
    assert len(props_listing) >= 4  # noqa: PLR2004


def test_from_type_string_import():
    """Test importing model from string path."""
    fs = JsonSchemaFileSystem.from_type("tests.test_jsonschema_from_type.User")

    fields = fs.ls("/properties", detail=False)
    assert "id" in fields
    assert "name" in fields


def test_from_type_isdir():
    """Test directory checking."""
    fs = JsonSchemaFileSystem.from_type(User)

    # Root should be directory
    assert fs.isdir("/")

    # Properties should be directory
    assert fs.isdir("/properties")

    # Individual fields are typically files (no nested properties)
    assert not fs.isdir("/properties/name")

    # Non-existent paths should not be directories
    assert not fs.isdir("/nonexistent")


def test_from_type_file_not_found_errors():
    """Test proper FileNotFoundError handling."""
    fs = JsonSchemaFileSystem.from_type(User)

    # Non-existent property
    with pytest.raises(FileNotFoundError):
        fs.cat("/properties/nonexistent/__schema__")


def test_from_type_resolve_refs_default():
    """Test that resolve_refs defaults to True for from_type."""
    fs = JsonSchemaFileSystem.from_type(User)
    assert fs.resolve_refs is True


def test_from_type_resolve_refs_disabled():
    """Test from_type with resolve_refs disabled."""
    fs = JsonSchemaFileSystem.from_type(User, resolve_refs=False)
    assert fs.resolve_refs is False


class Address(BaseModel):
    """Nested model for testing ref resolution."""

    street: str
    city: str


class UserWithAddress(BaseModel):
    """User with nested address for testing ref resolution."""

    name: str
    addresses: list[Address] = []


def test_from_type_with_nested_model_resolve_refs():
    """Test ref resolution with nested models."""
    fs = JsonSchemaFileSystem.from_type(UserWithAddress, resolve_refs=True)

    # Navigate into array items - should resolve the $ref
    items_contents = fs.ls("/properties/addresses/items", detail=False)
    assert "properties" in items_contents

    # Should be able to see Address fields through the resolved ref
    address_fields = fs.ls("/properties/addresses/items/properties", detail=False)
    assert "street" in address_fields
    assert "city" in address_fields


def test_from_type_with_nested_model_no_resolve():
    """Test without ref resolution - refs stay as-is."""
    fs = JsonSchemaFileSystem.from_type(UserWithAddress, resolve_refs=False)

    # Without resolution, items just shows __schema__
    items_contents = fs.ls("/properties/addresses/items", detail=False)
    # The $ref is not resolved, so no properties are shown directly
    assert "properties" not in items_contents


def test_from_type_defs_access():
    """Test that $defs are still accessible separately."""
    fs = JsonSchemaFileSystem.from_type(UserWithAddress)

    # $defs should contain Address
    defs = fs.ls("/$defs", detail=False)
    assert "Address" in defs

    # Can browse Address definition directly
    address_props = fs.ls("/$defs/Address/properties", detail=False)
    assert "street" in address_props
    assert "city" in address_props


if __name__ == "__main__":
    pytest.main(["-vv", __file__])
