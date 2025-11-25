"""Test OpenAPIFS functionality."""

from __future__ import annotations

import json

import pytest

from upathtools.filesystems import OpenAPIFS


openapi3 = pytest.importorskip("openapi3")
requests = pytest.importorskip("requests")


@pytest.fixture
def minimal_spec():
    """Create a minimal OpenAPI spec for testing."""
    return {
        "openapi": "3.0.1",
        "info": {"title": "Test API", "version": "1.0.0", "description": "A test API"},
        "paths": {
            "/users": {
                "get": {
                    "summary": "List users",
                    "operationId": "listUsers",
                    "responses": {"200": {"description": "Success"}},
                },
                "post": {
                    "summary": "Create user",
                    "operationId": "createUser",
                    "responses": {"201": {"description": "Created"}},
                },
            },
            "/users/{id}": {
                "get": {
                    "summary": "Get user",
                    "operationId": "getUser",
                    "parameters": [
                        {
                            "name": "id",
                            "in": "path",
                            "required": True,
                            "schema": {"type": "integer"},
                        }
                    ],
                    "responses": {
                        "200": {"description": "Success"},
                        "404": {"description": "Not found"},
                    },
                }
            },
        },
        "components": {
            "schemas": {
                "User": {
                    "type": "object",
                    "properties": {"id": {"type": "integer"}, "name": {"type": "string"}},
                }
            }
        },
    }


@pytest.fixture
def spec_file(tmp_path, minimal_spec):
    """Create temporary spec file."""
    spec_path = tmp_path / "openapi.json"
    with spec_path.open("w") as f:
        json.dump(minimal_spec, f, indent=2)
    return str(spec_path)


def test_openapi_fs_init_requires_url():
    """Test that spec_url is required."""
    with pytest.raises(ValueError, match="OpenAPI spec URL required"):
        OpenAPIFS()


def test_openapi_fs_init_local_file(spec_file):
    """Test initializing OpenAPIFS with local file."""
    fs = OpenAPIFS(spec_file)
    assert fs.spec_url == spec_file
    assert fs._spec is None  # Not loaded yet


def test_openapi_fs_root_listing(spec_file):
    """Test listing root sections."""
    fs = OpenAPIFS(spec_file)

    # Test simple listing
    sections = fs.ls("/", detail=False)
    assert "info" in sections
    assert "paths" in sections
    assert "components" in sections

    # Test detailed listing
    detailed = fs.ls("/", detail=True)
    assert len(detailed) == len(sections)
    assert all("type" in item for item in detailed)


def test_openapi_fs_info_section(spec_file):
    """Test info section browsing."""
    fs = OpenAPIFS(spec_file)

    # List info fields
    info_fields = fs.ls("/info", detail=False)
    assert "title" in info_fields
    assert "version" in info_fields
    assert "description" in info_fields

    # Get info content
    info_content = fs.cat("/info").decode()
    info_data = json.loads(info_content)
    assert info_data["title"] == "Test API"
    assert info_data["version"] == "1.0.0"


def test_openapi_fs_paths_listing(spec_file):
    """Test paths section browsing."""
    fs = OpenAPIFS(spec_file)

    # List all paths
    paths = fs.ls("/paths", detail=False)
    assert "/users" in paths
    assert "/users/{id}" in paths

    # List operations for /users
    operations = fs.ls("/paths/users", detail=False)
    assert "GET" in operations
    assert "POST" in operations

    # Get detailed operation info
    detailed_ops = fs.ls("/paths/users", detail=True)
    get_op = next(op for op in detailed_ops if op["name"] == "GET")
    assert get_op["summary"] == "List users"
    assert get_op["operation_id"] == "listUsers"


def test_openapi_fs_operation_details(spec_file):
    """Test operation detail browsing."""
    fs = OpenAPIFS(spec_file)

    # List operation sections
    sections = fs.ls("/paths/users/{id}/GET", detail=False)
    assert "parameters" in sections
    assert "responses" in sections
    assert "__curl__" in sections
    assert "__summary__" in sections

    # Get operation summary
    summary = fs.cat("/paths/users/{id}/GET/__summary__").decode()
    summary_data = json.loads(summary)
    assert summary_data["method"] == "GET"
    assert summary_data["operationId"] == "getUser"

    # Get parameters
    params = fs.cat("/paths/users/{id}/GET/parameters").decode()
    params_data = json.loads(params)
    assert len(params_data) == 1
    assert params_data[0]["name"] == "id"


def test_openapi_fs_components(spec_file):
    """Test components section browsing."""
    fs = OpenAPIFS(spec_file)

    # List component types
    component_types = fs.ls("/components", detail=False)
    assert "schemas" in component_types

    # List schemas
    schemas = fs.ls("/components/schemas", detail=False)
    assert "User" in schemas

    # Get schema content
    user_schema = fs.cat("/components/schemas/User").decode()
    schema_data = json.loads(user_schema)
    assert schema_data["type"] == "object"
    assert "id" in schema_data["properties"]
    assert "name" in schema_data["properties"]


def test_openapi_fs_special_paths(spec_file):
    """Test special paths like __raw__ and __openapi__."""
    fs = OpenAPIFS(spec_file)

    # Test __raw__ path
    raw_spec = fs.cat("/__raw__").decode()
    raw_data = json.loads(raw_spec)
    assert raw_data["openapi"] == "3.0.1"
    assert raw_data["info"]["title"] == "Test API"

    # Test __openapi__ path
    openapi_info = fs.cat("/__openapi__").decode()
    openapi_data = json.loads(openapi_info)
    assert openapi_data["openapi"] == "3.0.1"
    assert openapi_data["spec_url"] == spec_file


def test_openapi_fs_info_method(spec_file):
    """Test info() method for various paths."""
    fs = OpenAPIFS(spec_file)

    # Root info
    root_info = fs.info("/")
    assert root_info["name"] == "Test API"
    assert root_info["type"] == "openapi_spec"
    assert root_info["version"] == "3.0.1"
    assert root_info["paths_count"] == 2  # noqa: PLR2004

    # Path info
    path_info = fs.info("/paths/users")
    assert path_info["name"] == "/users"
    assert path_info["type"] == "api_path"
    assert "GET" in path_info["operations"]
    assert "POST" in path_info["operations"]

    # Operation info
    op_info = fs.info("/paths/users/GET")
    assert op_info["method"] == "GET"
    assert op_info["operation_id"] == "listUsers"
    assert op_info["summary"] == "List users"


def test_openapi_fs_curl_generation(spec_file):
    """Test curl command generation."""
    fs = OpenAPIFS(spec_file)

    # Generate curl for GET operation
    curl_cmd = fs.cat("/paths/users/GET/__curl__").decode()
    assert "curl -X GET" in curl_cmd
    assert "/users" in curl_cmd


def test_openapi_fs_error_handling(spec_file):
    """Test error handling for invalid paths."""
    fs = OpenAPIFS(spec_file)

    # Non-existent path
    with pytest.raises(FileNotFoundError):
        fs.cat("/nonexistent")

    # Non-existent operation
    empty_result = fs.ls("/paths/nonexistent", detail=False)
    assert empty_result == []

    # Invalid operation method
    empty_ops = fs.ls("/paths/users/DELETE", detail=False)
    assert empty_ops == []


@pytest.mark.skipif(not requests, reason="requests not available")
def test_openapi_fs_remote_spec():
    """Test with remote OpenAPI spec (if network available)."""
    # Use a reliable public API spec
    try:
        fs = OpenAPIFS("https://petstore3.swagger.io/api/v3/openapi.json")

        # Basic connectivity test
        root_info = fs.info("/")
        assert root_info["type"] == "openapi_spec"

        # Test that we can list paths
        paths = fs.ls("/paths", detail=False)
        assert len(paths) > 0

    except Exception:  # noqa: BLE001
        # Skip if network issues or API unavailable
        pytest.skip("Remote API spec not accessible")


def test_openapi_fs_with_headers(spec_file):
    """Test OpenAPIFS with custom headers."""
    headers = {"Authorization": "Bearer test-token"}
    fs = OpenAPIFS(spec_file, headers=headers)

    assert fs.headers == headers

    # Should still work with local files
    root_info = fs.info("/")
    assert root_info["name"] == "Test API"


def test_openapi_fs_chaining(spec_file):
    """Test chaining with file protocol."""
    url = f"openapi::file://{spec_file}"

    import fsspec

    # Test that we can access via chaining
    with fsspec.open(url, mode="rb") as f:
        content = f.read().decode()

    # Should get the raw OpenAPI spec
    import json

    spec_data = json.loads(content)
    assert spec_data["openapi"] == "3.0.1"
    assert spec_data["info"]["title"] == "Test API"
