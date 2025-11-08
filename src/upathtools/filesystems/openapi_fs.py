"""Filesystem implementation for browsing OpenAPI specifications."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Literal, overload
from urllib.parse import urlparse

import fsspec
from fsspec.spec import AbstractFileSystem
from upath import UPath


if TYPE_CHECKING:
    from openapi3 import OpenAPI
    from openapi3.paths import Operation


class OpenAPIPath(UPath):
    """UPath implementation for browsing OpenAPI specifications."""

    __slots__ = ()

    def iterdir(self):
        if not self.is_dir():
            raise NotADirectoryError(str(self))
        yield from super().iterdir()

    @property
    def path(self) -> str:
        path = super().path
        return "/" if path == "." else path


class OpenAPIFS(AbstractFileSystem):
    """Filesystem for browsing OpenAPI specifications and API documentation."""

    protocol = "openapi"

    def __init__(
        self,
        spec_url: str,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the filesystem.

        Args:
            spec_url: URL or file path to OpenAPI specification
            headers: HTTP headers for fetching remote specs (e.g., {"Authorization": "Bearer token"})
            kwargs: Additional keyword arguments for the filesystem
        """  # noqa: E501
        super().__init__(**kwargs)

        self.spec_url = spec_url
        self.headers = headers or {}
        self._spec: OpenAPI | None = None

    def _make_path(self, path: str) -> UPath:
        """Create a path object from string."""
        return OpenAPIPath(path)

    def _load_spec(self) -> None:
        """Load and parse the OpenAPI specification."""
        from openapi3 import OpenAPI
        import requests

        if self._spec is not None:
            return

        try:
            parsed_url = urlparse(self.spec_url)
            if parsed_url.scheme in ("http", "https"):
                response = requests.get(self.spec_url, headers=self.headers, timeout=30)
                response.raise_for_status()
                spec_dict = response.json()
            else:
                # Local file
                with fsspec.open(self.spec_url, "r") as f:
                    content = f.read()  # pyright: ignore[reportAttributeAccessIssue]
                if self.spec_url.endswith((".yaml", ".yml")):
                    try:
                        import yaml

                        spec_dict = yaml.safe_load(content)
                    except ImportError as exc:
                        msg = "PyYAML is required for YAML files. Install with: pip install pyyaml"  # noqa: E501
                        raise ImportError(msg) from exc
                else:
                    spec_dict = json.loads(content)

            self._spec = OpenAPI(spec_dict, validate=True)

        except Exception as exc:
            msg = f"Failed to load OpenAPI spec from {self.spec_url}: {exc}"
            raise FileNotFoundError(msg) from exc

    def _resolve_path_key(self, path_key: str) -> str | None:
        """Resolve a path key, handling parameterized paths like {id}."""
        self._load_spec()
        assert self._spec is not None

        if path_key in self._spec.paths:
            return path_key

        # Try to match parameterized paths
        for spec_path in self._spec.paths:
            if self._paths_match(spec_path, path_key):
                return spec_path

        return None

    def _paths_match(self, spec_path: str, input_path: str) -> bool:
        """Check if paths match, considering parameters like {id}."""
        # Simple matching: remove all braces and compare
        spec_normalized = spec_path.replace("{", "").replace("}", "")
        input_normalized = input_path.replace("{", "").replace("}", "")
        return spec_normalized == input_normalized

    def _get_operation_info(
        self, operation: Operation, method: str, path_key: str
    ) -> dict[str, Any]:
        """Extract operation information for listing."""
        return {
            "method": method.upper(),
            "path": path_key,
            "operation_id": operation.operationId,
            "summary": operation.summary,
            "description": operation.description,
            "tags": operation.tags or [],
            "deprecated": getattr(operation, "deprecated", False),
            "parameters": len(operation.parameters) if operation.parameters else 0,
            "responses": list(operation.responses.keys()) if operation.responses else [],
        }

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

    def ls(  # noqa: PLR0911
        self,
        path: str = "",
        detail: bool = True,
        **kwargs: Any,
    ) -> list[dict[str, Any]] | list[str]:
        """List OpenAPI specification contents."""
        self._load_spec()
        assert self._spec is not None

        path = self._strip_protocol(path).strip("/")  # type: ignore

        if not path:
            # Root - show main sections
            items = [
                "info",
                "servers",
                "paths",
                "components",
                "tags",
                "__openapi__",
                "__raw__",
            ]
            if not detail:
                return items

            return [
                {"name": "info", "type": "section", "description": "API information"},
                {
                    "name": "servers",
                    "type": "section",
                    "description": "Server configurations",
                },
                {"name": "paths", "type": "section", "description": "API endpoints"},
                {
                    "name": "components",
                    "type": "section",
                    "description": "Reusable components",
                },
                {"name": "tags", "type": "section", "description": "Endpoint tags"},
                {
                    "name": "__openapi__",
                    "type": "special",
                    "description": "OpenAPI version info",
                },
                {
                    "name": "__raw__",
                    "type": "special",
                    "description": "Raw specification",
                },
            ]

        parts = path.split("/")

        match parts[0]:
            case "info":
                if len(parts) == 1:
                    items = ["title", "version", "description", "contact", "license"]
                    if not detail:
                        return [
                            item
                            for item in items
                            if hasattr(self._spec.info, item)
                            and getattr(self._spec.info, item)
                        ]

                    return [
                        {
                            "name": item,
                            "type": "info_field",
                            "value": str(getattr(self._spec.info, item))[:100],
                        }
                        for item in items
                        if hasattr(self._spec.info, item)
                        and getattr(self._spec.info, item)
                    ]

            case "servers":
                if len(parts) == 1:
                    if not self._spec.servers:
                        return []

                    items = [str(i) for i in range(len(self._spec.servers))]
                    if not detail:
                        return items

                    return [
                        {
                            "name": str(i),
                            "type": "server",
                            "url": server.url,
                            "description": server.description or "",
                        }
                        for i, server in enumerate(self._spec.servers)
                    ]

            case "paths":
                if len(parts) == 1:
                    # List all paths
                    items = list(self._spec.paths.keys())
                    if not detail:
                        return items

                    result = []
                    for path_key in items:
                        path_obj = self._spec.paths[path_key]
                        operations = [
                            method.upper()
                            for method in [
                                "get",
                                "post",
                                "put",
                                "delete",
                                "patch",
                                "head",
                                "options",
                                "trace",
                            ]
                            if hasattr(path_obj, method) and getattr(path_obj, method)
                        ]

                        result.append({
                            "name": path_key,
                            "type": "api_path",
                            "operations": operations,
                            "description": getattr(path_obj, "description", "") or "",
                        })
                    return result

                # Check if the last part is an HTTP method
                possible_methods = [
                    "GET",
                    "POST",
                    "PUT",
                    "DELETE",
                    "PATCH",
                    "HEAD",
                    "OPTIONS",
                    "TRACE",
                ]
                is_operation_detail = parts[-1].upper() in possible_methods

                if is_operation_detail:
                    # List operation details
                    # Reconstruct path from parts excluding the method
                    path_parts = parts[
                        1:-1
                    ]  # All parts except first (paths) and last (method)
                    path_key = "/" + "/".join(path_parts)
                    method = parts[-1].lower()  # Last part is the method

                    resolved_path = self._resolve_path_key(path_key)
                    if not resolved_path:
                        return []
                    path_key = resolved_path

                    path_obj = self._spec.paths[path_key]
                    if not hasattr(path_obj, method) or not getattr(path_obj, method):
                        return []

                    items = [
                        "parameters",
                        "responses",
                        "requestBody",
                        "__curl__",
                        "__summary__",
                    ]
                    if not detail:
                        return list(items)

                    return [
                        {
                            "name": "parameters",
                            "type": "section",
                            "description": "Request parameters",
                        },
                        {
                            "name": "responses",
                            "type": "section",
                            "description": "Response definitions",
                        },
                        {
                            "name": "requestBody",
                            "type": "section",
                            "description": "Request body schema",
                        },
                        {
                            "name": "__curl__",
                            "type": "special",
                            "description": "Generated curl command",
                        },
                        {
                            "name": "__summary__",
                            "type": "special",
                            "description": "Operation summary",
                        },
                    ]
                # List operations for a specific path
                # Reconstruct path from parts, handling {id} parameters
                path_key = "/" + "/".join(parts[1:])
                resolved_path = self._resolve_path_key(path_key)
                if not resolved_path:
                    return []
                path_key = resolved_path

                path_obj = self._spec.paths[path_key]
                items = []

                for method in [
                    "get",
                    "post",
                    "put",
                    "delete",
                    "patch",
                    "head",
                    "options",
                    "trace",
                ]:
                    if hasattr(path_obj, method) and getattr(path_obj, method):
                        items.append(method.upper())

                if not detail:
                    return items

                result = []
                for method in items:
                    operation = getattr(path_obj, method.lower())
                    result.append({
                        "name": method,
                        "type": "operation",
                        **self._get_operation_info(operation, method, path_key),
                    })
                return result

            case "components":
                if len(parts) == 1:
                    items = []
                    if self._spec.components:
                        if self._spec.components.schemas:
                            items.append("schemas")
                        if self._spec.components.responses:
                            items.append("responses")
                        if self._spec.components.parameters:
                            items.append("parameters")
                        if self._spec.components.examples:
                            items.append("examples")
                        if self._spec.components.securitySchemes:
                            items.append("securitySchemes")

                    if not detail:
                        return items

                    return [{"name": item, "type": "component_section"} for item in items]

                if len(parts) == 2 and self._spec.components:  # noqa: PLR2004
                    component_type = parts[1]
                    component_map = getattr(self._spec.components, component_type, None)

                    if not component_map:
                        return []

                    items = list(component_map.keys())
                    if not detail:
                        return items

                    return [
                        {"name": item, "type": f"component_{component_type[:-1]}"}
                        for item in items
                    ]

            case "tags":
                if len(parts) == 1 and self._spec.tags:
                    items = [tag.name for tag in self._spec.tags]
                    if not detail:
                        return items

                    return [
                        {
                            "name": tag.name,
                            "type": "tag",
                            "description": tag.description or "",
                        }
                        for tag in self._spec.tags
                    ]

        return []

    def cat(self, path: str = "") -> bytes:  # noqa: PLR0911
        """Get OpenAPI specification content."""
        self._load_spec()
        assert self._spec is not None

        path = self._strip_protocol(path).strip("/")  # type: ignore

        if not path:
            # Return full spec as JSON
            return json.dumps(self._spec.raw_element, indent=2).encode()

        parts = path.split("/")

        # Handle special paths
        if parts[-1].startswith("__") and parts[-1].endswith("__"):
            special_path = parts[-1]

            match special_path:
                case "__openapi__":
                    return json.dumps(
                        {
                            "openapi": self._spec.openapi,
                            "spec_url": self.spec_url,
                        },
                        indent=2,
                    ).encode()

                case "__raw__":
                    return json.dumps(self._spec.raw_element, indent=2).encode()

                case "__curl__":
                    # Generate curl command for operation
                    if len(parts) >= 4:  # noqa: PLR2004
                        path_parts = parts[
                            1:-2
                        ]  # All parts except first, method, and special
                        path_key = "/" + "/".join(path_parts)
                        method = parts[-2].lower()  # Method is second to last

                        resolved_path = self._resolve_path_key(path_key)
                        if resolved_path:
                            path_key = resolved_path

                            if path_key in self._spec.paths:
                                path_obj = self._spec.paths[path_key]
                                if hasattr(path_obj, method) and getattr(
                                    path_obj, method
                                ):
                                    # Basic curl command generation
                                    server_url = (
                                        self._spec.servers[0].url
                                        if self._spec.servers
                                        else "https://api.example.com"
                                    )
                                    curl_cmd = f"curl -X {method.upper()} \\\n  '{server_url}{path_key}'"  # noqa: E501

                                    operation = getattr(path_obj, method)
                                    if operation.parameters:
                                        curl_cmd += " \\\n  # Add parameters as needed"

                                    if (
                                        hasattr(operation, "requestBody")
                                        and operation.requestBody
                                    ):
                                        curl_cmd += " \\\n  -H 'Content-Type: application/json' \\\n  -d '{\"key\": \"value\"}'"  # noqa: E501

                                    return curl_cmd.encode()

                case "__summary__":
                    # Operation summary
                    if len(parts) >= 4:  # noqa: PLR2004
                        path_parts = parts[
                            1:-2
                        ]  # All parts except first, method, and special
                        path_key = "/" + "/".join(path_parts)
                        method = parts[-2].lower()  # Method is second to last

                        resolved_path = self._resolve_path_key(path_key)
                        if resolved_path:
                            path_key = resolved_path

                            if path_key in self._spec.paths:
                                path_obj = self._spec.paths[path_key]
                                if hasattr(path_obj, method) and getattr(
                                    path_obj, method
                                ):
                                    operation = getattr(path_obj, method)
                                    summary = {
                                        "method": method.upper(),
                                        "path": path_key,
                                        "operationId": operation.operationId,
                                        "summary": operation.summary,
                                        "description": operation.description,
                                        "tags": operation.tags,
                                    }
                                    return json.dumps(summary, indent=2).encode()

        # Handle regular paths
        match parts[0]:
            case "info":
                if len(parts) == 1:
                    info_data = {
                        "title": self._spec.info.title,
                        "version": self._spec.info.version,
                        "description": self._spec.info.description,
                    }
                    if self._spec.info.contact:
                        info_data["contact"] = {
                            "name": self._spec.info.contact.name,
                            "email": self._spec.info.contact.email,
                            "url": self._spec.info.contact.url,
                        }
                    return json.dumps(info_data, indent=2).encode()
                if len(parts) == 2:  # noqa: PLR2004
                    field = parts[1]
                    if hasattr(self._spec.info, field):
                        value = getattr(self._spec.info, field)
                        if isinstance(value, str):
                            return value.encode()
                        return json.dumps(value, indent=2, default=str).encode()

            case "servers":
                if len(parts) == 2 and self._spec.servers:  # noqa: PLR2004
                    try:
                        idx = int(parts[1])
                        if 0 <= idx < len(self._spec.servers):
                            server = self._spec.servers[idx]
                            return json.dumps(
                                {
                                    "url": server.url,
                                    "description": server.description,
                                },
                                indent=2,
                            ).encode()
                    except ValueError:
                        pass

            case "paths":
                if len(parts) == 2:  # noqa: PLR2004
                    path_key = "/" + parts[1]
                    resolved_path = self._resolve_path_key(path_key)
                    if resolved_path and resolved_path in self._spec.paths:
                        path_obj = self._spec.paths[resolved_path]
                        return json.dumps(path_obj.raw_element, indent=2).encode()
                if len(parts) >= 3:  # noqa: PLR2004
                    # Check if last part is an HTTP method
                    possible_methods = [
                        "get",
                        "post",
                        "put",
                        "delete",
                        "patch",
                        "head",
                        "options",
                        "trace",
                    ]

                    # Determine if we have an operation section or just the operation
                    if len(parts) >= 4 and parts[-1] in [  # noqa: PLR2004
                        "parameters",
                        "responses",
                        "requestBody",
                    ]:
                        # Format: /paths/.../METHOD/SECTION
                        method = parts[-2].lower()
                        section = parts[-1]
                        path_parts = parts[
                            1:-2
                        ]  # Everything except paths, method, section
                    elif parts[-1].lower() in possible_methods:
                        # Format: /paths/.../METHOD
                        method = parts[-1].lower()
                        section = None
                        path_parts = parts[1:-1]  # Everything except paths and method
                    else:
                        return b""

                    path_key = "/" + "/".join(path_parts)
                    resolved_path = self._resolve_path_key(path_key)
                    if not resolved_path:
                        return b""
                    path_key = resolved_path

                    if path_key in self._spec.paths:
                        path_obj = self._spec.paths[path_key]
                        if hasattr(path_obj, method) and getattr(path_obj, method):
                            operation = getattr(path_obj, method)

                            if section is None:
                                # Return full operation
                                return json.dumps(
                                    operation.raw_element, indent=2
                                ).encode()
                            if section == "parameters" and operation.parameters:
                                params = [p.raw_element for p in operation.parameters]
                                return json.dumps(params, indent=2).encode()
                            if section == "responses" and operation.responses:
                                responses = {
                                    k: v.raw_element
                                    for k, v in operation.responses.items()
                                }
                                return json.dumps(responses, indent=2).encode()
                            if (
                                section == "requestBody"
                                and hasattr(operation, "requestBody")
                                and operation.requestBody
                            ):
                                return json.dumps(
                                    operation.requestBody.raw_element, indent=2
                                ).encode()

            case "components":
                if len(parts) >= 2 and self._spec.components:  # noqa: PLR2004
                    component_type = parts[1]
                    component_map = getattr(self._spec.components, component_type, None)

                    if component_map:
                        if len(parts) == 2:  # noqa: PLR2004
                            return json.dumps(
                                {k: v.raw_element for k, v in component_map.items()},
                                indent=2,
                            ).encode()
                        if len(parts) == 3:  # noqa: PLR2004
                            component_name = parts[2]
                            if component_name in component_map:
                                return json.dumps(
                                    component_map[component_name].raw_element, indent=2
                                ).encode()

        msg = f"Path {path} not found"
        raise FileNotFoundError(msg)

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Get detailed info about an OpenAPI element."""
        self._load_spec()
        assert self._spec is not None

        path = self._strip_protocol(path).strip("/")  # type: ignore

        if not path:
            # Root spec info
            return {
                "name": self._spec.info.title,
                "type": "openapi_spec",
                "version": self._spec.openapi,
                "api_version": self._spec.info.version,
                "description": self._spec.info.description,
                "paths_count": len(self._spec.paths) if self._spec.paths else 0,
                "components_count": len(self._spec.components.schemas)
                if self._spec.components and self._spec.components.schemas
                else 0,
                "spec_url": self.spec_url,
            }

        parts = path.split("/")

        match parts[0]:
            case "paths":
                if len(parts) == 2:  # noqa: PLR2004
                    path_key = "/" + parts[1]
                    if path_key in self._spec.paths:
                        path_obj = self._spec.paths[path_key]
                        operations = []
                        for method in [
                            "get",
                            "post",
                            "put",
                            "delete",
                            "patch",
                            "head",
                            "options",
                            "trace",
                        ]:
                            if hasattr(path_obj, method) and getattr(path_obj, method):
                                operations.append(method.upper())  # noqa: PERF401

                        return {
                            "name": path_key,
                            "type": "api_path",
                            "operations": operations,
                            "description": getattr(path_obj, "description", "") or "",
                        }
                elif len(parts) >= 3:  # noqa: PLR2004
                    # Reconstruct path from parts excluding the method
                    path_parts = parts[
                        1:-1
                    ]  # All parts except first (paths) and last (method)
                    path_key = "/" + "/".join(path_parts)
                    method = parts[-1].lower()  # Last part is the method

                    resolved_path = self._resolve_path_key(path_key)
                    if resolved_path:
                        path_key = resolved_path

                    if path_key in self._spec.paths:
                        path_obj = self._spec.paths[path_key]
                        if hasattr(path_obj, method) and getattr(path_obj, method):
                            operation = getattr(path_obj, method)
                            return {
                                "name": f"{method.upper()} {path_key}",
                                "type": "operation",
                                **self._get_operation_info(operation, method, path_key),
                            }

            case "components":
                if len(parts) == 3 and self._spec.components:  # noqa: PLR2004
                    component_type = parts[1]
                    component_name = parts[2]
                    component_map = getattr(self._spec.components, component_type, None)

                    if component_map and component_name in component_map:
                        return {
                            "name": component_name,
                            "type": f"component_{component_type[:-1]}",
                            "component_type": component_type,
                        }

        msg = f"Path {path} not found"
        raise FileNotFoundError(msg)


if __name__ == "__main__":
    fs = OpenAPIFS("https://petstore3.swagger.io/api/v3/openapi.json")
    print("Root sections:", fs.ls("/", detail=False))
    print("API Info:", fs.info("/"))
    print("Paths:", fs.ls("/paths", detail=False)[:5])  # First 5 paths
