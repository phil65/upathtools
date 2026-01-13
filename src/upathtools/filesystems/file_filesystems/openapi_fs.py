"""Filesystem implementation for browsing OpenAPI specifications."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Required, TypedDict, overload
from urllib.parse import urlparse

import fsspec

from upathtools.filesystems.base import BaseFileFileSystem, BaseUPath, ProbeResult


if TYPE_CHECKING:
    from collections.abc import Callable

    from openapi3 import OpenAPI
    from openapi3.paths import Operation


class OpenApiInfo(TypedDict, total=False):
    """Info dict for OpenAPI filesystem paths."""

    name: Required[str]
    type: Required[str]
    size: int
    version: str
    url: str
    value: str
    api_version: str
    description: str | None
    paths_count: int
    components_count: int
    spec_url: str
    operations: list[str]
    component_type: str
    method: str | None
    operation_id: str | None
    summary: str | None
    parameters: list[str]
    responses: list[str]
    request_body: bool
    deprecated: bool
    tags: list[str]


class OpenAPIPath(BaseUPath[OpenApiInfo]):
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


class OpenAPIFileSystem(BaseFileFileSystem[OpenAPIPath, OpenApiInfo]):
    """Filesystem for browsing OpenAPI specifications and API documentation."""

    protocol = "openapi"
    upath_cls = OpenAPIPath
    supported_extensions: ClassVar[frozenset[str]] = frozenset({"yaml", "yml", "json"})
    priority: ClassVar[int] = 40  # Higher priority than JSON Schema for API specs

    @classmethod
    def probe_content(cls, content: bytes, extension: str = "") -> ProbeResult:  # noqa: PLR0911
        """Probe content to check if it's an OpenAPI specification.

        Looks for OpenAPI indicators like openapi version field or swagger field.
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

            # Strong indicators of OpenAPI
            if "openapi" in data:
                version = str(data.get("openapi", ""))
                if version.startswith("3."):
                    return ProbeResult.SUPPORTED
            if "swagger" in data:
                version = str(data.get("swagger", ""))
                if version.startswith("2."):
                    return ProbeResult.SUPPORTED

            # Weaker indicators - has paths and info like an API spec
            if "paths" in data and "info" in data:
                return ProbeResult.MAYBE

        except Exception:  # noqa: BLE001
            return ProbeResult.UNSUPPORTED
        else:
            return ProbeResult.UNSUPPORTED

    @classmethod
    def from_content(
        cls,
        content: bytes,
        **kwargs: Any,
    ) -> OpenAPIFileSystem:
        """Create filesystem instance from raw OpenAPI spec content.

        Args:
            content: Raw OpenAPI spec content as bytes (JSON or YAML).
            **kwargs: Additional filesystem options.

        Returns:
            Configured filesystem instance with pre-loaded spec.
        """
        from openapi3 import OpenAPI

        fs = cls(spec_url="<content>", **kwargs)
        text = content.decode("utf-8")
        # Try JSON first, then YAML
        try:
            spec_dict = json.loads(text)
        except json.JSONDecodeError:
            try:
                import yaml

                spec_dict = yaml.safe_load(text)
            except ImportError as exc:
                msg = "PyYAML required for YAML content"
                raise ImportError(msg) from exc
        fs._spec = OpenAPI(spec_dict, validate=True)
        return fs

    @classmethod
    def from_file(
        cls,
        path: str,
        target_protocol: str | None = None,
        target_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> OpenAPIFileSystem:
        """Create filesystem instance from an OpenAPI spec file path."""
        return cls(spec_url=path, **kwargs)

    def __init__(
        self,
        spec_url: str = "",
        headers: dict[str, str] | None = None,
        serializer: Literal["json", "json-formatted", "yaml"]
        | Callable[[dict[str, Any] | list[Any]], str] = "json",
        **kwargs: Any,
    ) -> None:
        """Initialize the filesystem.

        Args:
            spec_url: URL or file path to OpenAPI specification
            headers: HTTP headers for fetching remote specs (e.g., {"Authorization": "Bearer token"})
            serializer: Output format - "json" (compact, default), "json-formatted" (pretty-printed), "yaml", or custom callable
            kwargs: Additional keyword arguments for the filesystem
        """  # noqa: E501
        super().__init__(**kwargs)

        # Handle both direct usage and chaining - fo is used by fsspec for chaining
        fo = kwargs.pop("fo", "")
        url = spec_url or fo

        if not url:
            msg = "OpenAPI spec URL required"
            raise ValueError(msg)

        self.spec_url = url
        self.headers = headers or {}
        self.serializer = serializer
        self._spec: OpenAPI | None = None

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, Any]:
        path = path.removeprefix("openapi://")
        return {"spec_url": path}

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
                        msg = "PyYAML is required for YAML files. Install with: pip install pyyaml"
                        raise ImportError(msg) from exc
                else:
                    spec_dict = json.loads(content)

            self._spec = OpenAPI(spec_dict, validate=True)

        except Exception as exc:
            msg = f"Failed to load OpenAPI spec from {self.spec_url}: {exc}"
            raise FileNotFoundError(msg) from exc

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
    def ls(self, path: str, detail: Literal[True] = ..., **kwargs: Any) -> list[OpenApiInfo]: ...

    @overload
    def ls(self, path: str, detail: Literal[False], **kwargs: Any) -> list[str]: ...

    def ls(self, path: str, detail: bool = True, **kwargs: Any) -> list[OpenApiInfo] | list[str]:  # noqa: PLR0911
        """List OpenAPI specification contents."""
        self._load_spec()
        assert self._spec is not None
        path = self._strip_protocol(path).strip("/")  # pyright: ignore[reportAttributeAccessIssue]
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
                OpenApiInfo(name="info", type="section", description="API information"),
                OpenApiInfo(name="servers", type="section", description="Server configurations"),
                OpenApiInfo(name="paths", type="section", description="API endpoints"),
                OpenApiInfo(name="components", type="section", description="Reusable components"),
                OpenApiInfo(name="tags", type="section", description="Endpoint tags"),
                OpenApiInfo(name="__openapi__", type="special", description="OpenAPI version info"),
                OpenApiInfo(name="__raw__", type="special", description="Raw specification"),
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
                            if hasattr(self._spec.info, item) and getattr(self._spec.info, item)
                        ]

                    return [
                        OpenApiInfo(
                            name=item,
                            type="info_field",
                            value=str(getattr(self._spec.info, item))[:100],
                        )
                        for item in items
                        if hasattr(self._spec.info, item) and getattr(self._spec.info, item)
                    ]

            case "servers":
                if len(parts) == 1:
                    if not self._spec.servers:
                        return []

                    items = [str(i) for i in range(len(self._spec.servers))]
                    if not detail:
                        return items

                    return [
                        OpenApiInfo(
                            name=str(i),
                            type="server",
                            url=server.url,
                            description=server.description or "",
                        )
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

                        result.append(
                            OpenApiInfo(
                                name=path_key,
                                type="api_path",
                                operations=operations,
                                description=getattr(path_obj, "description", "") or "",
                            )
                        )
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
                    path_parts = [p for p in parts[1:-1] if p]  # Filter empty strings
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
                        OpenApiInfo(
                            name="parameters",
                            type="section",
                            description="Request parameters",
                        ),
                        OpenApiInfo(
                            name="responses",
                            type="section",
                            description="Response definitions",
                        ),
                        OpenApiInfo(
                            name="requestBody",
                            type="section",
                            description="Request body schema",
                        ),
                        OpenApiInfo(
                            name="__curl__",
                            type="special",
                            description="Generated curl command",
                        ),
                        OpenApiInfo(
                            name="__summary__",
                            type="special",
                            description="Operation summary",
                        ),
                    ]
                # List operations for a specific path
                # Reconstruct path from parts, handling {id} parameters
                path_parts = [p for p in parts[1:] if p]
                path_key = "/" + "/".join(path_parts)
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

                detail_result: list[OpenApiInfo] = []
                for method in items:
                    operation = getattr(path_obj, method.lower())
                    detail_result.append(
                        OpenApiInfo(
                            name=method,
                            type="operation",
                            **self._get_operation_info(operation, method, path_key),  # type: ignore[typeddict-item]
                        )
                    )
                return detail_result

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

                    return [OpenApiInfo(name=item, type="component_section") for item in items]

                if len(parts) == 2 and self._spec.components:  # noqa: PLR2004
                    component_type = parts[1]
                    component_map = getattr(self._spec.components, component_type, None)

                    if not component_map:
                        return []

                    items = list(component_map.keys())
                    if not detail:
                        return items

                    return [
                        OpenApiInfo(name=item, type=f"component_{component_type[:-1]}")
                        for item in items
                    ]

            case "tags":
                if len(parts) == 1 and self._spec.tags:
                    items = [tag.name for tag in self._spec.tags]
                    if not detail:
                        return items

                    return [
                        OpenApiInfo(
                            name=tag.name,
                            type="tag",
                            description=tag.description or "",
                        )
                        for tag in self._spec.tags
                    ]

        return []

    def cat(self, path: str = "") -> bytes:  # noqa: PLR0911
        """Get OpenAPI specification content."""
        self._load_spec()
        assert self._spec is not None

        path = self._strip_protocol(path).strip("/")  # pyright: ignore[reportAttributeAccessIssue]

        if not path:
            # Return full spec as JSON
            return self._serialize(self._spec.raw_element)

        parts = path.split("/")

        # Handle special paths
        if parts[-1].startswith("__") and parts[-1].endswith("__"):
            special_path = parts[-1]

            match special_path:
                case "__openapi__":
                    return self._serialize({
                        "openapi": self._spec.openapi,
                        "spec_url": self.spec_url,
                    })

                case "__raw__":
                    return self._serialize(self._spec.raw_element)

                case "__curl__":
                    # Generate curl command for operation
                    if len(parts) >= 4:  # noqa: PLR2004
                        path_parts = parts[1:-2]  # All parts except first, method, and special
                        path_key = "/" + "/".join(path_parts)
                        method = parts[-2].lower()  # Method is second to last

                        resolved_path = self._resolve_path_key(path_key)
                        if resolved_path:
                            path_key = resolved_path

                            if path_key in self._spec.paths:
                                path_obj = self._spec.paths[path_key]
                                if hasattr(path_obj, method) and getattr(path_obj, method):
                                    # Basic curl command generation
                                    server_url = (
                                        self._spec.servers[0].url
                                        if self._spec.servers
                                        else "https://api.example.com"
                                    )
                                    curl_cmd = (
                                        f"curl -X {method.upper()} \\\n  '{server_url}{path_key}'"
                                    )

                                    operation = getattr(path_obj, method)
                                    if operation.parameters:
                                        curl_cmd += " \\\n  # Add parameters as needed"

                                    if hasattr(operation, "requestBody") and operation.requestBody:
                                        curl_cmd += " \\\n  -H 'Content-Type: application/json' \\\n  -d '{\"key\": \"value\"}'"  # noqa: E501

                                    return curl_cmd.encode()

                case "__summary__":
                    # Operation summary
                    if len(parts) >= 4:  # noqa: PLR2004
                        path_parts = parts[1:-2]  # All parts except first, method, and special
                        path_key = "/" + "/".join(path_parts)
                        method = parts[-2].lower()  # Method is second to last

                        resolved_path = self._resolve_path_key(path_key)
                        if resolved_path:
                            path_key = resolved_path

                            if path_key in self._spec.paths:
                                path_obj = self._spec.paths[path_key]
                                if hasattr(path_obj, method) and getattr(path_obj, method):
                                    operation = getattr(path_obj, method)
                                    summary = {
                                        "method": method.upper(),
                                        "path": path_key,
                                        "operationId": operation.operationId,
                                        "summary": operation.summary,
                                        "description": operation.description,
                                        "tags": operation.tags,
                                    }
                                    return self._serialize(summary)

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
                    return self._serialize(info_data)
                if len(parts) == 2:  # noqa: PLR2004
                    field = parts[1]
                    if hasattr(self._spec.info, field):
                        value = getattr(self._spec.info, field)
                        if isinstance(value, str):
                            return value.encode()
                        return self._serialize(value)

            case "servers":
                if len(parts) == 2 and self._spec.servers:  # noqa: PLR2004
                    try:
                        idx = int(parts[1])
                        if 0 <= idx < len(self._spec.servers):
                            server = self._spec.servers[idx]
                            return self._serialize({
                                "url": server.url,
                                "description": server.description,
                            })
                    except ValueError:
                        pass

            case "paths":
                # Filter empty parts from double slashes
                non_empty_parts = [p for p in parts[1:] if p]

                if len(non_empty_parts) == 1:
                    # Reading a full path object like /paths//session
                    path_key = "/" + non_empty_parts[0]
                    resolved_path = self._resolve_path_key(path_key)
                    if resolved_path and resolved_path in self._spec.paths:
                        path_obj = self._spec.paths[resolved_path]
                        return self._serialize(path_obj.raw_element)
                if len(non_empty_parts) >= 2:  # noqa: PLR2004
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
                        path_parts = [p for p in parts[1:-2] if p]  # Filter empty strings
                    elif parts[-1].lower() in possible_methods:
                        # Format: /paths/.../METHOD
                        method = parts[-1].lower()
                        section = None
                        path_parts = [p for p in parts[1:-1] if p]  # Filter empty strings
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
                                return self._serialize(operation.raw_element)
                            if section == "parameters" and operation.parameters:
                                params = [p.raw_element for p in operation.parameters]
                                return self._serialize(params)
                            if section == "responses" and operation.responses:
                                responses = {
                                    k: v.raw_element for k, v in operation.responses.items()
                                }
                                return self._serialize(responses)
                            if (
                                section == "requestBody"
                                and hasattr(operation, "requestBody")
                                and operation.requestBody
                            ):
                                return self._serialize(operation.requestBody.raw_element)

            case "components":
                if len(parts) >= 2 and self._spec.components:  # noqa: PLR2004
                    component_type = parts[1]
                    component_map = getattr(self._spec.components, component_type, None)

                    if component_map:
                        if len(parts) == 2:  # noqa: PLR2004
                            return self._serialize({
                                k: v.raw_element for k, v in component_map.items()
                            })
                        if len(parts) == 3:  # noqa: PLR2004
                            component_name = parts[2]
                            if component_name in component_map:
                                return self._serialize(component_map[component_name].raw_element)

        msg = f"Path {path} not found"
        raise FileNotFoundError(msg)

    def cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any
    ) -> bytes:
        """Async version of cat for fsspec compatibility."""
        content = self.cat(path)
        if start is not None or end is not None:
            return content[start:end]
        return content

    def exists(self, path: str, **kwargs: Any) -> bool:
        """Check if path exists in OpenAPI spec."""
        try:
            self.info(path, **kwargs)
        except FileNotFoundError:
            return False
        else:
            return True

    def isfile(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a file (not a directory)."""
        return not self.isdir(path)

    def info(self, path: str, **kwargs: Any) -> OpenApiInfo:
        """Get detailed info about an OpenAPI element."""
        self._load_spec()
        assert self._spec is not None

        path = self._strip_protocol(path).strip("/")  # pyright: ignore[reportAttributeAccessIssue]

        if not path:
            # Root spec info
            spec_content = self.cat("").decode()
            return OpenApiInfo(
                name=self._spec.info.title,
                type="openapi_spec",
                version=self._spec.openapi,
                api_version=self._spec.info.version,
                description=self._spec.info.description,
                paths_count=len(self._spec.paths) if self._spec.paths else 0,
                components_count=len(self._spec.components.schemas)
                if self._spec.components and self._spec.components.schemas
                else 0,
                spec_url=self.spec_url,
                size=len(spec_content),
            )

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

                        return OpenApiInfo(
                            name=path_key,
                            type="api_path",
                            operations=operations,
                            description=getattr(path_obj, "description", "") or "",
                            size=len(str(path_obj.raw_element)),
                        )
                elif len(parts) >= 3:  # noqa: PLR2004
                    # Reconstruct path from parts excluding the method
                    path_parts = [p for p in parts[1:-1] if p]  # Filter empty strings
                    path_key = "/" + "/".join(path_parts)
                    method = parts[-1].lower()  # Last part is the method

                    resolved_path = self._resolve_path_key(path_key)
                    if resolved_path:
                        path_key = resolved_path

                    if path_key in self._spec.paths:
                        path_obj = self._spec.paths[path_key]
                        if hasattr(path_obj, method) and getattr(path_obj, method):
                            operation = getattr(path_obj, method)
                            op_info = self._get_operation_info(operation, method, path_key)
                            return OpenApiInfo(
                                name=f"{method.upper()} {path_key}",
                                type="operation",
                                size=len(str(operation.raw_element)),
                                method=op_info.get("method"),
                                operation_id=op_info.get("operation_id"),
                                summary=op_info.get("summary"),
                                description=op_info.get("description"),
                                parameters=op_info.get("parameters", []),
                                responses=op_info.get("responses", []),
                                request_body=op_info.get("request_body", False),
                                deprecated=op_info.get("deprecated", False),
                                tags=op_info.get("tags", []),
                            )

            case "components":
                if len(parts) == 3 and self._spec.components:  # noqa: PLR2004
                    component_type = parts[1]
                    component_name = parts[2]
                    component_map = getattr(self._spec.components, component_type, None)

                    if component_map and component_name in component_map:
                        component = component_map[component_name]
                        return OpenApiInfo(
                            name=component_name,
                            type=f"component_{component_type[:-1]}",
                            size=len(str(component.raw_element)),
                            component_type=component_type,
                        )

        msg = f"Path {path} not found"
        raise FileNotFoundError(msg)

    def isdir(self, path: str) -> bool:  # noqa: PLR0911
        """Check if path is a directory (has navigable children)."""
        self._load_spec()
        assert self._spec is not None

        path = self._strip_protocol(path).strip("/")  # pyright: ignore[reportAttributeAccessIssue]

        if not path:
            # Root is always a directory
            return True

        parts = path.split("/")

        # Top-level sections are directories
        if len(parts) == 1:
            return parts[0] in {"info", "servers", "paths", "components", "tags"}

        match parts[0]:
            case "paths":
                if len(parts) == 2:  # noqa: PLR2004
                    # /paths/{path} - has operations as children
                    path_key = "/" + parts[1]
                    resolved = self._resolve_path_key(path_key)
                    return (resolved or path_key) in self._spec.paths
                # /paths/{path}/{method} - operation is a file
                return False

            case "components":
                if len(parts) == 2:  # noqa: PLR2004
                    # /components/{type} - has component names as children
                    component_type = parts[1]
                    return (
                        self._spec.components is not None
                        and hasattr(self._spec.components, component_type)
                        and getattr(self._spec.components, component_type) is not None
                    )
                # /components/{type}/{name} - component is a file
                return False

            case "servers":
                # /servers is a directory, /servers/{n} is a file
                return len(parts) == 1

            case "info" | "tags":
                # These are directories at top level only
                return len(parts) == 1

        return False


if __name__ == "__main__":
    fs = OpenAPIFileSystem("https://petstore3.swagger.io/api/v3/openapi.json")
    print("Root sections:", fs.ls("/", detail=False))
    print("API Info:", fs.info("/"))
    print("Paths:", fs.ls("/paths", detail=False)[:5])  # First 5 paths
