from __future__ import annotations

import io
import json
from typing import TYPE_CHECKING, Any

from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath


if TYPE_CHECKING:
    from collections.abc import Callable


class NotionPath(BaseUPath):
    """UPath implementation for Notion filesystem."""

    __slots__ = ()


class NotionFS(BaseAsyncFileSystem[NotionPath]):
    protocol = "notion"
    upath_cls = NotionPath

    def __init__(self, token: str, parent_page_id: str, **kwargs: Any):
        """Initialize NotionFS with a Notion integration token.

        Args:
            token: Notion integration token
            parent_page_id: ID of the parent page where new pages will be created
            kwargs: Keyword arguments passed to parent class
        """
        from notion_client import Client

        super().__init__(**kwargs)
        try:
            self.notion = Client(auth=token)
            # Verify the token by making a test request
            self.notion.users.me()
        except Exception as e:
            msg = f"Invalid Notion token: {e!s}"
            raise ValueError(msg) from e
        self.parent_page_id = parent_page_id
        self._path_cache: dict[str, str] = {}

    def exists(self, path: str, **kwargs: Any) -> bool:
        """Check if a path exists."""
        from notion_client import APIResponseError

        stripped = self._strip_protocol(path)
        assert isinstance(stripped, str)
        try:
            return self._get_page_id_from_path(stripped) is not None
        except APIResponseError:
            return False

    def mkdir(self, path: str, **kwargs: Any):
        """Create a new page (folder) in Notion."""
        stripped = self._strip_protocol(path)
        assert isinstance(stripped, str)
        parts = [p for p in stripped.split("/") if p]

        # Get the parent page ID
        parent_id = self.parent_page_id
        current_path = ""

        # Create pages hierarchically
        for part in parts:
            current_path += "/" + part
            existing_id = self._get_page_id_from_path(current_path)

            if existing_id:
                parent_id = existing_id
                continue

            # Create new page under the parent
            response = self.notion.pages.create(
                parent={"type": "page_id", "page_id": parent_id},
                properties={"title": {"title": [{"text": {"content": part}}]}},
            )

            new_id = response["id"]  # type: ignore
            self._path_cache[current_path] = new_id
            parent_id = new_id

    def makedirs(self, path: str, exist_ok: bool = False):
        """Create a directory and any parent directories."""
        path = self._strip_protocol(path)  # type: ignore

        if self._get_page_id_from_path(path):
            if not exist_ok:
                msg = f"Path already exists: {path}"
                raise OSError(msg)
            return

        # Create parent directories
        parts = [p for p in path.split("/") if p]
        current_path = ""
        parent_id = self.parent_page_id

        for part in parts:
            current_path += "/" + part
            existing_id = self._get_page_id_from_path(current_path)

            if existing_id:
                parent_id = existing_id
                continue

            # Create new page
            try:
                response = self.notion.pages.create(
                    parent={"type": "page_id", "page_id": parent_id},
                    properties={"title": {"title": [{"text": {"content": part}}]}},
                )
                new_id = response["id"]  # type: ignore
                self._path_cache[current_path] = new_id
                parent_id = new_id
            except Exception as e:
                if not exist_ok:
                    msg = f"Failed to create directory: {e!s}"
                    raise OSError(msg) from e

    def rm(self, path: str, **kwargs: Any) -> None:
        """Remove (archive) a page and its children."""
        page_id = self._get_page_id_from_path(self._strip_protocol(path))  # type: ignore
        if not page_id:
            msg = f"Path not found: {path}"
            raise FileNotFoundError(msg)

        # Archive the page first to prevent further modifications
        self.notion.pages.update(page_id=page_id, archived=True)

        # Remove from cache
        self._path_cache = {
            k: v for k, v in self._path_cache.items() if not k.startswith(path)
        }

    def rm_file(self, path: str) -> None:
        """Remove a file (alias for rm)."""
        self.rm(path)

    def rmdir(self, path: str) -> None:
        """Remove a directory (page that may contain other pages)."""
        self.rm(path)

    def _get_page_id_from_path(self, path: str) -> str | None:
        """Convert a path to a Notion page ID."""
        if path in self._path_cache:
            return self._path_cache[path]

        # Remove leading/trailing slashes
        path = path.strip("/")

        if not path:
            return None  # Root directory

        parts = path.split("/")
        current_id = self.parent_page_id
        current_path = ""

        for part in parts:
            current_path += "/" + part

            # First try cache
            if current_path in self._path_cache:
                current_id = self._path_cache[current_path]
                continue

            # Search for the page by title under current parent
            found = False
            _filter = {"property": "object", "value": "page"}
            response = self.notion.search(query=part, filter=_filter).get("results", [])  # type: ignore

            for page in response:
                page_title = (
                    page.get("properties", {})
                    .get("title", {})
                    .get("title", [{}])[0]
                    .get("text", {})
                    .get("content", "")
                )
                if page_title == part:
                    current_id = page["id"]
                    self._path_cache[current_path] = current_id
                    found = True
                    break

            if not found:
                return None

        return current_id

    def ls(
        self, path: str, detail: bool = False, **kwargs: Any
    ) -> list[str] | list[dict[str, Any]]:
        """List contents of a path."""
        path = self._strip_protocol(path)  # type: ignore

        if not path or path == "/":
            # Root directory - list all pages under parent page
            children = self.notion.blocks.children.list(block_id=self.parent_page_id)
            results = children.get("results", [])  # type: ignore

            if not results:
                return []

            if detail:
                return [
                    {
                        "name": self._get_block_title(result),
                        "size": len(json.dumps(result)),
                        "type": result["type"],
                        "created": result.get("created_time"),
                        "modified": result.get("last_edited_time"),
                    }
                    for result in results
                    if result["type"] == "child_page"
                ]
            return [
                self._get_block_title(result)
                for result in results
                if result["type"] == "child_page"
            ]

        page_id = self._get_page_id_from_path(path)
        if not page_id:
            msg = f"Path not found: {path}"
            raise FileNotFoundError(msg)
        children = self.notion.blocks.children.list(block_id=page_id)
        results = children.get("results", [])  # type: ignore

        if not results:
            return []

        if detail:
            return [
                {
                    "name": self._get_block_title(block),
                    "size": len(json.dumps(block)),
                    "type": block["type"],
                    "created": block.get("created_time"),
                    "modified": block.get("last_edited_time"),
                }
                for block in results
                if block["type"] == "child_page"
            ]
        return [self._get_block_title(b) for b in results if b["type"] == "child_page"]

    def _get_page_title(self, page: dict[str, Any]) -> str:
        """Extract page title safely."""
        try:
            return page["properties"]["title"]["title"][0]["text"]["content"]
        except (KeyError, IndexError):
            return "Untitled"

    def _get_block_title(self, block: dict[str, Any]) -> str:
        """Extract block title safely."""
        if block["type"] == "child_page":
            return block.get("child_page", {}).get("title", "Untitled")
        if block["type"] == "page":
            try:
                return block["properties"]["title"]["title"][0]["text"]["content"]
            except (KeyError, IndexError):
                return "Untitled"
        return block.get("type", "unknown")

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int | None = None,
        autocommit: bool = True,
        cache_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> NotionFile:
        """Open a Notion page as a file."""
        if mode not in ["rb", "wb", "r", "w"]:
            msg = "Only read/write modes supported"
            raise ValueError(msg)

        path = self._strip_protocol(path)  # type: ignore
        page_id = self._get_page_id_from_path(path)
        is_binary = "b" in mode

        if "r" in mode:
            if not page_id:
                msg = f"Page not found: {path}"
                raise FileNotFoundError(msg)
            content = self._read_page_content(page_id, binary=is_binary)
            return NotionFile(content, mode, binary=is_binary)
        return NotionFile(
            b"" if is_binary else "",
            mode,
            write_callback=lambda data: self._write_page_content(path, data),
            binary=is_binary,
        )

    def _read_page_content(self, page_id: str, binary: bool = False) -> str | bytes:
        """Read content from a Notion page."""
        children = self.notion.blocks.children.list(block_id=page_id)
        blocks = children.get("results", [])  # type: ignore
        content: list[str] = []

        for block in blocks:
            match block["type"]:
                case "paragraph":
                    text = block["paragraph"].get("rich_text", [])
                    content.extend(t.get("plain_text", "") for t in text)
                case "file":
                    # Handle file blocks
                    file_url = block["file"].get("external", {}).get("url", "")
                    if file_url:
                        content.append(file_url)
                case _:
                    pass

        result = "\n".join(filter(None, content))
        return result.encode("utf-8") if binary else result

    def _write_page_content(self, path: str, content: str | bytes) -> None:
        """Write content to a Notion page."""
        page_title = path.split("/")[-1]
        properties = {"title": {"title": [{"text": {"content": page_title}}]}}

        page_id = self._get_page_id_from_path(path)

        try:
            if page_id:
                # Update existing page
                self.notion.pages.update(page_id=page_id, properties=properties)
                # Clear existing content
                children = self.notion.blocks.children.list(block_id=page_id)
                for block in children.get("results", []):  # type: ignore
                    self.notion.blocks.delete(block_id=block["id"])
            else:
                # Create new page
                response = self.notion.pages.create(
                    parent={"type": "page_id", "page_id": self.parent_page_id},
                    properties=properties,
                )
                page_id = response["id"]  # type: ignore
                assert page_id
                self._path_cache[path] = page_id

            # Convert content to string if it's bytes
            if isinstance(content, bytes):
                content = content.decode("utf-8")

            # Split content into chunks and create paragraph blocks
            chunks = [content[i : i + 2000] for i in range(0, len(content), 2000)]
            for chunk in chunks:
                self.notion.blocks.children.append(
                    block_id=page_id,
                    children=[
                        {
                            "type": "paragraph",
                            "paragraph": {
                                "rich_text": [
                                    {"type": "text", "text": {"content": chunk}}
                                ]
                            },
                        }
                    ],
                )
        except Exception as e:
            msg = f"Failed to write content: {e!s}"
            raise OSError(msg) from e


class NotionFile:
    def __init__(
        self,
        content: str | bytes,
        mode: str,
        write_callback: Callable[..., Any] | None = None,
        binary: bool = False,
    ):
        self.content = (
            content
            if isinstance(content, bytes)
            else content.encode("utf-8")
            if binary
            else content
        )
        self.mode = mode
        self.write_callback = write_callback
        self.binary = binary
        self.buffer = (
            io.BytesIO(
                self.content
                if isinstance(self.content, bytes)
                else self.content.encode("utf-8")
            )
            if binary
            else io.StringIO(
                self.content
                if isinstance(self.content, str)
                else self.content.decode("utf-8")
            )
        )
        self._closed = False

    def readable(self) -> bool:
        return "r" in self.mode

    def writable(self) -> bool:
        return "w" in self.mode

    def seekable(self) -> bool:
        return True

    @property
    def closed(self) -> bool:
        return self._closed

    def tell(self) -> int:
        if self._closed:
            msg = "I/O operation on closed file."
            raise ValueError(msg)
        return self.buffer.tell()

    def seek(self, offset: int, whence: int = 0) -> int:
        if self._closed:
            msg = "I/O operation on closed file."
            raise ValueError(msg)
        return self.buffer.seek(offset, whence)

    def read(self, size: int = -1) -> str | bytes:
        if self._closed:
            msg = "I/O operation on closed file."
            raise ValueError(msg)
        if not self.readable():
            msg = "File not open for reading"
            raise OSError(msg)
        data = self.buffer.read(size)
        # Ensure correct type is returned based on mode
        if "b" in self.mode:
            return data if isinstance(data, bytes) else data.encode("utf-8")
        return data if isinstance(data, str) else data.decode("utf-8")

    def write(self, data: str | bytes) -> int:
        if self._closed:
            msg = "I/O operation on closed file."
            raise ValueError(msg)
        if not self.writable():
            msg = "File not open for writing"
            raise OSError(msg)
        if self.binary and isinstance(data, str):
            data = data.encode("utf-8")
        elif not self.binary and isinstance(data, bytes):
            data = data.decode("utf-8")
        return self.buffer.write(data)  # type: ignore[arg-type]

    def flush(self) -> None:
        if self._closed:
            msg = "I/O operation on closed file."
            raise ValueError(msg)
        if self.writable() and self.write_callback:
            value = self.buffer.getvalue()
            self.write_callback(value)
        self.buffer.flush()

    def close(self) -> None:
        if not self._closed:
            if self.writable() and self.write_callback:
                value = self.buffer.getvalue()
                self.write_callback(value)
            self.buffer.close()
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


if __name__ == "__main__":
    import os

    KEY = os.environ.get("NOTION_API_KEY")
    PARENT_PAGE_ID = os.environ.get("NOTION_PARENT_PAGE_ID")
    assert KEY
    assert PARENT_PAGE_ID

    fs = NotionFS(token=KEY, parent_page_id=PARENT_PAGE_ID)
    print(fs.ls("/"))
    with fs.open("/New Page", "w") as f:
        f.write("Hello from NotionFS!")
    # Read a page
    with fs.open("/New Page", "rb") as f:
        content = f.read()
        print(content)
