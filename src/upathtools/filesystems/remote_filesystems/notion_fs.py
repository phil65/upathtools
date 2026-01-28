"""Notion async filesystem implementation for upathtools."""

from __future__ import annotations

import io
import json
from typing import TYPE_CHECKING, Any

from upathtools.async_helpers import sync_wrapper
from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath, FileInfo


if TYPE_CHECKING:
    from upathtools.filesystems.base import CreationMode


class NotionInfo(FileInfo, total=False):
    """Info dict for Notion filesystem paths."""

    size: int
    created: str
    modified: str


class NotionPath(BaseUPath[NotionInfo]):
    """UPath implementation for Notion filesystem."""

    __slots__ = ()


class NotionFileSystem(BaseAsyncFileSystem[NotionPath, NotionInfo]):
    """Async filesystem for Notion pages.

    This filesystem provides access to Notion pages as files,
    allowing you to read, write, and manage pages through the
    Notion API.
    """

    protocol = "notion"
    upath_cls = NotionPath

    def __init__(self, token: str, parent_page_id: str, **kwargs: Any):
        """Initialize NotionFileSystem with a Notion integration token.

        Args:
            token: Notion integration token
            parent_page_id: ID of the parent page where new pages will be created
            kwargs: Keyword arguments passed to parent class
        """
        from notion_client import AsyncClient

        super().__init__(**kwargs)
        self.notion = AsyncClient(auth=token)
        self._token = token
        self.parent_page_id = parent_page_id
        self._path_cache: dict[str, str] = {}

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, Any]:
        path = path.removeprefix("notion://")
        token, parent_page_id = path.split(":")
        return {"token": token, "parent_page_id": parent_page_id}

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        """Check if a path exists."""
        from notion_client import APIResponseError

        stripped = self._strip_protocol(path)
        assert isinstance(stripped, str)
        try:
            return await self._get_page_id_from_path(stripped) is not None
        except APIResponseError:
            return False

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        """Create a new page (folder) in Notion."""
        stripped = self._strip_protocol(path)
        assert isinstance(stripped, str)
        parts = [p for p in stripped.split("/") if p]

        parent_id = self.parent_page_id
        current_path = ""

        for part in parts:
            current_path += "/" + part
            existing_id = await self._get_page_id_from_path(current_path)

            if existing_id:
                parent_id = existing_id
                continue

            response = await self.notion.pages.create(
                parent={"type": "page_id", "page_id": parent_id},
                properties={"title": {"title": [{"text": {"content": part}}]}},
            )

            new_id = response["id"]
            self._path_cache[current_path] = new_id
            parent_id = new_id

    async def _makedirs(self, path: str, exist_ok: bool = False) -> None:
        """Create a directory and any parent directories."""
        path = self._strip_protocol(path)  # type: ignore

        if await self._get_page_id_from_path(path):
            if not exist_ok:
                msg = f"Path already exists: {path}"
                raise OSError(msg)
            return

        parts = [p for p in path.split("/") if p]
        current_path = ""
        parent_id = self.parent_page_id

        for part in parts:
            current_path += "/" + part
            existing_id = await self._get_page_id_from_path(current_path)

            if existing_id:
                parent_id = existing_id
                continue

            try:
                response = await self.notion.pages.create(
                    parent={"type": "page_id", "page_id": parent_id},
                    properties={"title": {"title": [{"text": {"content": part}}]}},
                )
                new_id = response["id"]
                self._path_cache[current_path] = new_id
                parent_id = new_id
            except Exception as e:
                if not exist_ok:
                    msg = f"Failed to create directory: {e!s}"
                    raise OSError(msg) from e

    async def _rm(self, path: str, **kwargs: Any) -> None:
        """Remove (archive) a page and its children."""
        page_id = await self._get_page_id_from_path(self._strip_protocol(path))  # type: ignore
        if not page_id:
            msg = f"Path not found: {path}"
            raise FileNotFoundError(msg)

        await self.notion.pages.update(page_id=page_id, archived=True)
        self._path_cache = {k: v for k, v in self._path_cache.items() if not k.startswith(path)}

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        """Remove a file (alias for rm)."""
        await self._rm(path, **kwargs)

    async def _rmdir(self, path: str, **kwargs: Any) -> None:
        """Remove a directory (page that may contain other pages)."""
        await self._rm(path, **kwargs)

    async def _get_page_id_from_path(self, path: str) -> str | None:
        """Convert a path to a Notion page ID."""
        if path in self._path_cache:
            return self._path_cache[path]

        path = path.strip("/")

        if not path:
            return None

        parts = path.split("/")
        current_id = self.parent_page_id
        current_path = ""

        for part in parts:
            current_path += "/" + part

            if current_path in self._path_cache:
                current_id = self._path_cache[current_path]
                continue

            found = False
            _filter = {"property": "object", "value": "page"}
            response = await self.notion.search(query=part, filter=_filter)
            results = response.get("results", [])

            for page in results:
                page_title = (
                    page
                    .get("properties", {})
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

    async def _ls(
        self, path: str, detail: bool = False, **kwargs: Any
    ) -> list[str] | list[NotionInfo]:
        """List contents of a path."""
        path = self._strip_protocol(path)  # type: ignore

        if not path or path == "/":
            children = await self.notion.blocks.children.list(block_id=self.parent_page_id)
            results = children.get("results", [])

            if not results:
                return []

            if detail:
                return [
                    NotionInfo(
                        name=self._get_block_title(result),
                        size=len(json.dumps(result)),
                        type=result["type"],
                        created=result.get("created_time"),
                        modified=result.get("last_edited_time"),
                    )
                    for result in results
                    if result["type"] == "child_page"
                ]
            return [
                self._get_block_title(result)
                for result in results
                if result["type"] == "child_page"
            ]

        page_id = await self._get_page_id_from_path(path)
        if not page_id:
            msg = f"Path not found: {path}"
            raise FileNotFoundError(msg)

        children = await self.notion.blocks.children.list(block_id=page_id)
        results = children.get("results", [])

        if not results:
            return []

        if detail:
            return [
                NotionInfo(
                    name=self._get_block_title(block),
                    size=len(json.dumps(block)),
                    type=block["type"],
                    created=block.get("created_time"),
                    modified=block.get("last_edited_time"),
                )
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

        return NotionFile(self, path, mode)

    async def _cat_file(self, path: str, **kwargs: Any) -> bytes:
        """Read file content."""
        path = self._strip_protocol(path)  # type: ignore
        page_id = await self._get_page_id_from_path(path)
        if not page_id:
            msg = f"Page not found: {path}"
            raise FileNotFoundError(msg)
        content = await self._read_page_content(page_id)
        return content if isinstance(content, bytes) else content.encode("utf-8")

    async def _pipe_file(
        self,
        path: str,
        value: bytes,
        mode: CreationMode = "overwrite",
        **kwargs: Any,
    ) -> None:
        """Write content to a file."""
        path = self._strip_protocol(path)  # type: ignore
        await self._write_page_content(path, value)

    async def _read_page_content(self, page_id: str) -> str:
        """Read content from a Notion page."""
        children = await self.notion.blocks.children.list(block_id=page_id)
        blocks = children.get("results", [])
        content: list[str] = []

        for block in blocks:
            match block["type"]:
                case "paragraph":
                    text = block["paragraph"].get("rich_text", [])
                    content.extend(t.get("plain_text", "") for t in text)
                case "file":
                    file_url = block["file"].get("external", {}).get("url", "")
                    if file_url:
                        content.append(file_url)
                case _:
                    pass

        return "\n".join(filter(None, content))

    async def _write_page_content(self, path: str, content: str | bytes) -> None:
        """Write content to a Notion page."""
        page_title = path.split("/")[-1]
        properties = {"title": {"title": [{"text": {"content": page_title}}]}}

        page_id = await self._get_page_id_from_path(path)

        try:
            if page_id:
                await self.notion.pages.update(page_id=page_id, properties=properties)
                children = await self.notion.blocks.children.list(block_id=page_id)
                for block in children.get("results", []):
                    await self.notion.blocks.delete(block_id=block["id"])
            else:
                response = await self.notion.pages.create(
                    parent={"type": "page_id", "page_id": self.parent_page_id},
                    properties=properties,
                )
                page_id = response["id"]
                assert page_id
                self._path_cache[path] = page_id

            if isinstance(content, bytes):
                content = content.decode("utf-8")

            chunks = [content[i : i + 2000] for i in range(0, len(content), 2000)]
            for chunk in chunks:
                await self.notion.blocks.children.append(
                    block_id=page_id,
                    children=[
                        {
                            "type": "paragraph",
                            "paragraph": {
                                "rich_text": [{"type": "text", "text": {"content": chunk}}]
                            },
                        }
                    ],
                )
        except Exception as e:
            msg = f"Failed to write content: {e!s}"
            raise OSError(msg) from e

    async def _isdir(self, path: str) -> bool:
        """Check if path is a directory (has child pages)."""
        path = self._strip_protocol(path)  # type: ignore

        if not path or path == "/":
            return True

        page_id = await self._get_page_id_from_path(path)
        if not page_id:
            return False

        # Check if page has child pages
        children = await self.notion.blocks.children.list(block_id=page_id)
        results = children.get("results", [])
        return any(block["type"] == "child_page" for block in results)

    async def _info(self, path: str, **kwargs: Any) -> NotionInfo:
        """Get info about a page."""
        path = self._strip_protocol(path)  # type: ignore

        if not path or path == "/":
            return NotionInfo(name="/", size=0, type="directory")

        page_id = await self._get_page_id_from_path(path)
        if not page_id:
            msg = f"Path not found: {path}"
            raise FileNotFoundError(msg)

        page = await self.notion.pages.retrieve(page_id=page_id)
        return NotionInfo(
            name=path.split("/")[-1],
            size=len(json.dumps(page)),
            type="file",
            created=page.get("created_time"),
            modified=page.get("last_edited_time"),
        )

    # Sync wrappers
    ls = sync_wrapper(_ls)
    exists = sync_wrapper(_exists)  # pyright: ignore[reportAssignmentType]
    mkdir = sync_wrapper(_mkdir)
    makedirs = sync_wrapper(_makedirs)
    rm = sync_wrapper(_rm)
    rm_file = sync_wrapper(_rm_file)
    rmdir = sync_wrapper(_rmdir)
    cat_file = sync_wrapper(_cat_file)  # pyright: ignore[reportAssignmentType]
    pipe_file = sync_wrapper(_pipe_file)
    info = sync_wrapper(_info)
    isdir = sync_wrapper(_isdir)


class NotionFile:
    """File-like object for Notion pages."""

    def __init__(self, fs: NotionFileSystem, path: str, mode: str) -> None:
        self.fs = fs
        stripped = fs._strip_protocol(path)
        self.path = stripped if isinstance(stripped, str) else stripped[0]
        self.mode = mode
        self.binary = "b" in mode
        self._buffer: io.BytesIO | io.StringIO
        self._closed = False
        self._loaded = False

        if self.binary:
            self._buffer = io.BytesIO()
        else:
            self._buffer = io.StringIO()

    async def _ensure_loaded(self) -> None:
        """Load content if reading and not yet loaded."""
        if self._loaded or "w" in self.mode:
            return

        page_id = await self.fs._get_page_id_from_path(self.path)
        if not page_id:
            msg = f"Page not found: {self.path}"
            raise FileNotFoundError(msg)

        content = await self.fs._read_page_content(page_id)
        if self.binary:
            self._buffer = io.BytesIO(content.encode("utf-8"))
        else:
            self._buffer = io.StringIO(content)
        self._loaded = True

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
        return self._buffer.tell()

    def seek(self, offset: int, whence: int = 0) -> int:
        if self._closed:
            msg = "I/O operation on closed file."
            raise ValueError(msg)
        return self._buffer.seek(offset, whence)

    async def read(self, size: int = -1) -> str | bytes:
        if self._closed:
            msg = "I/O operation on closed file."
            raise ValueError(msg)
        if not self.readable():
            msg = "File not open for reading"
            raise OSError(msg)

        await self._ensure_loaded()
        data = self._buffer.read(size)

        if self.binary:
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

        return self._buffer.write(data)  # type: ignore[arg-type]

    async def flush(self) -> None:
        if self._closed:
            msg = "I/O operation on closed file."
            raise ValueError(msg)
        if self.writable():
            value = self._buffer.getvalue()
            await self.fs._write_page_content(self.path, value)  # type: ignore
        self._buffer.flush()

    async def close(self) -> None:
        if not self._closed:
            if self.writable():
                value = self._buffer.getvalue()
                await self.fs._write_page_content(self.path, value)  # type: ignore
            self._buffer.close()
            self._closed = True

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()


if __name__ == "__main__":
    import asyncio
    import os

    async def main():
        key = os.environ.get("NOTION_API_KEY")
        parent_page_id = os.environ.get("NOTION_PARENT_PAGE_ID")
        assert key
        assert parent_page_id

        fs = NotionFileSystem(token=key, parent_page_id=parent_page_id)
        pages = await fs._ls("/")
        print(pages)

    asyncio.run(main())
