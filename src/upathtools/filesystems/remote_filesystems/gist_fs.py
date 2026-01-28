"""GitHub Gist filesystem implementation with async support using httpx."""

from __future__ import annotations

import base64
import contextlib
import io
import logging
import os
from typing import TYPE_CHECKING, Any, Literal, overload
import weakref

from fsspec.utils import infer_storage_options

from upathtools.async_helpers import sync, sync_wrapper
from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath, BufferedWriter, FileInfo


if TYPE_CHECKING:
    import httpx


class GistInfo(FileInfo, total=False):
    """Info dict for Gist filesystem paths."""

    size: int
    description: str | None
    created_at: str | None
    updated_at: str | None
    html_url: str | None
    git_pull_url: str | None
    git_push_url: str | None
    gist_id: str
    mime_type: str
    comments: int | None
    public: bool | None
    owner: str | None
    files_count: int | None
    files_preview: list[str] | None
    # File-specific fields from GitHub API
    filename: str | None
    raw_url: str | None
    language: str | None
    content: str | None
    truncated: bool | None


logger = logging.getLogger(__name__)


class GistPath(BaseUPath[GistInfo]):
    """UPath implementation for GitHub Gist filesystem."""

    __slots__ = ()


class GistFileSystem(BaseAsyncFileSystem[GistPath, GistInfo]):
    """Filesystem for accessing GitHub Gists files.

    Supports both individual gists and listing all gists for a user.
    Uses httpx for both synchronous and asynchronous operations.
    """

    protocol = "gist"
    upath_cls = GistPath
    gist_url = "https://api.github.com/gists/{gist_id}"
    gist_rev_url = "https://api.github.com/gists/{gist_id}/{sha}"
    user_gists_url = "https://api.github.com/users/{username}/gists"
    auth_gists_url = "https://api.github.com/gists"

    def __init__(
        self,
        gist_id: str | None = None,
        username: str | None = None,
        token: str | None = None,
        sha: str | None = None,
        timeout: int | None = None,
        asynchronous: bool = False,
        loop: Any = None,
        client_kwargs: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the filesystem.

        Args:
            gist_id: Specific gist ID to access
            username: GitHub username for listing all gists
            token: GitHub personal access token for authentication
            sha: Specific revision of a gist
            timeout: Connection timeout in seconds
            asynchronous: Whether to use async operations
            loop: Event loop for async operations
            client_kwargs: Additional arguments for httpx client
            **kwargs: Additional filesystem options
        """
        super().__init__(asynchronous=asynchronous, loop=loop, **kwargs)

        self.gist_id = gist_id
        self.username = username
        self.token = token or os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
        self.sha = sha
        self.timeout = timeout if timeout is not None else 60.0
        self.client_kwargs = client_kwargs or {}
        self._session: httpx.AsyncClient | None = None

        # We can work in two modes:
        # 1. Single gist mode (gist_id is provided)
        # 2. User gists mode (username is provided, or token alone for authenticated user)
        if not gist_id and not username and not self.token:
            msg = "Either gist_id, username, or token must be provided"
            raise ValueError(msg)

        self.headers = {"Authorization": f"token {self.token}"} if self.token else {}
        self.dircache: dict[str, Any] = {}

    @property
    def fsid(self) -> str:
        """Filesystem ID."""
        return "gist"

    async def set_session(self) -> httpx.AsyncClient:
        """Set up and return the httpx async client."""
        if self._session is None:
            import httpx

            self._session = httpx.AsyncClient(
                follow_redirects=True,
                timeout=self.timeout,
                headers=self.headers,
                **self.client_kwargs,
            )

            if not self.asynchronous:
                weakref.finalize(self, self.close_session, self.loop, self._session)

        return self._session

    @staticmethod
    def close_session(loop: Any, session: httpx.AsyncClient) -> None:
        """Close the httpx session."""
        if loop is not None and loop.is_running():
            with contextlib.suppress(TimeoutError, RuntimeError):
                sync(loop, session.aclose, timeout=0.1)

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        """Strip protocol prefix from path."""
        path = infer_storage_options(path).get("path", path)
        return path.lstrip("/")

    @classmethod
    def _get_kwargs_from_urls(cls, path: str) -> dict[str, Any]:
        """Parse URL into constructor kwargs."""
        so = infer_storage_options(path)
        out = {}

        if so.get("username"):
            out["username"] = so["username"]
        if so.get("password"):
            out["token"] = so["password"]
        if so.get("host"):
            # The host could be a gist ID or a username
            host = so["host"]
            # Simple heuristic: gist IDs are typically 32 hex chars
            if len(host) == 32 and all(c in "0123456789abcdef" for c in host.lower()):  # noqa: PLR2004
                out["gist_id"] = host
            else:
                out["username"] = host

        return out

    async def _fetch_gist_metadata(self, gist_id: str) -> dict[str, Any]:
        """Fetch metadata for a specific gist.

        Args:
            gist_id: ID of the gist to fetch

        Returns:
            Dictionary containing gist metadata

        Raises:
            FileNotFoundError: If gist is not found
        """
        session = await self.set_session()

        if self.sha:
            url = self.gist_rev_url.format(gist_id=gist_id, sha=self.sha)
        else:
            url = self.gist_url.format(gist_id=gist_id)

        logger.debug("Fetching gist metadata: %s", url)
        response = await session.get(url)
        if response.status_code == 404:  # noqa: PLR2004
            msg = f"Gist not found: {gist_id}@{self.sha or 'latest'}"
            raise FileNotFoundError(msg)

        response.raise_for_status()
        return response.json()

    async def _fetch_user_gists(self, page: int = 1, per_page: int = 100) -> list[dict[str, Any]]:
        """Fetch gists for a user.

        Args:
            page: Page number for pagination
            per_page: Number of gists per page

        Returns:
            List of gist metadata dictionaries

        Raises:
            FileNotFoundError: If user is not found
        """
        session = await self.set_session()

        params = {"page": page, "per_page": per_page}
        if self.username and not self.token:
            url = self.user_gists_url.format(username=self.username)
        else:
            url = self.auth_gists_url

        logger.debug("Fetching user gists: %s", url)
        response = await session.get(url, params=params)

        if response.status_code == 404:  # noqa: PLR2004
            msg = f"User not found: {self.username}"
            raise FileNotFoundError(msg)

        response.raise_for_status()
        return response.json()

    async def _get_gist_file_list(self, gist_id: str) -> list[GistInfo]:
        """Get list of files in a specific gist.

        Args:
            gist_id: ID of the gist

        Returns:
            List of detailed file metadata dictionaries
        """
        if gist_id in self.dircache:
            return self.dircache[gist_id]

        # Fetch the specific gist metadata
        meta = await self._fetch_gist_metadata(gist_id)
        out = [
            GistInfo(
                name=fname,
                type="file",
                size=finfo.get("size", 0),
                raw_url=finfo.get("raw_url"),
                language=finfo.get("language"),
                mime_type=finfo.get("type", "text/plain"),
                gist_id=gist_id,
                description=meta.get("description", ""),
                html_url=meta.get("html_url"),
                created_at=meta.get("created_at"),
                updated_at=meta.get("updated_at"),
                public=meta.get("public", False),
                truncated=finfo.get("truncated", False),
            )
            for fname, finfo in meta.get("files", {}).items()
            if finfo is not None
        ]
        self.dircache[gist_id] = out
        return out

    async def _get_all_gists(self) -> list[GistInfo]:
        """Get metadata for all gists of the user."""
        if "" in self.dircache:
            return self.dircache[""]

        gists = await self._fetch_user_gists(page=1, per_page=100)
        all_gists = gists.copy()
        page = 2
        while len(gists) == 100:  # noqa: PLR2004
            gists = await self._fetch_user_gists(page=page, per_page=100)
            all_gists.extend(gists)
            page += 1
        out = [
            GistInfo(
                gist_id=gist["id"],
                name=gist["id"],
                type="directory",
                description=gist.get("description", ""),
                created_at=gist.get("created_at"),
                updated_at=gist.get("updated_at"),
                html_url=gist.get("html_url"),
                git_pull_url=gist.get("git_pull_url"),
                git_push_url=gist.get("git_push_url"),
                comments=gist.get("comments", 0),
                public=gist.get("public", False),
                owner=gist.get("owner", {}).get("login") if gist.get("owner") else None,
                files_count=len(gist.get("files", {})),
                # Include truncated file names as a preview
                files_preview=list(gist.get("files", {}).keys()),
                size=sum(f.get("size", 0) for f in gist.get("files", {}).values()),
            )
            for gist in all_gists
        ]

        self.dircache[""] = out
        return out

    @overload
    async def _ls(
        self, path: str, detail: Literal[True] = ..., **kwargs: Any
    ) -> list[GistInfo]: ...

    @overload
    async def _ls(self, path: str, detail: Literal[False], **kwargs: Any) -> list[str]: ...

    async def _ls(
        self,
        path: str,
        detail: bool = True,
        **kwargs: Any,
    ) -> list[GistInfo] | list[str]:
        """List contents of path.

        Args:
            path: Path to list
            detail: Whether to include detailed information
            **kwargs: Additional arguments

        Returns:
            List of file/gist information or names

        Raises:
            FileNotFoundError: If path doesn't exist
        """
        path = self._strip_protocol(path or "")
        logger.debug("Listing path: %s (with gist_id: %s)", path, self.gist_id)

        # Different modes of operation
        if self.gist_id:
            # Single gist mode - always list files in this gist
            # Regardless of the path (root or specific file)
            results = await self._get_gist_file_list(self.gist_id)

            # If path is specified, filter to just that file
            if path:
                results = [f for f in results if f["name"] == path]
                if not results:
                    msg = f"File not found: {path}"
                    raise FileNotFoundError(msg)
        # User gists mode
        elif not path:
            # Root - list all gists
            results = await self._get_all_gists()
        else:
            # Specific gist - list its files
            parts = path.split("/", 1)
            gist_id = parts[0]

            try:
                results = await self._get_gist_file_list(gist_id)
            except FileNotFoundError:
                msg = f"Gist not found: {gist_id}"
                raise FileNotFoundError(msg)  # noqa: B904

            # If path includes a file, filter to just that file
            if len(parts) > 1 and parts[1]:
                file_name = parts[1]
                results = [f for f in results if f["name"] == file_name]
                if not results:
                    msg = f"File not found: {path}"
                    raise FileNotFoundError(msg)

        if detail:
            return results
        return [f["name"] for f in results]

    ls = sync_wrapper(_ls)

    async def _cat_file(
        self,
        path: str,
        start: int | None = None,
        end: int | None = None,
        **kwargs: Any,
    ) -> bytes:
        """Get contents of a file."""
        path = self._strip_protocol(path)
        if self.gist_id:
            gist_id = self.gist_id
            file_name = path
        else:  # Parse path into gist_id and file_name
            if "/" not in path:
                msg = f"Invalid file path: {path}"
                raise ValueError(msg)
            gist_id, file_name = path.split("/", 1)

        # Find file info in dircache
        files = await self._get_gist_file_list(gist_id)
        matches = [f for f in files if f["name"] == file_name]
        if not matches:
            msg = f"File not found: {path}"
            raise FileNotFoundError(msg)

        raw_url = matches[0].get("raw_url")
        if not raw_url:
            msg = f"No raw URL for file: {path}"
            raise FileNotFoundError(msg)

        session = await self.set_session()
        response = await session.get(raw_url)
        if response.status_code == 404:  # noqa: PLR2004
            msg = f"File not found: {path}"
            raise FileNotFoundError(msg)

        response.raise_for_status()
        content = response.content
        if start is not None or end is not None:
            start = start or 0
            end = min(end or len(content), len(content))
            content = content[start:end]
        return content

    cat_file = sync_wrapper(_cat_file)  # type: ignore

    async def _pipe_file(self, path: str, value: bytes, **kwargs: Any) -> None:
        """Write bytes to a file in a gist.

        Args:
            path: Path in format "gist_id/filename" or "filename" (for single gist mode)
            value: Content to write
            **kwargs: Additional keyword arguments
                gist_description: Optional description for new gists
                public: Whether the gist should be public (default: False)

        Raises:
            ValueError: If token is not provided for write operations
        """
        if not self.token:
            msg = "GitHub token is required for write operations"
            raise ValueError(msg)

        session = await self.set_session()
        path = self._strip_protocol(path)
        logger.debug("Writing to path: %s", path)
        # Parse the path into gist_id and filename
        if self.gist_id and "/" not in path:
            # Single gist mode with just a filename
            gist_id = self.gist_id
            filename = path
        else:
            # Path should include gist_id/filename
            if "/" not in path:
                msg = "Cannot create file without gist_id. Use 'gist_id/filename' format"
                raise ValueError(msg)
            gist_id, filename = path.split("/", 1)

        logger.debug("Resolved gist_id=%s, filename=%s", gist_id, filename)
        # Determine if we're updating an existing gist or creating a new one
        is_update = True
        try:
            await self._fetch_gist_metadata(gist_id)
        except FileNotFoundError:
            # Gist doesn't exist, create new one
            is_update = False
            logger.debug("Gist %s not found, will create new gist", gist_id)

        # Convert bytes to string content
        try:
            content = value.decode()
        except UnicodeDecodeError:
            # If content is binary, base64 encode it
            content = f"base64:{base64.b64encode(value).decode('ascii')}"

        files_data = {filename: {"content": content}}
        if is_update:
            # Update existing gist
            update_url = f"https://api.github.com/gists/{gist_id}"
            logger.debug("Updating existing gist: %s", update_url)
            data = {"files": files_data}
            response = await session.patch(update_url, json=data)
        else:
            # Create new gist
            create_url = "https://api.github.com/gists"
            logger.debug("Creating new gist: %s", create_url)
            description = kwargs.get("gist_description", "Gist created via GistFileSystem")
            public = kwargs.get("public", False)
            data = {"description": description, "public": public, "files": files_data}
            response = await session.post(create_url, json=data)

        if response.status_code >= 400:  # noqa: PLR2004
            logger.error("API error: %s %s", response.status_code, response.text)
            response.raise_for_status()

        # Invalidate cache for this gist
        self.dircache.pop(gist_id, None)

    pipe_file = sync_wrapper(_pipe_file)

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        """Delete a file from a gist.

        Args:
            path: Path in format "gist_id/filename" or "filename" (for single gist mode)
            **kwargs: Additional keyword arguments

        Raises:
            ValueError: If token is not provided for delete operations
        """
        if not self.token:
            msg = "GitHub token is required for delete operations"
            raise ValueError(msg)

        session = await self.set_session()
        path = self._strip_protocol(path)

        # Parse the path
        if self.gist_id and "/" not in path:
            gist_id = self.gist_id
            filename = path
        else:
            if "/" not in path:
                msg = "Cannot identify file without gist_id. Use 'gist_id/filename' format"
                raise ValueError(msg)
            gist_id, filename = path.split("/", 1)

        # To delete a file, we need to set its content to null in the API
        update_url = f"https://api.github.com/gists/{gist_id}"
        data = {"files": {filename: None}}  # Setting to null/None deletes the file

        response = await session.patch(update_url, json=data)
        response.raise_for_status()
        self.dircache.pop(gist_id, None)  # Invalidate cache for this gist

    rm_file = sync_wrapper(_rm_file)

    async def _rm(self, path: str, recursive: bool = False, **kwargs: Any) -> None:
        """Remove a file or entire gist.

        Args:
            path: Path to file or gist
            recursive: If True and path points to a gist, delete the entire gist
            **kwargs: Additional keyword arguments

        Raises:
            ValueError: If token is not provided for delete operations
        """
        if not self.token:
            msg = "GitHub token is required for delete operations"
            raise ValueError(msg)

        path = self._strip_protocol(path)
        # Determine if we're deleting a file or an entire gist
        if "/" in path or (self.gist_id and not path):
            # Path contains a filename or we're in single gist mode - delete file
            return await self._rm_file(path, **kwargs)

        # We're dealing with a gist ID directly
        gist_id = path if path else self.gist_id
        if not gist_id:
            msg = "No gist ID specified for deletion"
            raise ValueError(msg)

        if not recursive:
            msg = "Cannot delete a gist without recursive=True"
            raise ValueError(msg)

        # Delete the entire gist
        session = await self.set_session()
        delete_url = f"https://api.github.com/gists/{gist_id}"
        response = await session.delete(delete_url)
        response.raise_for_status()

        # Remove from cache
        self.dircache.pop(gist_id, None)
        self.dircache.pop("", None)  # Also invalidate root listing
        return None

    rm = sync_wrapper(_rm)

    async def _info(self, path: str, **kwargs: Any) -> GistInfo:
        """Get info about a path."""
        path = self._strip_protocol(path)
        if not path:
            return GistInfo(
                name="",
                type="directory",
                size=0,
                description="Root directory listing gists",
            )

        parts = path.split("/")
        if len(parts) == 1:
            if self.gist_id:
                # In single gist mode, this must be a file
                try:
                    files = await self._get_gist_file_list(self.gist_id)
                    matches = [f for f in files if f["name"] == path]
                    if not matches:
                        msg = f"File not found: {path}"
                        raise FileNotFoundError(msg)  # noqa: TRY301
                    file_info = matches[0]
                    return GistInfo(
                        name=file_info.get("name", path),
                        type="file",
                        size=file_info.get("size", 0),
                        description=file_info.get("description"),
                        filename=file_info.get("filename"),
                        raw_url=file_info.get("raw_url"),
                        language=file_info.get("language"),
                        content=file_info.get("content"),
                        truncated=file_info.get("truncated", False),
                    )
                except FileNotFoundError:
                    msg = f"File not found: {path}"
                    raise FileNotFoundError(msg)  # noqa: B904
            else:
                # In user gists mode, this is a gist ID
                try:
                    gists = await self._get_all_gists()
                    matches = [g for g in gists if g["name"] == parts[0]]
                    if not matches:
                        # Try to fetch the specific gist
                        try:
                            meta = await self._fetch_gist_metadata(parts[0])
                            return GistInfo(
                                name=parts[0],
                                type="directory",
                                description=meta.get("description", ""),
                                created_at=meta.get("created_at"),
                                updated_at=meta.get("updated_at"),
                                html_url=meta.get("html_url"),
                                git_pull_url=meta.get("git_pull_url"),
                                git_push_url=meta.get("git_push_url"),
                                comments=meta.get("comments", 0),
                                public=meta.get("public", False),
                                owner=meta.get("owner", {}).get("login")
                                if meta.get("owner")
                                else None,
                                files_count=len(meta.get("files", {})),
                                files_preview=list(meta.get("files", {}).keys()),
                                size=sum(f.get("size", 0) for f in meta.get("files", {}).values()),
                            )
                        except FileNotFoundError:
                            msg = f"Gist not found: {parts[0]}"
                            raise FileNotFoundError(msg)  # noqa: B904
                    file_info = matches[0]
                    return GistInfo(
                        name=file_info.get("name", parts[0]),
                        type="file",
                        size=file_info.get("size", 0),
                        description=file_info.get("description"),
                        filename=file_info.get("filename"),
                        raw_url=file_info.get("raw_url"),
                        language=file_info.get("language"),
                        content=file_info.get("content"),
                        truncated=file_info.get("truncated", False),
                    )
                except FileNotFoundError:
                    msg = f"Gist not found: {parts[0]}"
                    raise FileNotFoundError(msg)  # noqa: B904
        else:
            # This is a file within a gist
            gist_id = parts[0] if not self.gist_id else self.gist_id
            file_name = parts[1] if not self.gist_id else parts[0]

            try:
                files = await self._get_gist_file_list(gist_id)
                matches = [f for f in files if f["name"] == file_name]
                if not matches:
                    msg = f"File not found: {path}"
                    raise FileNotFoundError(msg)  # noqa: TRY301
                file_info = matches[0]
                return GistInfo(
                    name=file_info.get("name", file_name),
                    type="file",
                    size=file_info.get("size", 0),
                    description=file_info.get("description"),
                    filename=file_info.get("filename"),
                    raw_url=file_info.get("raw_url"),
                    language=file_info.get("language"),
                    content=file_info.get("content"),
                    truncated=file_info.get("truncated", False),
                )
            except FileNotFoundError:
                msg = f"File not found: {path}"
                raise FileNotFoundError(msg)  # noqa: B904

    info = sync_wrapper(_info)

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        """Check if a path exists."""
        try:
            await self._info(path, **kwargs)
        except FileNotFoundError:
            return False
        else:
            return True

    exists = sync_wrapper(_exists)  # pyright: ignore

    async def _isdir(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a directory."""
        if not path:
            return True  # Root is always a directory

        try:
            info = await self._info(path, **kwargs)
            return info["type"] == "directory"  # pyright: ignore[reportTypedDictNotRequiredAccess]
        except FileNotFoundError:
            return False

    isdir = sync_wrapper(_isdir)

    async def _isfile(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a file."""
        try:
            info = await self._info(path, **kwargs)
            return info["type"] == "file"  # pyright: ignore[reportTypedDictNotRequiredAccess]
        except FileNotFoundError:
            return False

    isfile = sync_wrapper(_isfile)

    def _open(self, path: str, mode: str = "rb", **kwargs: Any) -> io.BytesIO | BufferedWriter:
        """Open a file.

        Args:
            path: Path to the file
            mode: File mode ('rb' for reading, 'wb' for writing)
            **kwargs: Additional arguments for write operations
                gist_description: Optional description for new gists
                public: Whether the gist should be public (default: False)

        Returns:
            File-like object for reading or writing

        Raises:
            ValueError: If token is not provided for write operations
            NotImplementedError: If mode is not supported
        """
        if "r" in mode:
            content = self.cat_file(path)
            assert isinstance(content, bytes)
            return io.BytesIO(content)
        if "w" in mode:
            if not self.token:
                msg = "GitHub token is required for write operations"
                raise ValueError(msg)

            buffer = io.BytesIO()
            return BufferedWriter(buffer, self, path, **kwargs)
        msg = f"Mode {mode} not supported"
        raise NotImplementedError(msg)

    def invalidate_cache(self, path: str | None = None) -> None:
        """Clear the cache."""
        if path is None:
            self.dircache.clear()
        else:
            path = self._strip_protocol(path)
            if self.gist_id:
                self.dircache.pop(self.gist_id, None)
            elif not path or path == "/":
                self.dircache.pop("", None)
            else:
                parts = path.split("/")
                if len(parts) >= 1:
                    self.dircache.pop(parts[0], None)


if __name__ == "__main__":
    import asyncio

    logging.basicConfig(level=logging.INFO)
    print(f"Environment GITHUB_TOKEN set: {'GITHUB_TOKEN' in os.environ}")
    fs = GistFileSystem(username="phil65")
    print("\nListing files with filesystem.ls():")

    async def main() -> None:
        upath = fs.get_upath("")
        async for p in upath.aiterdir():
            print(p)

    asyncio.run(main())
    # test_filename = "test_file2.py"
    # print(f"\nWriting to {test_filename}")
    # fs.pipe_file(test_filename, b"test content")
    # print("Write successful")
    # print("\nReading file:")
    # content = fs.cat_file(test_filename)
    # print(f"Content: {content}")
    #
    #
    # fs = GistFileSystem(username="phil65")
    # files = fs.ls("")
