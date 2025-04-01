"""GitHub Gist filesystem implementation with async support using httpx."""

from __future__ import annotations

import asyncio
import contextlib
import io
import logging
from typing import TYPE_CHECKING, Any, Literal, overload
import weakref

from fsspec import register_implementation
from fsspec.asyn import AsyncFileSystem, sync, sync_wrapper
from fsspec.utils import infer_storage_options
from upath import UPath, registry


if TYPE_CHECKING:
    import httpx


logger = logging.getLogger(__name__)


class GistPath(UPath):
    """UPath implementation for GitHub Gist filesystem."""

    __slots__ = ()


class GistFileSystem(AsyncFileSystem):
    """Filesystem for accessing GitHub Gists files.

    Supports both individual gists and listing all gists for a user.
    Uses httpx for both synchronous and asynchronous operations.
    """

    protocol = "gist"
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
        client_kwargs: dict | None = None,
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
        self.token = token
        self.sha = sha
        self.timeout = timeout if timeout is not None else 60.0
        self.client_kwargs = client_kwargs or {}
        self._session: httpx.AsyncClient | None = None

        # We can work in two modes:
        # 1. Single gist mode (gist_id is provided)
        # 2. User gists mode (username is provided, or token alone for authenticated user)
        if not gist_id and not username and not token:
            msg = "Either gist_id, username, or token must be provided"
            raise ValueError(msg)

        # Setup authentication
        if token:
            self.headers = {"Authorization": f"token {token}"}
        else:
            self.headers = {}

        # Initialize cache
        self.dircache: dict[str, Any] = {}

    def _make_path(self, path: str) -> UPath:
        """Create a path object from string."""
        return GistPath(path)

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
        """Fetch metadata for a specific gist."""
        session = await self.set_session()

        if self.sha:
            url = self.gist_rev_url.format(gist_id=gist_id, sha=self.sha)
        else:
            url = self.gist_url.format(gist_id=gist_id)

        response = await session.get(url)
        if response.status_code == 404:  # noqa: PLR2004
            msg = f"Gist not found: {gist_id}@{self.sha or 'latest'}"
            raise FileNotFoundError(msg)

        response.raise_for_status()
        return response.json()

    async def _fetch_user_gists(
        self, page: int = 1, per_page: int = 100
    ) -> list[dict[str, Any]]:
        """Fetch gists for a user."""
        session = await self.set_session()

        params = {"page": page, "per_page": per_page}
        if self.username and not self.token:
            url = self.user_gists_url.format(username=self.username)
        else:
            url = self.auth_gists_url
        response = await session.get(url, params=params)

        if response.status_code == 404:  # noqa: PLR2004
            msg = f"User not found: {self.username}"
            raise FileNotFoundError(msg)

        response.raise_for_status()
        return response.json()

    async def _get_gist_file_list(self, gist_id: str) -> list[dict[str, Any]]:
        """Get list of files in a specific gist."""
        if gist_id in self.dircache:
            return self.dircache[gist_id]
        meta = await self._fetch_gist_metadata(gist_id)
        files = meta.get("files", {})
        out = []
        for fname, finfo in files.items():
            if finfo is None:
                continue

            out.append({
                "name": fname,
                "type": "file",
                "size": finfo.get("size", 0),
                "raw_url": finfo.get("raw_url"),
                "gist_id": gist_id,
                "description": meta.get("description", ""),
                "created_at": meta.get("created_at"),
                "updated_at": meta.get("updated_at"),
            })

        self.dircache[gist_id] = out
        return out

    async def _get_all_gists(self) -> list[dict[str, Any]]:
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

        out = []
        for gist in all_gists:
            gist_entry = {
                "name": gist["id"],
                "type": "directory",
                "description": gist.get("description", ""),
                "created_at": gist.get("created_at"),
                "updated_at": gist.get("updated_at"),
                "files": len(gist.get("files", {})),
                "public": gist.get("public", False),
            }
            out.append(gist_entry)

        self.dircache[""] = out
        return out

    @overload
    async def _ls(
        self,
        path: str = "",
        detail: Literal[True] = True,
        **kwargs: Any,
    ) -> list[dict[str, Any]]: ...

    @overload
    async def _ls(
        self,
        path: str = "",
        detail: Literal[False] = False,
        **kwargs: Any,
    ) -> list[str]: ...

    async def _ls(
        self,
        path: str = "",
        detail: bool = True,
        **kwargs: Any,
    ) -> list[dict[str, Any]] | list[str]:
        """List contents of path."""
        path = self._strip_protocol(path or "")

        # Handle different modes of operation
        if self.gist_id:
            # Single gist mode
            results = await self._get_gist_file_list(self.gist_id)
        # User gists mode
        elif path == "":
            # Root - list all gists
            results = await self._get_all_gists()
        else:
            # Specific gist - list its files
            gist_id = path.split("/")[0]
            results = await self._get_gist_file_list(gist_id)

            # If path includes a file, filter to just that file
            if "/" in path:
                file_name = path.split("/", 1)[1]
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

        # Parse path into gist_id and file_name
        if self.gist_id:
            gist_id = self.gist_id
            file_name = path
        else:
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

        file_info = matches[0]
        raw_url = file_info["raw_url"]

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

    async def _info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Get info about a path."""
        path = self._strip_protocol(path)
        if not path:
            return {"name": "", "type": "directory", "size": 0}
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
                    return matches[0]
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
                            return {
                                "name": parts[0],
                                "type": "directory",
                                "description": meta.get("description", ""),
                                "created_at": meta.get("created_at"),
                                "updated_at": meta.get("updated_at"),
                                "files": len(meta.get("files", {})),
                                "public": meta.get("public", False),
                            }
                        except FileNotFoundError:
                            msg = f"Gist not found: {parts[0]}"
                            raise FileNotFoundError(msg)  # noqa: B904
                    return matches[0]
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
                return matches[0]
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
            return info["type"] == "directory"
        except FileNotFoundError:
            return False

    isdir = sync_wrapper(_isdir)

    async def _isfile(self, path: str, **kwargs: Any) -> bool:
        """Check if path is a file."""
        try:
            info = await self._info(path, **kwargs)
            return info["type"] == "file"
        except FileNotFoundError:
            return False

    isfile = sync_wrapper(_isfile)

    def _open(
        self,
        path: str,
        mode: str = "rb",
        **kwargs: Any,
    ) -> io.BytesIO:
        """Open a file."""
        if mode != "rb":
            msg = "Write mode not supported"
            raise NotImplementedError(msg)

        content = self.cat_file(path)
        return io.BytesIO(content)  # pyright: ignore

    async def open_async(
        self,
        path: str,
        mode: str = "rb",
        **kwargs: Any,
    ) -> io.BytesIO:
        """Open a file asynchronously."""
        if mode != "rb":
            msg = "Write mode not supported"
            raise NotImplementedError(msg)

        content = await self._cat_file(path, **kwargs)
        return io.BytesIO(content)

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


register_implementation("gist", GistFileSystem, clobber=True)
registry.register_implementation("gist", GistPath, clobber=True)


if __name__ == "__main__":
    # Example usage
    import asyncio

    async def main():
        fs = GistFileSystem(gist_id="74192a94041f3d02ed910551670eb838")
        files = await fs._ls()
        print("Files in gist:")
        for file in files:
            print(f"- {file['name']} ({file['size']} bytes)")
            content = await fs._cat_file(file["name"])
            text = content.decode()
            print(f"  First 100 chars: {text[:100]}")
        user_fs = GistFileSystem(username="phil65")
        gists = await user_fs._ls()
        print("\nGists for user:")
        for gist in gists:
            print(f"- {gist['name']}: {gist.get('description', '')}")
            gist_files = await user_fs._ls(gist["name"])
            print(f"  Files: {', '.join(f['name'] for f in gist_files)}")

    asyncio.run(main())
