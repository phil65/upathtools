"""HTTP filesystem implementation with async support using httpx."""

from __future__ import annotations

import asyncio
import contextlib
from copy import copy
import io
import logging
import os
import re
from typing import TYPE_CHECKING, Any, Literal, overload
import weakref

from fsspec.asyn import AbstractAsyncStreamedFile
from fsspec.caching import AllBytes
from fsspec.callbacks import DEFAULT_CALLBACK
from fsspec.exceptions import FSTimeoutError
from fsspec.spec import AbstractBufferedFile
from fsspec.utils import (
    DEFAULT_BLOCK_SIZE,
    glob_translate,
    isfilelike,
    nullcontext,
    tokenize,
)

from upathtools.async_helpers import sync, sync_wrapper
from upathtools.filesystems.base import BaseAsyncFileSystem, BaseUPath, FileInfo


if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Mapping

    import httpx
    from httpx import Response
    from yarl import URL

    from upathtools.filesystems.base import CreationMode


class HttpInfo(FileInfo, total=False):
    """Info dict for HTTP filesystem paths."""

    size: int | None
    mimetype: str | None
    partial: bool | None
    url: str | None
    ETag: str | None
    Content_MD5: str | None
    Digest: str | None
    # Additional fields that may be present
    last_modified: str | None
    content_type: str | None
    etag: str | None


# URL pattern in HTML href tags
HREF_PATTERN = re.compile(r"""<(a|A)\s+(?:[^>]*?\s+)?(href|HREF)=["'](?P<url>[^"']+)""")
# URL pattern for direct links
URL_PATTERN = re.compile(r"""(?P<url>http[s]?://[-a-zA-Z0-9@:%_+.~#?&/=]+)""")
logger = logging.getLogger("fsspec.http")


class HttpPath(BaseUPath[HttpInfo]):
    """UPath implementation for CLI filesystems."""

    __slots__ = ()


async def get_client(**kwargs: Any) -> httpx.AsyncClient:
    """Create and return an async HTTP client."""
    import httpx

    return httpx.AsyncClient(follow_redirects=True, **kwargs)


class HTTPFileSystem(BaseAsyncFileSystem[HttpPath, HttpInfo]):
    """Simple File-System for fetching data via HTTP(S)."""

    sep = "/"
    # protocol = "http"
    upath_cls = HttpPath

    def __init__(
        self,
        simple_links: bool = True,
        block_size: int | None = None,
        same_scheme: bool = True,
        size_policy: str | None = None,
        cache_type: str = "bytes",
        cache_options: dict[str, Any] | None = None,
        asynchronous: bool = False,
        loop: Any = None,
        client_kwargs: dict[str, Any] | None = None,
        get_client: Any = get_client,
        encoded: bool = False,
        **storage_options: Any,
    ) -> None:
        """Initialize the filesystem."""
        super().__init__(asynchronous=asynchronous, loop=loop, **storage_options)
        self.block_size = block_size or DEFAULT_BLOCK_SIZE
        self.simple_links = simple_links
        self.same_schema = same_scheme
        self.cache_type = cache_type
        self.cache_options = cache_options
        self.client_kwargs = client_kwargs or {}
        self.get_client = get_client
        self.encoded = encoded
        self._session = None

        # Clean caching-related parameters from storage_options
        request_options = copy(storage_options)
        self.use_listings_cache = request_options.pop("use_listings_cache", False)
        request_options.pop("listings_expiry_time", None)
        request_options.pop("max_paths", None)
        request_options.pop("skip_instance_cache", None)
        self.kwargs = request_options

    @property
    def fsid(self) -> str:
        return "http"

    def encode_url(self, url: str) -> URL:
        from yarl import URL

        return URL(url, encoded=self.encoded)

    @staticmethod
    def close_session(loop: Any, session: httpx.AsyncClient) -> None:
        if loop is not None and loop.is_running():
            try:
                sync(loop, session.aclose, timeout=0.1)
            except (TimeoutError, FSTimeoutError, NotImplementedError, RuntimeError):
                # If we can't close it properly, just let it be garbage collected
                pass
            else:
                return

    async def set_session(self) -> httpx.AsyncClient:
        if self._session is None:
            self._session = await self.get_client(**self.client_kwargs)
            assert self._session
            if not self.asynchronous:
                weakref.finalize(self, self.close_session, self.loop, self._session)
        return self._session

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        """For HTTP, we always want to keep the full URL."""
        return path

    @classmethod
    def _parent(cls, path: str) -> str:
        par = super()._parent(path)
        return par if len(par) > 7 else ""  # noqa: PLR2004

    async def _get_decompressor(self, response: httpx.Response) -> Callable[[bytes], bytes] | None:
        """Get decompressor based on Content-Encoding header."""
        encoding = response.headers.get("Content-Encoding", "").lower()
        if encoding == "gzip":
            import gzip

            return gzip.decompress
        if encoding == "deflate":
            import zlib

            return zlib.decompress
        if encoding == "br":
            try:
                import brotli

            except ImportError:
                msg = "brotli module is required for brotli decompression"
                raise ImportError(msg)  # noqa: B904
            else:
                return brotli.decompress

        return None

    async def _ls_real(
        self,
        url: str,
        detail: bool = True,
        **kwargs: Any,
    ) -> list[str] | list[dict[str, Any]]:
        """List contents of a URL path."""
        from urllib.parse import urlparse

        kw = self.kwargs.copy()
        kw.update(kwargs)
        logger.debug("URL: %s", url)
        session = await self.set_session()

        base_url = url.rstrip("/")
        r = await session.get(str(self.encode_url(base_url)), **kw)
        r.raise_for_status()

        text = r.text
        out = set()

        # Extract links
        links = URL_PATTERN.findall(text) if self.simple_links else []

        href_matches = HREF_PATTERN.findall(text)
        links.extend(m[2] for m in href_matches)

        # Process links
        parts = urlparse(base_url)
        base_path = parts.path.rstrip("/")

        for link in links:
            # Skip parent directory links and relative navigation
            if (
                link in ["..", "../"]
                or link == "/data/"
                or "[To Parent Directory]" in link
                or (
                    link.startswith("/")
                    and os.path.dirname(base_path).rstrip("/") == link.rstrip("/")  # noqa
                )
            ):
                continue

            if link.startswith("/"):
                link = f"{parts.scheme}://{parts.netloc}{link}"
            elif not link.startswith(("http://", "https://")):
                link = f"{base_url}/{link.lstrip('/')}"

            link_parts = urlparse(link)
            if link_parts.netloc != parts.netloc:
                link = f"{parts.scheme}://{parts.netloc}{link_parts.path}"

            out.add(link)

        if detail:
            return [
                {
                    "name": u,
                    "size": None,
                    "type": "directory" if u.endswith("/") else "file",
                }
                for u in sorted(out)
            ]
        return sorted(out)

    @overload
    async def _ls(
        self, path: str, detail: Literal[True] = True, **kwargs: Any
    ) -> list[HttpInfo]: ...

    @overload
    async def _ls(self, path: str, detail: Literal[False] = False, **kwargs: Any) -> list[str]: ...

    async def _ls(
        self, path: str, detail: bool = True, **kwargs: Any
    ) -> list[HttpInfo] | list[str]:
        """List directory contents."""
        if self.use_listings_cache and path in self.dircache:
            out = self.dircache[path]
        else:
            try:
                out = await self._ls_real(path, detail=detail, **kwargs)
                if not out:
                    raise FileNotFoundError(path)  # noqa: TRY301
                if self.use_listings_cache:
                    self.dircache[path] = out
            except Exception as e:
                raise FileNotFoundError(path) from e

        if detail:
            return out  # pyright: ignore[reportReturnType]
        return sorted(out)  # pyright: ignore[reportArgumentType]

    ls = sync_wrapper(_ls)

    def _raise_not_found_for_status(self, response: httpx.Response, url: str) -> None:
        """Raise FileNotFoundError for 404s, otherwise raises HTTP errors."""
        if response.status_code == 404:  # noqa: PLR2004
            raise FileNotFoundError(url)
        response.raise_for_status()

    async def _cat_file(
        self,
        url: str,
        start: int | None = None,
        end: int | None = None,
        **kwargs: Any,
    ) -> bytes:
        kw = self.kwargs.copy()
        kw.update(kwargs)
        logger.debug(url)

        if start is not None or end is not None:
            if start == end:
                return b""
            headers = kw.pop("headers", {}).copy()
            headers["Range"] = await self._process_limits(url, start, end)
            kw["headers"] = headers

        session = await self.set_session()
        r = await session.get(str(self.encode_url(url)), **kw)
        self._raise_not_found_for_status(r, url)
        return r.content

    async def _get_file(
        self,
        rpath: str,
        lpath: str | io.IOBase,
        chunk_size: int = 5 * 2**20,
        callback=DEFAULT_CALLBACK,
        **kwargs: Any,
    ) -> None:
        kw = self.kwargs.copy()
        kw.update(kwargs)
        logger.debug(rpath)
        session = await self.set_session()

        r = await session.get(str(self.encode_url(rpath)), **kw)
        try:
            size = int(r.headers["content-length"])
        except (ValueError, KeyError):
            size = None

        callback.set_size(size)
        self._raise_not_found_for_status(r, rpath)

        outfile = lpath if isfilelike(lpath) else open(lpath, "wb")  # type: ignore  # noqa: PTH123, SIM115

        try:
            async for chunk in r.aiter_bytes(chunk_size):
                outfile.write(chunk)  # type: ignore
                callback.relative_update(len(chunk))
        finally:
            if not isfilelike(lpath):
                outfile.close()  # type: ignore

    async def _put_file(
        self,
        lpath: str | io.IOBase,
        rpath: str,
        chunk_size: int = 5 * 2**20,
        callback=DEFAULT_CALLBACK,
        method: str = "post",
        mode: CreationMode = "overwrite",
        **kwargs: Any,
    ) -> None:
        if mode != "overwrite":
            msg = "Only 'overwrite' mode is supported"
            raise NotImplementedError(msg)

        def gen_chunks():
            if isinstance(lpath, io.IOBase):
                context = nullcontext(lpath)
                use_seek = False
            else:
                context = open(lpath, "rb")  # noqa: PTH123, SIM115
                use_seek = True

            with context as f:
                if use_seek:
                    callback.set_size(f.seek(0, 2))
                    f.seek(0)
                else:
                    callback.set_size(getattr(f, "size", None))

                while chunk := f.read(chunk_size):
                    yield chunk
                    callback.relative_update(len(chunk))

        kw = self.kwargs.copy()
        kw.update(kwargs)
        session = await self.set_session()

        method = method.lower()
        if method not in {"post", "put"}:
            msg = f"method must be either 'post' or 'put', not: {method!r}"
            raise ValueError(msg)

        r = await getattr(session, method)(str(self.encode_url(rpath)), content=gen_chunks(), **kw)
        self._raise_not_found_for_status(r, rpath)

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        import httpx

        kw = self.kwargs.copy()
        kw.update(kwargs)
        try:
            logger.debug(path)
            session = await self.set_session()
            r = await session.get(str(self.encode_url(path)), **kw)
        except httpx.RequestError:
            return False
        else:
            return r.status_code < 400  # noqa: PLR2004

    async def _isfile(self, path: str, **kwargs: Any) -> bool:
        return await self._exists(path, **kwargs)

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int | None = None,
        autocommit: None = None,
        cache_type: str | None = None,
        cache_options: dict[str, Any] | None = None,
        size: int | None = None,
        **kwargs: Any,
    ) -> HTTPFile | HTTPStreamFile:
        """Create a file-like object."""
        if mode != "rb":
            msg = "Write mode not supported"
            raise NotImplementedError(msg)

        block_size = block_size if block_size is not None else self.block_size
        kw = self.kwargs.copy()
        kw["asynchronous"] = self.asynchronous
        kw.update(kwargs)

        # Force streaming for gzip encoding
        headers = kw.get("headers", {})
        if "gzip_encoding" in headers:  # Changed condition here
            return HTTPStreamFile(
                self,
                path,
                mode=mode,
                loop=self.loop,
                session=sync(self.loop, self.set_session),
                **kw,
            )

        # Try to get size unless explicitly streaming
        if block_size != 0 and cache_type != "none":
            try:
                info = {}
                size = size or info.update(self.info(path, **kwargs)) or info["size"]
                if size and info.get("partial", True):
                    return HTTPFile(
                        self,
                        path,
                        session=sync(self.loop, self.set_session),
                        block_size=block_size,
                        mode=mode,
                        size=size,
                        cache_type=cache_type or self.cache_type,
                        cache_options=cache_options or self.cache_options,
                        loop=self.loop,
                        **kw,
                    )
            except Exception:  # noqa: BLE001
                pass

        # Default to streaming
        return HTTPStreamFile(
            self,
            path,
            mode=mode,
            loop=self.loop,
            session=sync(self.loop, self.set_session),
            **kw,
        )

    async def open_async(
        self,
        path: str,
        mode: str = "rb",
        size: int | None = None,
        **kwargs: Any,
    ) -> AsyncStreamFile:
        session = await self.set_session()
        if size is None:
            with contextlib.suppress(FileNotFoundError):
                size = (await self._info(path, **kwargs)).get("size")
        return AsyncStreamFile(
            self,
            path,
            loop=self.loop,
            session=session,
            size=size,
            **kwargs,
        )

    def ukey(self, path: str) -> str:
        """Unique identifier; assume HTTP files are static, unchanging."""
        return tokenize(path, self.kwargs, self.protocol)

    async def _pipe_file(
        self,
        path: str,
        value: bytes,
        mode: CreationMode = "overwrite",
        **kwargs: Any,
    ) -> None:
        """Write bytes to a remote file over HTTP.

        Parameters
        ----------
        path : str
            Target URL where the data should be written
        value : bytes
            Data to be written
        mode : str
            How to write to the file - only 'overwrite' is supported
        **kwargs : Any
            Additional parameters to pass to the HTTP request
        """
        if mode != "overwrite":
            msg = "Only 'overwrite' mode is supported"
            raise NotImplementedError(msg)

        url = self._strip_protocol(path)
        kw = self.kwargs.copy()
        kw.update(kwargs)

        headers = kw.pop("headers", {}).copy()
        headers["Content-Length"] = str(len(value))
        kw["headers"] = headers

        session = await self.set_session()
        r = await session.put(str(self.encode_url(url)), content=value, **kw)
        self._raise_not_found_for_status(r, url)

    async def _info(self, path: str, **kwargs: Any) -> HttpInfo:
        """Get info of URL."""
        info = {}
        session = await self.set_session()

        for policy in ["head", "get"]:
            try:
                info.update(
                    await _file_info(
                        str(self.encode_url(path)),
                        size_policy=policy,
                        session=session,
                        **self.kwargs,
                        **kwargs,
                    )
                )
                if info.get("size") is not None:
                    break
            except Exception as exc:
                if policy == "get":
                    raise FileNotFoundError(path) from exc
                logger.debug("HEAD request failed", exc_info=exc)

        return {"name": path, "size": None, **info, "type": "file"}  # type: ignore[return-value, typeddict-item]

    async def _glob(self, path: str, maxdepth: int | None = None, **kwargs: Any):
        """Find files by glob-matching."""
        if maxdepth is not None and maxdepth < 1:
            msg = "maxdepth must be at least 1"
            raise ValueError(msg)

        ends_with_slash = path.endswith("/")
        path = self._strip_protocol(path)
        append_slash_to_dirname = ends_with_slash or path.endswith(("/**", "/*"))
        idx_star = path.find("*") if "*" in path else len(path)
        idx_brace = path.find("[") if "[" in path else len(path)

        min_idx = min(idx_star, idx_brace)
        detail = kwargs.pop("detail", False)

        if not has_magic(path):
            if await self._exists(path, **kwargs):
                if not detail:
                    return [path]
                return {path: await self._info(path, **kwargs)}
            if not detail:
                return []
            return {}

        if "/" in path[:min_idx]:
            min_idx = path[:min_idx].rindex("/")
            root = path[: min_idx + 1]
            depth = path[min_idx + 1 :].count("/") + 1
        else:
            root = ""
            depth = path[min_idx + 1 :].count("/") + 1
        if "**" in path:
            if maxdepth is not None:
                idx_double_stars = path.find("**")
                depth_double_stars = path[idx_double_stars:].count("/") + 1
                depth = depth - depth_double_stars + maxdepth
            else:
                depth = None  # type: ignore

        allpaths = await self._find(root, maxdepth=depth, withdirs=True, detail=True, **kwargs)

        pattern = glob_translate(path + ("/" if ends_with_slash else ""))
        pattern = re.compile(pattern)

        out = {
            (
                p.rstrip("/")
                if not append_slash_to_dirname and info["type"] == "directory" and p.endswith("/")
                else p
            ): info
            for p, info in sorted(allpaths.items())  # type: ignore
            if pattern.match(p.rstrip("/"))
        }

        if detail:
            return out
        return list(out)

    async def _isdir(self, path: str) -> bool:
        try:
            return bool(await self._ls(path))
        except (FileNotFoundError, ValueError):
            return False


class HTTPFile(AbstractBufferedFile):
    """A file-like object pointing to a remote HTTP(S) resource."""

    def __init__(
        self,
        fs: HTTPFileSystem,
        url: str,
        session: httpx.AsyncClient | None = None,
        block_size: Literal["default"] | int | None = None,
        mode: str = "rb",
        cache_type: str = "bytes",
        cache_options: dict[str, Any] | None = None,
        size: int | None = None,
        loop: Any = None,
        asynchronous: bool = False,
        **kwargs: Any,
    ) -> None:
        """Initialize the file object."""
        if mode != "rb":
            msg = "Write mode not supported"
            raise NotImplementedError(msg)

        self.asynchronous = asynchronous
        self.loop = loop
        self.url = url
        self.session = session
        self.details = {"name": url, "size": size, "type": "file"}

        super().__init__(
            fs=fs,
            path=url,
            mode=mode,
            block_size=block_size,  # pyright: ignore
            cache_type=cache_type,
            cache_options=cache_options,
            **kwargs,
        )

    def read(self, length: int = -1) -> bytes:
        """Read bytes from file."""
        file_size = self.size  # type: ignore
        assert isinstance(self.blocksize, int)
        if (length < 0 and self.loc == 0) and not (
            file_size is not None and file_size <= self.blocksize
        ):
            self._fetch_all()
        if file_size is None:
            if length < 0:
                self._fetch_all()
        else:
            length = min(file_size - self.loc, length)
        return super().read(length)

    async def async_fetch_all(self) -> None:
        """Read whole file in one shot, without caching."""
        if not isinstance(self.cache, AllBytes):  # type: ignore[has-type]
            assert self.session, "Session is not initialized"
            r = await self.session.get(str(self.fs.encode_url(self.url)), **self.kwargs)
            r.raise_for_status()
            out = r.content
            self.cache = AllBytes(
                size=len(out),
                fetcher=None,
                blocksize=None,
                data=out,
            )
            self.size = len(out)

    _fetch_all = sync_wrapper(async_fetch_all)

    def _parse_content_range(self, headers: Mapping[str, str]) -> tuple[int | None, ...]:
        """Parse the Content-Range header."""
        content_range = headers.get("Content-Range", "")
        match = re.match(r"bytes (\d+-\d+|\*)/(\d+|\*)", content_range)
        if not match:
            return None, None, None

        if match[1] == "*":
            return None, None, None

        start, end = [int(x) for x in match[1].split("-")]
        total = None if match[2] == "*" else int(match[2])
        return start, end, total

    async def async_fetch_range(self, start: int, end: int) -> bytes:
        """Download a block of data."""
        kwargs = self.kwargs.copy()
        headers = kwargs.pop("headers", {}).copy()
        headers["Range"] = f"bytes={start}-{end - 1}"
        assert self.session
        r = await self.session.get(
            str(self.fs.encode_url(self.url)),
            headers=headers,
            **kwargs,
        )

        if r.status_code == 416:  # noqa: PLR2004
            # Range request outside file
            return b""

        r.raise_for_status()

        # Check if server handled range request correctly
        response_is_range = (
            r.status_code == 206  # noqa: PLR2004
            or self._parse_content_range(r.headers)[0] == start
            or int(r.headers.get("Content-Length", end + 1)) <= end - start
        )

        if response_is_range:
            # Partial content, as expected
            return r.content
        if start > 0:
            msg = (
                "The HTTP server doesn't support range requests. "
                "Only reading from the beginning is supported. "
                "Open with block_size=0 for a streaming file interface."
            )
            raise ValueError(msg)
        # Response is not a range, but we want the start of the file
        content = []
        total_bytes = 0
        async for chunk in r.aiter_bytes(2**20):
            content.append(chunk)
            total_bytes += len(chunk)
            if total_bytes > end - start:
                break
        return b"".join(content)[: end - start]

    _fetch_range = sync_wrapper(async_fetch_range)


class HTTPStreamFile(AbstractBufferedFile):
    def __init__(
        self,
        fs: HTTPFileSystem,
        url: str,
        mode: str = "rb",
        loop: Any = None,
        session: httpx.AsyncClient | None = None,
        **kwargs: Any,
    ) -> None:
        self.asynchronous = kwargs.pop("asynchronous", False)
        self.url = url
        self.loop = loop
        self.session = session
        self._content_buffer = b""
        self._stream: AsyncIterator[bytes] | None = None

        if mode != "rb":
            msg = "Write mode not supported"
            raise ValueError(msg)

        self.details = {"name": url, "size": None}
        super().__init__(fs=fs, path=url, mode=mode, cache_type="none", **kwargs)

        async def _init() -> Response:
            assert self.session
            r = await self.session.get(str(self.fs.encode_url(url)), **kwargs)
            self.fs._raise_not_found_for_status(r, url)
            return r

        self.r = sync(self.loop, _init)

    def seek(self, loc: int, whence: int = 0) -> int:
        """Seek to position in file."""
        if not self.seekable():
            msg = "Stream is not seekable"
            raise ValueError(msg)
        current_loc = self.loc  # type: ignore
        if whence == 1:  # SEEK_CUR
            loc = current_loc + loc
        elif whence == 2:  # SEEK_END  # noqa: PLR2004
            msg = "Cannot seek from end in streaming file"
            raise ValueError(msg)

        # SEEK_SET or converted SEEK_CUR
        if loc < 0:
            msg = "Cannot seek before start of file"
            raise ValueError(msg)

        if loc == current_loc:
            return current_loc

        if loc < current_loc:
            if loc == 0:
                # Only support seeking back to start
                self.r = sync(self.loop, self._init)  # pyright: ignore
                self._content_buffer = b""
                self._stream = None
                self.loc = 0
                return 0
            msg = "Cannot seek backwards except to start"
            raise ValueError(msg)

        # Check for explicit range support
        headers = self.kwargs.get("headers", {})
        if not headers or headers.get("accept_range") == "none":
            # Either no headers (default) or explicitly disabled ranges
            msg = "Random access not supported with streaming file"
            raise ValueError(msg)

        # For forward seeks within buffered data
        if self._content_buffer and loc <= len(self._content_buffer):
            self._content_buffer = self._content_buffer[loc:]
            self.loc = loc
            return self.loc

        # Need to read and discard data
        to_read = loc - self.loc
        self.read(to_read)
        return self.loc

    async def _read(self, num: int = -1) -> bytes:
        """Read bytes from remote file."""
        if not self._stream:
            assert self.r
            self._stream = self.r.aiter_bytes()
            self._content_buffer = b""

        if num < 0:
            # Read all remaining data
            chunks = [self._content_buffer]
            assert self._stream
            async for chunk in self._stream:  # pyright: ignore
                chunks.append(chunk)  # noqa: PERF401
            self._content_buffer = b""
            data = b"".join(chunks)
            self.loc += len(data)
            return data

        if len(self._content_buffer) >= num:
            # Return from buffer
            data = self._content_buffer[:num]
            self._content_buffer = self._content_buffer[num:]
            self.loc += len(data)
            return data

        # Need more data
        result = [self._content_buffer]
        bytes_needed = num - len(self._content_buffer)
        try:
            assert self._stream
            while bytes_needed > 0:
                chunk = await self._stream.__anext__()  # pyright: ignore
                result.append(chunk)
                bytes_needed -= len(chunk)
        except StopAsyncIteration:
            pass

        data = b"".join(result)
        if len(data) <= num:
            self._content_buffer = b""
            self.loc += len(data)
            return data

        self._content_buffer = data[num:]
        self.loc += num
        return data[:num]

    read = sync_wrapper(_read)  # pyright: ignore[reportAssignmentType]

    async def _close(self) -> None:
        assert self.r
        await self.r.aclose()  # pyright: ignore

    def close(self) -> None:
        asyncio.run_coroutine_threadsafe(self._close(), self.loop)
        super().close()


class AsyncStreamFile(AbstractAsyncStreamedFile):
    """Async streaming file-like object for HTTP(S) resources."""

    def __init__(
        self,
        fs: HTTPFileSystem,
        url: str,
        mode: str = "rb",
        loop: Any = None,
        session: httpx.AsyncClient | None = None,
        size: int | None = None,
        **kwargs: Any,
    ) -> None:
        self.url = url
        self.session = session
        self.r: httpx.Response | None = None
        if mode != "rb":
            msg = "Write mode not supported"
            raise ValueError(msg)

        self.details = {"name": url, "size": None}
        self.kwargs = kwargs
        super().__init__(fs=fs, path=url, mode=mode, cache_type="none")
        self.size = size

    async def read(self, length: int = -1) -> bytes:
        assert self.session
        if self.r is None:
            r = await self.session.get(str(self.fs.encode_url(self.url)), **self.kwargs)
            self.fs._raise_not_found_for_status(r, self.url)
            self.r = r

        chunk = await self.r.aread()  # (num)
        self.loc += len(chunk)
        return chunk

    async def close(self) -> None:
        if self.r is not None:
            await self.r.aclose()
            self.r = None
        await super().close()


async def get_range(
    session: httpx.AsyncClient,
    url: str,
    start: int,
    end: int,
    file: str | None = None,
    **kwargs,
) -> bytes | None:
    """Explicitly get a range of bytes when we know it must be safe."""
    kwargs = kwargs.copy()
    headers = kwargs.pop("headers", {}).copy()
    headers["Range"] = f"bytes={start}-{end - 1}"

    r = await session.get(url, headers=headers, **kwargs)
    r.raise_for_status()

    out = r.content
    if file:
        with open(file, "r+b") as f:  # noqa: PTH123
            f.seek(start)
            f.write(out)
    else:
        return out
    return None


async def _file_info(
    url: str,
    session: httpx.AsyncClient,
    size_policy: str = "head",
    **kwargs: Any,
) -> dict[str, Any]:
    """Get details about the file (size/checksum etc)."""
    logger.debug("Retrieve file size for %s", url)
    kwargs = kwargs.copy()
    ar = kwargs.pop("allow_redirects", True)
    headers = kwargs.get("headers", {}).copy()
    headers["Accept-Encoding"] = "identity"
    kwargs["headers"] = headers

    info: dict[str, Any] = {}
    if size_policy == "head":
        r = await session.head(url, follow_redirects=ar, **kwargs)
    elif size_policy == "get":
        r = await session.get(url, follow_redirects=ar, **kwargs)
    else:
        msg = f'size_policy must be "head" or "get", got {size_policy}'
        raise ValueError(msg)

    r.raise_for_status()

    if "Content-Length" in r.headers:
        # Some servers may ignore Accept-Encoding and return compressed content
        if "Content-Encoding" not in r.headers or r.headers["Content-Encoding"] in {
            "identity",
            "",
        }:
            info["size"] = int(r.headers["Content-Length"])
    elif "Content-Range" in r.headers:
        info["size"] = int(r.headers["Content-Range"].split("/")[1])

    if "Content-Type" in r.headers:
        info["mimetype"] = r.headers["Content-Type"].partition(";")[0]

    if r.headers.get("Accept-Ranges") == "none":
        # Server explicitly discourages partial content requests
        info["partial"] = False

    info["url"] = str(r.url)

    # Include checksum information if available
    for checksum_field in ["ETag", "Content-MD5", "Digest"]:
        if r.headers.get(checksum_field):
            info[checksum_field] = r.headers[checksum_field]

    return info


async def _file_size(
    url: str,
    *args: Any,
    session: httpx.AsyncClient | None = None,
    **kwargs: Any,
) -> int | None:
    """Get file size from remote server."""
    if session is None:
        session = await get_client()
        cleanup = True
    else:
        cleanup = False

    try:
        kw_args = kwargs.copy()
        kw_args["session"] = session
        info = await _file_info(url, *args, **kwargs)
        return info.get("size")
    finally:
        if cleanup:
            await session.aclose()


file_size = sync_wrapper(_file_size)


def has_magic(path: str) -> bool:
    """Check if a path contains glob magic characters."""
    magic_check = re.compile(r"[*?\[\]]")
    return bool(magic_check.search(path))


def get_compression(
    filename: str | None = None,
    compression: str | None = None,
) -> tuple[str | None, str | None]:
    """Get compression type and extension from filename or compression param."""
    if compression == "infer" and filename:
        compression = infer_compression(filename)
    if compression is not None:
        compression = compression.lower()
    return compression, None


def infer_compression(filename: str) -> str | None:
    """Infer compression type from file extension."""
    extension = os.path.splitext(filename)[-1].strip(".")  # noqa: PTH122
    if extension in ["gz", "gzip"]:
        return "gzip"
    if extension == "bz2":
        return "bz2"
    if extension == "xz":
        return "xz"
    return None


# Additional utility for handling compression
def get_decompressor(
    compression: str | None,
    filename: str | None = None,
) -> Callable[[bytes], bytes] | None:
    """Get decompressor function based on compression type."""
    if compression == "infer" and filename:
        compression = infer_compression(filename)

    if compression == "gzip":
        import gzip

        return gzip.decompress
    if compression == "bz2":
        import bz2

        return bz2.decompress
    if compression == "xz":
        import lzma

        return lzma.decompress
    return None
